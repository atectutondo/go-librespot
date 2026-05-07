package output

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"syscall"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"golang.org/x/sync/errgroup"
)

type pipeOutput struct {
	reader librespot.Float32Reader
	file   *os.File

	lock sync.Mutex
	cond *sync.Cond

	externalVolume bool

	volume float32
	paused bool
	closed bool

	volumeUpdate chan float32
	err          chan error

	transform func([]float32, []byte) int
}

func newPipeOutput(opts *NewOutputOptions) (out *pipeOutput, err error) {
	out = &pipeOutput{
		reader:         opts.Reader,
		volume:         opts.InitialVolume,
		err:            make(chan error, 2),
		externalVolume: opts.ExternalVolume,
		volumeUpdate:   opts.VolumeUpdate,
	}

	out.cond = sync.NewCond(&out.lock)

	switch opts.OutputPipeFormat {
	case "s16le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int16(in[i] * 32768)
				binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
			}
			return len(in) * 2
		}
	case "s32le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int32(in[i] * 2147483648)
				binary.LittleEndian.PutUint32(out[i*4:], uint32(sample))
			}
			return len(in) * 4
		}
	case "f32le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := math.Float32bits(in[i])
				binary.LittleEndian.PutUint32(out[i*4:], sample)
			}
			return len(in) * 4
		}
	default:
		return nil, fmt.Errorf("unknown output pipe format: %s", opts.OutputPipeFormat)
	}

	// Open the FIFO for writing as non-blocking to cause an error if there is no reader.
	out.file, err = os.OpenFile(opts.OutputPipe, os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open fifo: %w", err)
	}
	// syscall.Syscall(syscall.SYS_FCNTL, out.file.Fd(), 1031, uintptr(50*1024*1024))

	// // Restore blocking mode now that we are sure we have a reader.
	if err := syscall.SetNonblock(int(out.file.Fd()), false); err != nil {
		return nil, fmt.Errorf("failed to set blocking mode on fifo: %w", err)
	}

	buffer_chan := make(chan []float32, 5)

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return out.readerLoop(ctx, buffer_chan) // legge da spotify e scrive sul canale
	})

	g.Go(func() error {
		return out.outputLoop(ctx, buffer_chan) // legge dal canale e scrive sul pipe i dati per pipewire
	})

	return out, nil
}

func (out *pipeOutput) readerLoop(ctx context.Context, buff_chan chan []float32) error {
	floats := make([]float32, 4*1024) // slice di dati da leggere da spotify

	defer out.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			n, err := out.reader.Read(floats)

			// Apply volume.
			if !out.externalVolume {
				// Map volume (in percent) to what is perceived as linear by
				// humans. This is the same as math.Pow(out.volume, 2) but simpler.
				volume := out.volume * out.volume

				for i := 0; i < n; i++ {
					floats[i] *= volume
				}
			}

			newBuf := make([]float32, n)
			copy(newBuf, floats[:n])
			buff_chan <- newBuf

			if errors.Is(err, io.EOF) {
				// Reached EOF, move to a "paused" state.
				time.Sleep(100 * time.Millisecond)
			} else if err != nil {
				// Got some other error. Close the output and report the error.
				out.err <- err
				out.closed = true
				return err
			}
		}
	}
}

func (out *pipeOutput) outputLoop(ctx context.Context, buff_chan chan []float32) error {
	bytes := make([]byte, 4*4096) // times four is the biggest we can get
	tempo := time.NewTicker(46200 * time.Microsecond)
	defer tempo.Stop()
	defer out.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			<-tempo.C
			out.lock.Lock()

			for out.paused && !out.closed {
				out.cond.Wait()
			}

			if out.closed {
				out.lock.Unlock()
				ctx.Done()
				return nil
			}

			floats := <-buff_chan

			nn := out.transform(floats, bytes)
			_, err := out.file.Write(bytes[:nn])
			if err != nil {
				out.err <- err
				out.closed = true
				out.lock.Unlock()
				return err
			}

			if errors.Is(err, io.EOF) {
				// Reached EOF, move to a "paused" state.
				out.paused = true
			} else if err != nil {
				// Got some other error. Close the output and report the error.
				out.err <- err
				out.closed = true
				out.lock.Unlock()
				return err
			}

			out.lock.Unlock()
		}
	}
}

func (out *pipeOutput) Pause() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	out.paused = true
	out.cond.Signal()
	return nil
}

func (out *pipeOutput) Resume() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	out.paused = false
	out.cond.Signal()
	return nil
}

func (out *pipeOutput) Drop() error {
	return nil
}

func (out *pipeOutput) DelayMs() (int64, error) {
	return 0, nil
}

func (out *pipeOutput) SetVolume(vol float32) {
	if vol < 0 || vol > 1 {
		panic(fmt.Sprintf("invalid volume value: %0.2f", vol))
	}

	out.volume = vol
	sendVolumeUpdate(out.volumeUpdate, vol)
}

func (out *pipeOutput) Error() <-chan error {
	// No need to lock here (out.err is only set in newOutput).
	return out.err
}

func (out *pipeOutput) Close() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	_ = out.file.Close()

	out.closed = true
	out.cond.Signal()

	return nil
}
