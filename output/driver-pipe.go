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

	group  *errgroup.Group
	cancel context.CancelFunc

	volumeUpdate chan float32
	err          chan error

	transform func([]float32, []byte) int
}

// Largest float that scales into an int16 without wrapping.
const maxSampleValueS16 = float32(0x7fff) / float32(0x8000)

func clampSample(f, max float32) float32 {
	if f < -1 {
		return -1
	}
	if f > max {
		return max
	}
	return f
}

func newPipeTransform(format string) (func([]float32, []byte) int, error) {
	switch format {
	case "s16le":
		return func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int16(clampSample(in[i], maxSampleValueS16) * 32768)
				binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
			}
			return len(in) * 2
		}, nil
	case "s32le":
		// float32 rounds 2147483647 up to 2^31, so scale in float64.
		return func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int32(float64(clampSample(in[i], 1)) * 2147483647)
				binary.LittleEndian.PutUint32(out[i*4:], uint32(sample))
			}
			return len(in) * 4
		}, nil
	case "f32le":
		return func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := math.Float32bits(in[i])
				binary.LittleEndian.PutUint32(out[i*4:], sample)
			}
			return len(in) * 4
		}, nil
	default:
		return nil, fmt.Errorf("unknown output pipe format: %s", format)
	}
}

func (out *pipeOutput) readerLoop(ctx context.Context, buff_chan chan []float32) error {
	floats := make([]float32, 4*1024)
	fmt.Println("READERLOOP: AVVIATO")
	defer close(buff_chan)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("readerLoop: mi sono chiuso")
			return nil

		default:
			n, err := out.reader.Read(floats)

			if !out.externalVolume {
				volume := out.volume * out.volume
				for i := 0; i < n; i++ {
					floats[i] *= volume
				}
			}

			newBuf := make([]float32, n)
			copy(newBuf, floats[:n])

			select {
			case <-ctx.Done():
				fmt.Println("readerLoop: mi sono chiuso durante l'invio")
				return nil
			case buff_chan <- newBuf:
			}

			if errors.Is(err, io.EOF) {
				time.Sleep(100 * time.Millisecond)
			} else if err != nil {
				fmt.Println("ERRORE readerLoop: mi sono chiuso")
				out.err <- err
				out.cancel()
				return err
			}
		}
	}
}

func (out *pipeOutput) outputLoop(ctx context.Context, buff_chan chan []float32) error {
	fmt.Println("OUTPUTLOOP: AVVIATO")
	bytes := make([]byte, 4*4096)
	tempo := time.NewTicker(46200 * time.Microsecond)
	defer tempo.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("outputLoop: mi sono chiuso")
			return nil

		default:
			<-tempo.C
			out.lock.Lock()
			paused := out.paused
			out.lock.Unlock()

			if paused {
				continue
			}

			var floats []float32
			var ok bool
			select {
			case floats, ok = <-buff_chan:
				if !ok {
					fmt.Println("outputLoop: mi sono chiuso")
					return nil
				}
			case <-ctx.Done():
				fmt.Println("outputLoop: mi sono chiuso")
				return nil
			}

			nn := out.transform(floats, bytes)
			_, err := out.file.Write(bytes[:nn])
			if err != nil {
				fmt.Println("ERRORE outputLoop: mi sono chiuso")
				out.err <- err
				out.cancel()
				return err
			}

			if errors.Is(err, io.EOF) {
				out.paused = true
			} else if err != nil {
				fmt.Println("ERRORE outputLoop: mi sono chiuso")
				out.err <- err
				out.cancel()
				return err
			}
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
	if vol < 0 {
		vol = 0
	}
	if vol > 1 {
		vol = 1
	}

	out.volume = vol
	sendVolumeUpdate(out.volumeUpdate, vol)
}

func (out *pipeOutput) Error() <-chan error {
	// No need to lock here (out.err is only set in newOutput).
	return out.err
}

func (out *pipeOutput) Close() error {
	fmt.Println("HO CHIAMATO LA FUNZIONE DI CHIUSURA!!!")
	out.lock.Lock()
	if out.cancel == nil {
		out.lock.Unlock()
		return nil
	}

	out.cancel()

	out.paused = false
	out.cond.Broadcast()
	out.lock.Unlock()

	if closer, ok := out.reader.(io.Closer); ok {
		_ = closer.Close()
		fmt.Println("HO CHIUSO READER DA SPOTIFY")
	}

	_ = out.file.Close()
	fmt.Println("HO CHIUSO WRITER SU PIPE")

	return nil
}
