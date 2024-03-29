package internal

import (
	"context"
	"sync"
)

type Fanin[T any] struct {
	in        []<-chan T
	interrupt chan struct{}

	selectFunc func(ctx context.Context) (int, T, bool)
	m          sync.Mutex
}

func NewFanin[T any](in ...<-chan T) *Fanin[T] {
	f := &Fanin[T]{
		in:        in,
		interrupt: make(chan struct{}, 1),
	}
	f.reload()
	return f
}

// Recv selects a value from one of the in channels. It's not safe for concurrent
// use.
func (f *Fanin[T]) Recv(ctx context.Context) (T, error) {
	for {
		f.m.Lock()
		ff := f.selectFunc
		f.m.Unlock()

		chosen, val, ok := ff(ctx)
		switch chosen {
		case contextChanIndex:
			return empty[T](), ctx.Err()
		case interruptChanIndex:
			continue // selectFunc was reloaded
		}

		if !ok {
			// one of the in channels is closed, not supported right now
			panic("input channel was closed, not supported action")
		}
		return val, nil
	}
}

func (f *Fanin[T]) Reload(in []<-chan T) {
	f.in = in
	f.reload()

	// notify the selectFunc to reload
	select {
	case f.interrupt <- struct{}{}:
	default:
	}
}

func (f *Fanin[T]) reload() {
	ff := f.chooseSelectFunc(f.interrupt, f.in)
	f.m.Lock()
	f.selectFunc = ff
	f.m.Unlock()
}
