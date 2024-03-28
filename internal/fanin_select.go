package internal

import (
	"context"
	"reflect"
)

const (
	contextChanIndex   = 0
	interruptChanIndex = 1
)

func (f *Fanin[T]) chooseSelectFunc(interrupt <-chan struct{}, in []<-chan T) func(context.Context) (int, T, bool) {
	switch len(in) {
	case 0:
		return func(ctx context.Context) (int, T, bool) { return f.select0(ctx, interrupt) }
	case 1:
		return func(ctx context.Context) (int, T, bool) { return f.select1(ctx, interrupt, in[0]) }
	case 2:
		return func(ctx context.Context) (int, T, bool) { return f.select2(ctx, interrupt, in[0], in[1]) }
	case 3:
		return func(ctx context.Context) (int, T, bool) { return f.select3(ctx, interrupt, in[0], in[1], in[2]) }
	case 4:
		return func(ctx context.Context) (int, T, bool) { return f.select4(ctx, interrupt, in[0], in[1], in[2], in[3]) }
	case 5:
		return func(ctx context.Context) (int, T, bool) {
			return f.select5(ctx, interrupt, in[0], in[1], in[2], in[3], in[4])
		}
	case 6:
		return func(ctx context.Context) (int, T, bool) {
			return f.select6(ctx, interrupt, in[0], in[1], in[2], in[3], in[4], in[5])
		}
	default:
		// use reflection for more channels
		cases := make([]reflect.SelectCase, len(in)+2)
		cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(interrupt)}
		for i, ch := range in {
			cases[i+2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}
		return func(ctx context.Context) (int, T, bool) {
			cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
			chosen, value, ok := reflect.Select(cases)
			if !ok { // a channel was closed
				return chosen, empty[T](), ok
			}
			return chosen, value.Interface().(T), ok
		}
	}
}

func (*Fanin[T]) select0(
	ctx context.Context,
	interrupt <-chan struct{},
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	}
}

func (*Fanin[T]) select1(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	}
}

func (*Fanin[T]) select2(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
	c2 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	case val, ok := <-c2:
		return 3, val, ok
	}
}

func (*Fanin[T]) select3(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
	c2 <-chan T,
	c3 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	case val, ok := <-c2:
		return 3, val, ok
	case val, ok := <-c3:
		return 4, val, ok
	}
}

func (*Fanin[T]) select4(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
	c2 <-chan T,
	c3 <-chan T,
	c4 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	case val, ok := <-c2:
		return 3, val, ok
	case val, ok := <-c3:
		return 4, val, ok
	case val, ok := <-c4:
		return 5, val, ok
	}
}

func (*Fanin[T]) select5(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
	c2 <-chan T,
	c3 <-chan T,
	c4 <-chan T,
	c5 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	case val, ok := <-c2:
		return 3, val, ok
	case val, ok := <-c3:
		return 4, val, ok
	case val, ok := <-c4:
		return 5, val, ok
	case val, ok := <-c5:
		return 6, val, ok
	}
}

func (*Fanin[T]) select6(
	ctx context.Context,
	interrupt <-chan struct{},
	c1 <-chan T,
	c2 <-chan T,
	c3 <-chan T,
	c4 <-chan T,
	c5 <-chan T,
	c6 <-chan T,
) (int, T, bool) {
	select {
	case <-ctx.Done():
		return 0, empty[T](), false
	case <-interrupt:
		return 1, empty[T](), false
	case val, ok := <-c1:
		return 2, val, ok
	case val, ok := <-c2:
		return 3, val, ok
	case val, ok := <-c3:
		return 4, val, ok
	case val, ok := <-c4:
		return 5, val, ok
	case val, ok := <-c5:
		return 6, val, ok
	case val, ok := <-c6:
		return 7, val, ok
	}
}

func empty[T any]() T {
	var nil T
	return nil
}
