package workqueue

import (
	"context"
	"math"
	"runtime/debug"
	"sync"

	"go.uber.org/zap"
)

type DoWorkPieceFunc func(piece int)

// ParallelizeUntil is a framework that allows for parallelizing N
// independent pieces of work until done or the context is canceled.
func ParallelizeUntil(ctx context.Context, workers, pieces int, doWorkPiece DoWorkPieceFunc) {
	var stop <-chan struct{}
	if ctx != nil {
		stop = ctx.Done()
	}

	toProcess := make(chan int, pieces)
	for i := 0; i < pieces; i++ {
		toProcess <- i
	}
	close(toProcess)

	if pieces < workers {
		workers = pieces
	}

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					zap.L().Error("work has panic", zap.Any("panic", r))
					debug.PrintStack()
				}
			}()
			for piece := range toProcess {
				select {
				case <-stop:
					return
				default:
					doWorkPiece(piece)
				}
			}
		}()
	}
	wg.Wait()
}

// ParallelizeUntil is a framework that allows for parallelizing N
// independent pieces of work until done or the context is canceled.
func ParallelizeUntilOptimize(ctx context.Context, workers, pieces int, doWorkPiece DoWorkPieceFunc) {
	var stop <-chan struct{}
	if ctx != nil {
		stop = ctx.Done()
	}

	if pieces < workers {
		workers = pieces
	}

	page := int(math.Ceil(float64(pieces) / float64(workers)))

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workIndex int) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					zap.L().Error("work has panic", zap.Any("panic", r))
					debug.PrintStack()
				}
			}()
			start := page * workIndex
			end := start + page
			if end >= pieces {
				end = pieces
			}

			for j := start; j < end; j++ {
				select {
				case <-stop:
					return
				default:
					doWorkPiece(j)
				}
			}
		}(i)
	}
	wg.Wait()
}
