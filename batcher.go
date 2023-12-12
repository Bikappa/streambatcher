// Package batcher provides a StreamBatcher that batches values by stream and flushes them after a specified timeout.

package batcher

import (
	"context"
	"sync"
	"time"
)

// valueStreamPair is a pair of a stream identifier and a value.
type valueStreamPair[S comparable, V any] struct {
	stream S
	value  V
}

// StreamFlusher is an interface for flushing values associated with a stream.
type StreamFlusher[S comparable, V any] interface {
	Flush(ctx context.Context, stream S, values []V)
}

// StreamBatcher batches values by stream and flushes them after a timeout.
type StreamBatcher[S comparable, V any] struct {
	batchPeriod time.Duration              // The duration after which a batch is flushed.
	ingestCh    chan valueStreamPair[S, V] // Channel for ingesting stream-value pairs.
	flusher     StreamFlusher[S, V]        // The flusher implementation.
}

// NewStreamBatcher creates a new StreamBatcher instance.
func NewStreamBatcher[S comparable, V any](flusher StreamFlusher[S, V], batchPeriod time.Duration) *StreamBatcher[S, V] {
	return &StreamBatcher[S, V]{
		batchPeriod: batchPeriod.Abs(),
		ingestCh:    make(chan valueStreamPair[S, V]),
		flusher:     flusher,
	}
}

// Record adds a stream-value pair to the StreamBatcher for batching.
func (sb *StreamBatcher[S, V]) Record(s S, values ...V) {
	for _, v := range values {
		p := valueStreamPair[S, V]{
			stream: s,
			value:  v,
		}

		sb.ingestCh <- p
	}
}

// Run starts the StreamBatcher's processing loop.
func (sb *StreamBatcher[S, V]) Run(ctx context.Context) error {
	var (
		batchByStream map[S][]V = map[S][]V{}  // Map to store batches by stream.
		timeoutCh     chan S    = make(chan S) // Channel to signal timeouts.

		timerByStream map[S]*time.Timer = map[S]*time.Timer{} // Map to store timers by stream.
		timersWg      sync.WaitGroup                          // Wait group to wait for all timers to finish.
	)

	defer close(timeoutCh)

	// flushStream flushes the batch associated with a stream.
	flushStream := func(stream S) {
		sb.flusher.Flush(ctx, stream, batchByStream[stream])
		delete(batchByStream, stream)
		delete(timerByStream, stream)
	}

loop:
	for {
		select {
		case p := <-sb.ingestCh:
			// Check if the stream has an existing batch; if not, create one.
			if batchByStream[p.stream] == nil {
				batchByStream[p.stream] = []V{}
			}
			batchByStream[p.stream] = append(batchByStream[p.stream], p.value)

			// If the stream does not have an active timer, create one.
			if timerByStream[p.stream] == nil {
				timer := time.NewTimer(sb.batchPeriod)
				timerByStream[p.stream] = timer
				timersWg.Add(1)
				// Start a goroutine to handle the timer expiration.
				go func() {
					defer timersWg.Done()
					defer timer.Stop()
					select {
					case <-timer.C:
						select {
						case timeoutCh <- p.stream:
						case <-ctx.Done():
						}
					case <-ctx.Done():
					}
				}()
			}
		case stream := <-timeoutCh:
			// Timeout occurred for the stream; flush its batch.
			flushStream(stream)
		case <-ctx.Done():
			// Context is cancelled; exit the loop.
			break loop
		}
	}
	// Flush all remaining batches.
	for stream := range batchByStream {
		flushStream(stream)
	}
	// Wait for all timers to finish before returning.
	timersWg.Wait()
	return nil
}
