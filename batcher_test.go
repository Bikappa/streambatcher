package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type batch struct {
	stream string
	values []int
}

// MockStreamFlusher is a mock implementation of StreamFlusher for testing purposes.
type MockStreamFlusher struct {
	outCh   chan batch
}

func (m *MockStreamFlusher) Flush(ctx context.Context, stream string, values []int) {
	m.outCh <- batch{
		stream: stream,
		values: values,
	}
}

func TestStreamBatcher(t *testing.T) {
	// Create a mock StreamFlusher
	mockFlusher := &MockStreamFlusher{
		outCh: make(chan batch),
	}

	// Create a StreamBatcher with a short batch period for testing
	batchPeriod := 1000 * time.Millisecond
	batcher := NewStreamBatcher[string, int](mockFlusher, batchPeriod)

	wg := sync.WaitGroup{}
	// Create a context for testing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the StreamBatcher in a goroutine
	goWithWg(&wg, func() {
		if err := batcher.Run(ctx); err != nil {
			t.Errorf("StreamBatcher.Run returned an error: %v", err)
		}
	})

	timesOfLastFlush := map[string]time.Time{
		"StreamA": time.Now(),
		"StreamB": time.Now(),
	}
	senderWg := sync.WaitGroup{}

	goWithWg(&senderWg, func() {
		// expect batches [1, 2], [3]
		for i := 0; i < 3; i++ {
			time.Sleep(450 * time.Millisecond)
			batcher.Record("StreamA", i+1)
		}
	})

	goWithWg(&senderWg, func() {
		// expect batches [1, 2, 3], [4, 5, 6], [7]
		for i := 0; i < 7; i++ {
			time.Sleep(300 * time.Millisecond)
			batcher.Record("StreamB", i+1)
		}
	})

	goWithWg(&wg, func() {
		senderWg.Wait()
		// Sleep to allow time for flushing
		time.Sleep(2000 * time.Millisecond)
		close(mockFlusher.outCh)
	})

	alreadyFlushed := map[string]int{
		"StreamA": 0,
		"StreamB": 0,
	}
	for b := range mockFlusher.outCh {
		require.True(t, time.Since(timesOfLastFlush[b.stream]) > batchPeriod)
		timesOfLastFlush[b.stream] = time.Now()
		require.Equal(t, alreadyFlushed[b.stream]+1, b.values[0])
		alreadyFlushed[b.stream] = b.values[len(b.values)-1]

	}

	require.Equal(t, alreadyFlushed["StreamA"], 3)
	require.Equal(t, alreadyFlushed["StreamB"], 7)
	// Cancel the context to stop the StreamBatcher
	cancel()
	wg.Wait()
}

func goWithWg(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}

func Test_BatcherStopWithContext(t *testing.T) {
	mockFlusher := &MockStreamFlusher{
		outCh: make(chan batch),
	}

	// Create a StreamBatcher with a short batch period for testing
	batchPeriod := 1000 * time.Millisecond
	batcher := NewStreamBatcher[string, int](mockFlusher, batchPeriod)

	wg := sync.WaitGroup{}
	// Create a context for testing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	goWithWg(&wg, func() {
		err := batcher.Run(ctx)
		require.Nil(t, err)
	})

	cancel()
	wg.Wait()
}
