package work

import (
	"sync"
)

type WorkerGroup struct {
	workerGroupID string
	namespace     string
	poolID        string
	workers       []*worker

	started bool
}

// Start starts the workers and associated processes.
func (wg *WorkerGroup) Start() {
	if wg.started {
		return
	}
	wg.started = true

	for _, w := range wg.workers {
		go w.start()
	}
}

// Stop stops the workers and associated processes.
func (wkg *WorkerGroup) Stop() {
	if !wkg.started {
		return
	}

	wg := sync.WaitGroup{}
	for _, w := range wkg.workers {
		wg.Add(1)
		go func(w *worker) {
			w.stop()
			wg.Done()
		}(w)
	}
	wg.Wait()

	wkg.started = false
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (wkg *WorkerGroup) Drain() {
	wg := sync.WaitGroup{}
	for _, w := range wkg.workers {
		wg.Add(1)
		go func(w *worker) {
			w.drain()
			wg.Done()
		}(w)
	}
	wg.Wait()
}
