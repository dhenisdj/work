package work

import (
	"github.com/dhenisdj/scheduler/component/actors/execute/group"
	"sync"
)

type WorkerGroup struct {
	*group.Group
	Workers []*Worker
}

type WorkerGroupOption func(wg *WorkerGroup)

func NewWorkerGroup(env, namespace, poolID string, opts ...group.GroupOption) *WorkerGroup {
	g := group.NewGroup(env, namespace, poolID)
	for _, opt := range opts {
		opt(g)
	}

	wg := &WorkerGroup{
		Group:   g,
		Workers: make([]*Worker, 0),
	}

	return wg
}

// Start starts the workers and associated processes.
func (wg *WorkerGroup) Start() {
	if wg.Started {
		return
	}
	wg.Started = true

	for _, w := range wg.Workers {
		go w.start()
	}
}

// Stop stops the workers and associated processes.
func (wkg *WorkerGroup) Stop() {
	if !wkg.Started {
		return
	}

	wg := sync.WaitGroup{}
	for _, w := range wkg.Workers {
		wg.Add(1)
		go func(w *Worker) {
			w.stop()
			wg.Done()
		}(w)
	}
	wg.Wait()

	wkg.Started = false
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (wkg *WorkerGroup) Drain() {
	wg := sync.WaitGroup{}
	for _, w := range wkg.Workers {
		wg.Add(1)
		go func(w *Worker) {
			w.drain()
			wg.Done()
		}(w)
	}
	wg.Wait()
}
