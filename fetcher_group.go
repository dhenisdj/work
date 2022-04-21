package work

import (
	"sync"
)

type FetcherGroup struct {
	fetcherGroupID string
	namespace      string
	poolID         string
	appKey         string
	appSecret      string
	url            string
	env            string
	fetchers       []*Fetcher

	started bool
}

// Start starts the workers and associated processes.
func (fg *FetcherGroup) Start(uri string) {
	if fg.started {
		return
	}
	fg.started = true

	for _, f := range fg.fetchers {
		go f.start(uri)
	}
}

// Stop stops the workers and associated processes.
func (fg *FetcherGroup) Stop() {
	if !fg.started {
		return
	}

	wg := sync.WaitGroup{}
	for _, f := range fg.fetchers {
		wg.Add(1)
		go func(f *Fetcher) {
			f.stop()
			wg.Done()
		}(f)
	}
	wg.Wait()

	fg.started = false
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (fg *FetcherGroup) Drain() {
	wg := sync.WaitGroup{}
	for _, f := range fg.fetchers {
		wg.Add(1)
		go func(f *Fetcher) {
			f.drain()
			wg.Done()
		}(f)
	}
	wg.Wait()
}
