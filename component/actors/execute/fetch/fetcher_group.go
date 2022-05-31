package fetch

import (
	"github.com/dhenisdj/scheduler/component/actors/execute/group"
	"sync"
)

type FetcherGroup struct {
	*group.Group
	appKey    string
	appSecret string
	url       string
	Fetchers  []*Fetcher
}

func NewFetcherGroup(env, namespace, poolID, groupName, appKey, appSecret, callBack string, opts ...group.GroupOption) *FetcherGroup {
	g := group.NewGroup(env, namespace, poolID, groupName)
	for _, opt := range opts {
		opt(g)
	}

	fg := &FetcherGroup{
		Group:     g,
		appKey:    appKey,
		appSecret: appSecret,
		url:       callBack,
		Fetchers:  make([]*Fetcher, 0),
	}

	return fg
}

// Start starts the workers and associated processes.
func (fg *FetcherGroup) Start(uri string) {
	if fg.Started {
		return
	}
	fg.Started = true

	for _, f := range fg.Fetchers {
		go f.start(uri)
	}
	fg.Ctx.If("fetcher group %s for %s started!", fg.GroupID, fg.GroupName)
}

// Stop stops the workers and associated processes.
func (fg *FetcherGroup) Stop() {
	if !fg.Started {
		return
	}

	wg := sync.WaitGroup{}
	for _, f := range fg.Fetchers {
		wg.Add(1)
		go func(f *Fetcher) {
			f.stop()
			wg.Done()
		}(f)
	}
	wg.Wait()

	fg.Started = false
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (fg *FetcherGroup) Drain() {
	wg := sync.WaitGroup{}
	for _, f := range fg.Fetchers {
		wg.Add(1)
		go func(f *Fetcher) {
			f.drain()
			wg.Done()
		}(f)
	}
	wg.Wait()
}
