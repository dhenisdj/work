package fetch

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/execute/parse"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Fetcher struct {
	*parse.Parser
	*enqueue.Enqueuer
	*entities.Credential
	FetcherID        string
	poolID           string
	groupID          string
	namespace        string
	executor         string
	businessGroup    string
	url              string
	env              string
	jobTypes         map[string]*task.JobType
	contextType      reflect.Type
	ctx              *context.Context
	sleepBackoffs    []int64
	redisFetchScript *redis.Script
	sampler          models.PrioritySampler
	middleware       []*handler.MiddlewareHandler

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

type FetcherOption func(f *Fetcher)

func WithEnv(env string) FetcherOption {
	return func(f *Fetcher) {
		f.env = env
	}
}

func WithExecutor(executor string) FetcherOption {
	return func(f *Fetcher) {
		f.executor = executor
	}
}

func WithBiz(businessGroup string) FetcherOption {
	return func(f *Fetcher) {
		f.businessGroup = businessGroup
	}
}

func WithCallback(callBack string) FetcherOption {
	return func(f *Fetcher) {
		f.url = callBack
	}
}

func WithCredential(credential *entities.Credential) FetcherOption {
	return func(f *Fetcher) {
		f.Credential = credential
	}
}

func WithParser(parser *parse.Parser) FetcherOption {
	return func(f *Fetcher) {
		f.Parser = parser
	}
}

func WithContext(ctx *context.Context) FetcherOption {
	return func(f *Fetcher) {
		f.ctx = ctx
		f.contextType = reflect.TypeOf(*ctx)
	}
}

func WithBackoffs(sleepBackoffs []int64) FetcherOption {
	return func(f *Fetcher) {
		f.sleepBackoffs = sleepBackoffs
	}
}

func WithEnqueuer(enqueuer *enqueue.Enqueuer) FetcherOption {
	return func(f *Fetcher) {
		f.Enqueuer = enqueuer
	}
}

func NewFetcher(namespace, id, poolID, groupID string, opts ...FetcherOption) *Fetcher {

	fetcher := &Fetcher{
		FetcherID: id,
		poolID:    poolID,
		groupID:   groupID,
		namespace: namespace,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(fetcher)
	}
	return fetcher
}

func (f *Fetcher) parseTask(resp *http.Response) (*task.Job, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := &entities.Response{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}

	if response.Code != 0 {
		// deal with error from backend
		return nil, errors.New(fmt.Sprintf("Error from backend with code %d, msg %s", response.Code, response.Message))
	}

	if response.Task == (entities.BaseTask{}) {
		f.ctx.Df("Task not available for executor %s and biz", f.executor, f.businessGroup)
		return nil, nil
	}

	response.Task.Env = f.env

	return f.Parse(&response.Task), nil
}

func (f *Fetcher) fetch(uri string) (*task.Job, error) {

	url := f.ctx.SignedURL(f.Credential.Key, f.Credential.Secret, f.url, uri, f.executor, f.businessGroup)

	//fmt.Printf("url %s\n", url)

	request, err := f.ctx.Request("GET", url, nil)
	if err != nil {
		return nil, err
	}

	response, err := f.ctx.DoRequest(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	return f.parseTask(response)

}

func (f *Fetcher) start(uri string) {
	go f.loop(uri)
	f.ctx.If("%s Fetcher %s Started!", f.env, f.FetcherID)
}

func (f *Fetcher) stop() {
	f.stopChan <- struct{}{}
	<-f.doneStoppingChan
}

func (f *Fetcher) drain() {
	f.drainChan <- struct{}{}
	<-f.doneDrainingChan
}

func (f *Fetcher) loop(uri string) {
	var drained bool
	var consequtiveNoJobs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-f.stopChan:
			f.doneStoppingChan <- struct{}{}
			return
		case <-f.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := f.fetch(uri)
			if err != nil {
				f.ctx.LE("fetcher.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if job != nil {
				f.EnqueueSparkTask(job)
				time.Sleep(15 * time.Second)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				if drained {
					f.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(f.sleepBackoffs)) {
					idx = int64(len(f.sleepBackoffs)) - 1
				}
				timer.Reset(time.Duration(f.sleepBackoffs[idx]) * time.Millisecond)
			}
		}
	}
}
