package work

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Fetcher struct {
	*Caller
	*Parser
	*Enqueuer
	*Credential
	fetcherID        string
	poolID           string
	groupID          string
	namespace        string
	executor         string
	businessGroup    string
	url              string
	env              string
	jobTypes         map[string]*jobType
	contextType      reflect.Type
	sleepBackoffs    []int64
	redisFetchScript *redis.Script
	sampler          prioritySampler
	middleware       []*middlewareHandler

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newFetcher(namespace, id, poolID, groupID, executor, businessGroup, callBack, env string, credential *Credential, sleepBackoffs []int64, contextType reflect.Type, caller *Caller, parser *Parser, pool *redis.Pool) *Fetcher {

	fetcher := &Fetcher{
		Credential:    credential,
		Caller:        caller,
		Parser:        parser,
		Enqueuer:      NewEnqueuer(namespace, pool),
		fetcherID:     id,
		poolID:        poolID,
		groupID:       groupID,
		namespace:     namespace,
		executor:      executor,
		businessGroup: businessGroup,
		url:           callBack,
		env:           env,
		contextType:   contextType,
		sleepBackoffs: sleepBackoffs,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
	return fetcher
}

func (f *Fetcher) parseTask(resp *http.Response) (*Job, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := &Response{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}

	if response.Code != 0 {
		// deal with error from backend
		return nil, errors.New(fmt.Sprintf("Error from backend with code %d, msg %s", response.Code, response.Message))
	}

	if response.Task == (BaseTask{}) {
		return nil, errors.New("task not available now")
	}

	response.Task.Env = f.env

	return f.parse(&response.Task), nil
}

func (f *Fetcher) fetch(uri string) (*Job, error) {

	url := f.signedURL(f.Credential.Key, f.Credential.Secret, f.url, uri, f.executor, f.businessGroup)

	fmt.Printf("url %s\n", url)

	request, err := f.request("GET", url, nil)
	if err != nil {
		return nil, err
	}

	response, err := f.doRequest(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	return f.parseTask(response)

}

// note: can't be called while the thing is started
func (f *Fetcher) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	f.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority,
			redisKeyJobs(f.namespace, jt.Name),
			redisKeyJobsInProgress(f.namespace, f.groupID, jt.Name),
			redisKeyJobsPaused(f.namespace, jt.Name),
			redisKeyJobsLock(f.namespace, jt.Name),
			redisKeyJobsLockInfo(f.namespace, jt.Name),
			redisKeyJobsConcurrency(f.namespace, jt.Name))
	}
	f.sampler = sampler
	f.jobTypes = jobTypes
	f.redisFetchScript = redis.NewScript(len(jobTypes)*fetchKeysPerJobType, redisLuaFetchJob)
}

func (f *Fetcher) start(uri string) {
	go f.loop(uri)
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
			task, err := f.fetch(uri)
			if err != nil {
				logError("fetcher.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if task != nil {
				f.enqueue(task)
				time.Sleep(10 * time.Second)
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
