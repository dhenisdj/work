package fetch

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/execute/parse"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"io/ioutil"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Fetcher struct {
	parser                *parse.Parser
	enqueue               *enqueue.Enqueuer
	credential            *entities.Credential
	account               *entities.Account
	FetcherID             string
	poolID                string
	groupID               string
	namespace             string
	executor              string
	businessGroup         string
	url                   string
	env                   string
	jobTypes              *task.JobType
	contextType           reflect.Type
	ctx                   context.Context
	sleepBackoffs         []int64
	redisCanPollJobScript *redis.Script

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
		f.credential = credential
	}
}

func WithParser(parser *parse.Parser) FetcherOption {
	return func(f *Fetcher) {
		f.parser = parser
	}
}

func WithContext(ctx context.Context) FetcherOption {
	return func(f *Fetcher) {
		f.ctx = ctx
		f.contextType = reflect.TypeOf(ctx)
	}
}

func WithAccount(account *entities.Account) FetcherOption {
	return func(f *Fetcher) {
		f.account = account
	}
}

func WithBackoffs(sleepBackoffs []int64) FetcherOption {
	return func(f *Fetcher) {
		f.sleepBackoffs = sleepBackoffs
	}
}

func WithEnqueuer(enqueuer *enqueue.Enqueuer) FetcherOption {
	return func(f *Fetcher) {
		f.enqueue = enqueuer
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

// UpdateJobTypes note: can't be called while the thing is started
func (f *Fetcher) UpdateJobTypes(jobType *task.JobType) {
	f.jobTypes = jobType
	f.redisCanPollJobScript = redis.NewScript(config.CheckKeysIsJobReachLimit, models.RedisLuaCanPollJob)
}

// signedURL can change attributes of cl
func (f *Fetcher) signedURL(uri string) string {
	rawUrl := f.url + uri

	nonce := utils.CreateNonce()
	timeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	text := strings.Join([]string{f.credential.Key, nonce, timeStamp}, "|")
	mac := hmac.New(sha1.New, []byte(f.credential.Secret))
	mac.Write([]byte(text))
	sign := hex.EncodeToString(mac.Sum(nil))

	//fmt.Printf("sign %s\n", sign)

	params := url.Values{}
	Url, err := url.Parse(rawUrl)
	if err != nil {
		fmt.Printf("Error building url path: %s", err.Error())
	}

	if f.executor == "SyncDistributionCommon" {
		params.Set("business_name", f.businessGroup)
	} else {
		params.Set("business_group", f.businessGroup)
	}
	params.Set("executor", f.executor)
	params.Set("nonce", nonce)
	params.Set("timestamp", timeStamp)
	params.Set("sign", sign)

	Url.RawQuery = params.Encode()
	urlPath := Url.String()

	return urlPath
}

func (f *Fetcher) canPollJob() bool {
	f.ctx.Df("canPollJob checking for %s", f.executor+strings.ToUpper(f.businessGroup))
	numKeys := config.CheckKeysIsJobReachLimit
	var scriptArgs = make([]interface{}, 0, numKeys)
	scriptArgs = append(scriptArgs, models.RedisKey2JobLock(f.namespace, f.jobTypes.Name))
	scriptArgs = append(scriptArgs, models.RedisKey2JobConcurrency(f.namespace, f.jobTypes.Name))
	conn := f.ctx.Redis().Get()
	defer conn.Close()
	canPoll, err := redis.Bool(f.redisCanPollJobScript.Do(conn, scriptArgs...))
	if err == redis.ErrNil {
		f.ctx.Df("canPollJob checking and nil err from redis")
	} else if err != nil {
		f.ctx.Ef("canPollJob checking error %s", err.Error())
		return false
	}
	return canPoll
}

func (f *Fetcher) fetch(uri string) (*task.Job, error) {
	// Check if the active process job count reach limit
	if f.canPollJob() {
		// Poll task by API
		api := f.ctx.Api()
		url := f.signedURL(uri)

		request, err := api.Request("GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch build request error %s", err.Error())
		}

		resp, err := api.DoRequest(request)
		if err != nil {
			return nil, fmt.Errorf("fetch do request error %s", err.Error())
		}

		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("fetch read body error %s", err.Error())
		}

		response := &entities.Response{}
		err = json.Unmarshal(body, response)
		if err != nil {
			return nil, fmt.Errorf("fetch unmarshal body error %s", err.Error())
		}

		if response.Code != 0 {
			// deal with error from backend
			return nil, fmt.Errorf("fetch response error with code %d, msg %s", response.Code, response.Message)
		}

		if response.Task == (entities.BaseTask{}) {
			f.ctx.Df("task unavailable now for %s", f.executor+strings.ToUpper(f.businessGroup))
			return nil, nil
		}

		response.Task.Env = f.env

		return f.parser.Parse(&response.Task), nil
	}

	return nil, nil
}

func (f *Fetcher) start(uri string) {
	go f.loop(uri)
	f.ctx.If("fetcher %s for %s started!", f.FetcherID, f.jobTypes.Name)
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
				timer.Reset(5 * time.Second)
			} else if job != nil {
				f.enqueue.EnqueueSpark(job)
				time.Sleep(2 * time.Second)
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
				timer.Reset(time.Duration(f.sleepBackoffs[idx]) * time.Second)
			}
		}
	}
}
