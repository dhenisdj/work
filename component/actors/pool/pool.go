package pool

import (
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/pool/heartbeat"
	"github.com/dhenisdj/scheduler/component/actors/pool/reape"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"github.com/gomodule/redigo/redis"
	"github.com/robfig/cron/v3"
	"reflect"
)

// You may provide your own backoff function for retrying failed jobs or use the builtin one.
// Returns the number of seconds to wait until the next attempt.
//
// PoolOptions can be passed to NewWorkerPoolWithOptions.
type PoolOptions struct {
	SleepBackoffs []int64 // Sleep backoffs in milliseconds
}

// Pool represents a pool of processors.
// It is the base structure of a pool which need to be implemented by adding process group or custom design items.
// You can attach jobs and middlware to them. You can start and stop them.
// Based on their concurrency setting, they'll spin up N work goroutines.
type Pool struct {
	kind           string // fetcherPool workerPool ...
	PoolID         string
	Namespace      string // eg, "am" which represents a Namespace within redis
	env            string
	Redis          *redis.Pool
	sleepBackoffs  []int64
	concurrencyMap map[string]int

	contextType reflect.Type
	ctx         context.Context
	jobTypes    map[string]*task.JobType
	middlewares []*handler.MiddlewareHandler
	started     bool

	periodicJobs     []*task.PeriodicJob
	periodicEnqueuer *enqueue.PeriodicEnqueuer

	heartbeater    *heartbeat.PoolHeartbeater
	retrier        *enqueue.Requeuer
	scheduler      *enqueue.Requeuer
	DeadPoolReaper *reape.DeadPoolReaper
}

type PoolOption func(p *Pool)

func WithContext(ctx context.Context) PoolOption {
	return func(p *Pool) {
		p.ctx = ctx
		p.contextType = reflect.TypeOf(ctx)
		p.env = ctx.CONF().Env
	}
}

func WithBackoffs(sleepBackoffs []int64) PoolOption {
	return func(f *Pool) {
		f.sleepBackoffs = sleepBackoffs
	}
}

func WithConcurrency(concurrency map[string]int) PoolOption {
	return func(f *Pool) {
		f.concurrencyMap = concurrency
	}
}

func WithRedis(pool *redis.Pool) PoolOption {
	return func(f *Pool) {
		f.Redis = pool
	}
}

// newPool creates a new base pool, and permits you to specify
// additional options such as sleep backoffs.
func newPool(namespace, kind string, opts ...PoolOption) *Pool {
	p := &Pool{
		kind:      kind,
		PoolID:    utils.MakeIdentifier(),
		Namespace: namespace,
		jobTypes:  make(map[string]*task.JobType),
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.Redis == nil {
		panic("NewPool needs a non-nil *redis.Pool")
	}

	utils.ValidateContextType(p.contextType)

	if len(p.sleepBackoffs) == 0 {
		p.sleepBackoffs = config.SleepBackoffsInSeconds
	}

	return p
}

// Middleware appends the specified function to the middleware chain. The fn can take one of these forms:
// (*ContextType).func(*Job, NextMiddlewareFunc) error, (ContextType matches the type of ctx specified when creating a Pool)
// func(*Job, NextMiddlewareFunc) error, for the generic middleware format.
func (p *Pool) middleware(fn interface{}) *Pool {
	vfn := reflect.ValueOf(fn)
	ValidateMiddlewareType(p.contextType, vfn)

	mw := &handler.MiddlewareHandler{
		DynamicMiddleware: vfn,
	}

	if gmh, ok := fn.(func(*task.Job, handler.NextMiddlewareFunc) error); ok {
		mw.IsGeneric = true
		mw.GenericMiddlewareHandler = gmh
	}

	p.middlewares = append(p.middlewares, mw)

	return p
}

// Job registers the job name to the specified handler fn. For instance, when workers pull jobs from the name queue they'll be processed by the specified handler function.
// fn can take one of these forms:
// (*ContextType).func(*Job) error, (ContextType matches the type of ctx specified when creating a Pool)
// func(*Job) error, for the generic handler format.
func (p *Pool) job(name string, fn interface{}) *Pool {
	return p.jobWithOptions(name, task.JobOptions{}, fn)
}

// JobWithOptions adds a handler for 'name' jobs as per the Job function, but permits you specify additional options
// such as a job's priority, retry count, and whether to send dead jobs to the dead job queue or trash them.
func (p *Pool) jobWithOptions(name string, jobOpts task.JobOptions, fn interface{}) *Pool {
	jobOpts = applyDefaultsAndValidate(jobOpts)

	vfn := reflect.ValueOf(fn)
	ValidateHandlerType(p.contextType, vfn)
	jt := &task.JobType{
		Name:           name,
		DynamicHandler: vfn,
		JobOptions:     jobOpts,
	}
	if gh, ok := fn.(func(*task.Job) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	p.jobTypes[name] = jt

	return p
}

// PeriodicallyEnqueue will periodically enqueue jobName according to the cron-based spec.
// The spec format is based on https://godoc.org/github.com/robfig/cron, which is a relatively standard cron format.
// Note that the first value is the seconds!
// If you have multiple work pools on different machines, they'll all coordinate and only enqueue your job once.
func (pl *Pool) periodicallyEnqueue(spec string, jobName string) *Pool {
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	schedule, err := p.Parse(spec)
	if err != nil {
		panic(err)
	}

	pl.periodicJobs = append(pl.periodicJobs, &task.PeriodicJob{JobName: jobName, Spec: spec, Schedule: schedule})

	return pl
}

// Start starts the workers and associated processes.
func (p *Pool) start(ids []string) {
	if p.started {
		return
	}
	p.started = true

	// TODO: we should cleanup stale keys on startup from previously registered jobs
	p.writeJobsConfigToRedis()
	go p.writeValidJobsToRedis()

	p.heartbeater = heartbeat.NewPoolHeartbeater(p.ctx, p.Namespace, p.kind, p.PoolID, p.jobTypes, p.concurrencyMap, ids)
	p.heartbeater.Start()
	p.startRequeuers(p.jobTypes)
	p.periodicEnqueuer = enqueue.NewPeriodicEnqueuer(p.ctx, p.Namespace, p.Redis, p.periodicJobs)
	p.periodicEnqueuer.Start()
	p.ctx.If("base pool %s started for %s", p.PoolID, p.kind)
}

// Stop stops the workers and associated processes.
func (p *Pool) stop() {
	p.heartbeater.Stop()
	p.retrier.Stop()
	p.scheduler.Stop()
	p.DeadPoolReaper.Stop()
	p.periodicEnqueuer.Stop()
}

func (p *Pool) startRequeuers(jobTypes map[string]*task.JobType) {
	jobNames := make([]string, 0, len(jobTypes))
	for k := range jobTypes {
		jobNames = append(jobNames, k)
	}
	p.retrier = enqueue.NewRequeuer(p.ctx, p.Namespace, models.RedisKey2JobRetry(p.Namespace), jobNames)
	p.scheduler = enqueue.NewRequeuer(p.ctx, p.Namespace, models.RedisKey2JobScheduled(p.Namespace), jobNames)
	p.DeadPoolReaper = reape.NewDeadPoolReaper(p.ctx, p.Namespace, jobNames)
	p.retrier.Start()
	p.scheduler.Start()
	p.DeadPoolReaper.Start()
	p.ctx.I("requeuers started!")
}

func (p *Pool) writeValidJobsToRedis() {
	if len(p.jobTypes) == 0 {
		return
	}

	conn := p.Redis.Get()
	defer conn.Close()
	jobNames := make([]interface{}, 0, len(p.jobTypes)+1)
	key := models.RedisKey2ValidJobs(p.Namespace)
	jobNames = append(jobNames, key)
	for k := range p.jobTypes {
		jobNames = append(jobNames, k)
	}

	if _, err := conn.Do("SADD", jobNames...); err != nil {
		p.ctx.LE("write valid jobs", err)
	}
}

func (p *Pool) writeJobsConfigToRedis() {
	if len(p.jobTypes) == 0 {
		return
	}

	conn := p.Redis.Get()
	defer conn.Close()
	for jobName, jobType := range p.jobTypes {
		//if err := conn.Send("HMSET", models.RedisKey2JobType(p.Namespace, jobName),
		//	"name", jobName,
		//	"isGeneric", jobType.IsGeneric,
		//	"time", utils.NowEpochSeconds(),
		//	"maxConcurrency", jobType.MaxConcurrency,
		//	"isBatch", jobType.IsBatch,
		//	"maxFails", jobType.MaxFails,
		//	"skipDead", jobType.SkipDead,
		//	"priority", jobType.Priority,
		//); err != nil {
		//	p.ctx.LE("Write Job type config error", err)
		//}
		if _, err := conn.Do("SET", models.RedisKey2JobConcurrency(p.Namespace, jobName), jobType.MaxConcurrency); err != nil {
			p.ctx.LE("write_concurrency_controls_max_concurrency", err)
		}
	}
}

func applyDefaultsAndValidate(jobOpts task.JobOptions) task.JobOptions {
	if jobOpts.Priority == 0 {
		jobOpts.Priority = 1
	}

	if jobOpts.MaxFails == 0 {
		jobOpts.MaxFails = 4
	}

	if jobOpts.Priority > 100000 {
		panic("work: JobOptions.Priority must be between 1 and 100000")
	}

	return jobOpts
}

func ValidateHandlerType(ctxType reflect.Type, vfn reflect.Value) {
	if !IsValidHandlerType(ctxType, vfn) {
		panic(utils.InstructiveMessage(vfn, "a handler", "handler", "job *task.Job", ctxType))
	}
}

func ValidateMiddlewareType(ctxType reflect.Type, vfn reflect.Value) {
	if !IsValidMiddlewareType(ctxType, vfn) {
		panic(utils.InstructiveMessage(vfn, "middleware", "middleware", "job *task.Job, next NextMiddlewareFunc", ctxType))
	}
}

func IsValidHandlerType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *task.Job
	if numIn == 1 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
	} else if numIn == 2 {
		//if fnType.In(0) != reflect.PtrTo(ctxType) {
		//	return false
		//}
		if !ctxType.Implements(fnType.In(0)) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
	} else {
		return false
	}

	return true
}

func IsValidMiddlewareType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *task.Job
	var nfn handler.NextMiddlewareFunc
	if numIn == 2 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(nfn) {
			return false
		}
	} else if numIn == 3 {
		if fnType.In(0) != reflect.PtrTo(ctxType) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(2) != reflect.TypeOf(nfn) {
			return false
		}
	} else {
		return false
	}

	return true
}
