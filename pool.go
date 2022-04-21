package work

import (
	"github.com/gomodule/redigo/redis"
	"github.com/robfig/cron/v3"
	"reflect"
)

// Pool represents a pool of processors.
// It is the base structure of a pool which need to be implemented by adding process group or custom design items.
// You can attach jobs and middlware to them. You can start and stop them.
// Based on their concurrency setting, they'll spin up N worker goroutines.
type Pool struct {
	kind          string // fetcherPool workerPool ...
	poolID        string
	namespace     string // eg, "am" which represents a namespace within redis
	pool          *redis.Pool
	sleepBackoffs []int64

	contextType  reflect.Type
	jobTypes     map[string]*jobType
	middlewares  []*middlewareHandler
	started      bool
	periodicJobs []*periodicJob

	heartbeater      *poolHeartbeater
	retrier          *requeuer
	scheduler        *requeuer
	deadPoolReaper   *deadPoolReaper
	periodicEnqueuer *periodicEnqueuer
}

// newPoolWithOptions creates a new base pool, and permits you to specify
// additional options such as sleep backoffs.
func newPoolWithOptions(ctx interface{}, namespace, kind, identifier string, pool *redis.Pool, poolOpts PoolOptions) *Pool {
	if pool == nil {
		panic("NewPool needs a non-nil *redis.Pool")
	}

	ctxType := reflect.TypeOf(ctx)
	validateContextType(ctxType)
	p := &Pool{
		kind:          kind,
		poolID:        makeIdentifier(identifier),
		pool:          pool,
		namespace:     namespace,
		sleepBackoffs: poolOpts.SleepBackoffs,
		contextType:   ctxType,
		jobTypes:      make(map[string]*jobType),
	}

	return p
}

// Middleware appends the specified function to the middleware chain. The fn can take one of these forms:
// (*ContextType).func(*Job, NextMiddlewareFunc) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Job, NextMiddlewareFunc) error, for the generic middleware format.
func (p *Pool) middleware(fn interface{}) *Pool {
	vfn := reflect.ValueOf(fn)
	validateMiddlewareType(p.contextType, vfn)

	mw := &middlewareHandler{
		DynamicMiddleware: vfn,
	}

	if gmh, ok := fn.(func(*Job, NextMiddlewareFunc) error); ok {
		mw.IsGeneric = true
		mw.GenericMiddlewareHandler = gmh
	}

	p.middlewares = append(p.middlewares, mw)

	return p
}

// Job registers the job name to the specified handler fn. For instance, when workers pull jobs from the name queue they'll be processed by the specified handler function.
// fn can take one of these forms:
// (*ContextType).func(*Job) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Job) error, for the generic handler format.
func (p *Pool) job(name string, fn interface{}) *Pool {
	return p.jobWithOptions(name, JobOptions{}, fn)
}

// JobWithOptions adds a handler for 'name' jobs as per the Job function, but permits you specify additional options
// such as a job's priority, retry count, and whether to send dead jobs to the dead job queue or trash them.
func (p *Pool) jobWithOptions(name string, jobOpts JobOptions, fn interface{}) *Pool {
	jobOpts = applyDefaultsAndValidate(jobOpts)

	vfn := reflect.ValueOf(fn)
	validateHandlerType(p.contextType, vfn)
	jt := &jobType{
		Name:           name,
		DynamicHandler: vfn,
		JobOptions:     jobOpts,
	}
	if gh, ok := fn.(func(*Job) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	p.jobTypes[name] = jt

	return p
}

// PeriodicallyEnqueue will periodically enqueue jobName according to the cron-based spec.
// The spec format is based on https://godoc.org/github.com/robfig/cron, which is a relatively standard cron format.
// Note that the first value is the seconds!
// If you have multiple worker pools on different machines, they'll all coordinate and only enqueue your job once.
func (pl *Pool) periodicallyEnqueue(spec string, jobName string) *Pool {
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	schedule, err := p.Parse(spec)
	if err != nil {
		panic(err)
	}

	pl.periodicJobs = append(pl.periodicJobs, &periodicJob{jobName: jobName, spec: spec, schedule: schedule})

	return pl
}

// Start starts the workers and associated processes.
func (p *Pool) start(kind string, total int, concurrences []byte, ids []string) {
	if p.started {
		return
	}
	p.started = true

	// TODO: we should cleanup stale keys on startup from previously registered jobs
	p.writeConcurrencyControlsToRedis()
	go p.writeKnownJobsToRedis()

	p.heartbeater = newPoolHeartbeater(p.namespace, p.poolID, total, p.pool, p.jobTypes, concurrences, ids)
	p.heartbeater.start(kind)
	p.startRequeuers(p.jobTypes, p.pool)
	p.periodicEnqueuer = newPeriodicEnqueuer(p.namespace, p.pool, p.periodicJobs)
	p.periodicEnqueuer.start()
}

// Stop stops the workers and associated processes.
func (p *Pool) stop() {
	p.heartbeater.stop()
	p.retrier.stop()
	p.scheduler.stop()
	p.deadPoolReaper.stop()
	p.periodicEnqueuer.stop()
}

func (p *Pool) startRequeuers(jobTypes map[string]*jobType, pool *redis.Pool) {
	jobNames := make([]string, 0, len(jobTypes))
	for k := range jobTypes {
		jobNames = append(jobNames, k)
	}
	p.retrier = newRequeuer(p.namespace, pool, redisKeyRetry(p.namespace), jobNames)
	p.scheduler = newRequeuer(p.namespace, pool, redisKeyScheduled(p.namespace), jobNames)
	p.deadPoolReaper = newDeadPoolReaper(p.namespace, pool, jobNames)
	p.retrier.start()
	p.scheduler.start()
	p.deadPoolReaper.start()
}

func (p *Pool) writeKnownJobsToRedis() {
	if len(p.jobTypes) == 0 {
		return
	}

	conn := p.pool.Get()
	defer conn.Close()
	jobNames := make([]interface{}, 0, len(p.jobTypes)+1)
	key := redisKeyKnownJobs(p.namespace)
	jobNames = append(jobNames, key)
	for k := range p.jobTypes {
		jobNames = append(jobNames, k)
	}

	if _, err := conn.Do("SADD", jobNames...); err != nil {
		logError("write_known_jobs", err)
	}
}

func (p *Pool) writeConcurrencyControlsToRedis() {
	if len(p.jobTypes) == 0 {
		return
	}

	conn := p.pool.Get()
	defer conn.Close()
	for jobName, jobType := range p.jobTypes {
		if _, err := conn.Do("SET", redisKeyJobsConcurrency(p.namespace, jobName), jobType.MaxConcurrency); err != nil {
			logError("write_concurrency_controls_max_concurrency", err)
		}
	}
}

func applyDefaultsAndValidate(jobOpts JobOptions) JobOptions {
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
