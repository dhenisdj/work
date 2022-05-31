package work

import (
	"fmt"
	"github.com/beltran/gohive"
	"github.com/dhenisdj/scheduler/component/actors/execute/observe"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Worker struct {
	env                         string
	isBatch                     bool
	sessionRenewIntervalSeconds int
	namespace                   string
	WorkerID                    string
	poolID                      string
	groupID                     string
	jobType                     *task.JobType
	sleepBackoffs               []int64

	// Livy configuration
	account *entities.Account

	// Context configuration
	contextType reflect.Type
	ctx         context.Context
	middleware  []*handler.MiddlewareHandler

	// Thrift connect configuration
	conn        *gohive.Connection
	concurrency int

	// Redis configuration
	redisFetchScript *redis.Script
	sampler          models.PrioritySampler
	*observe.Observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

type WorkerOption func(w *Worker)

func WithContext(ctx context.Context) WorkerOption {
	return func(w *Worker) {
		w.ctx = ctx
		w.contextType = reflect.TypeOf(ctx)
	}
}

func WithBackoffs(sleepBackoffs []int64) WorkerOption {
	return func(w *Worker) {
		w.sleepBackoffs = sleepBackoffs
	}
}

func WithSingleSession() WorkerOption {
	return func(w *Worker) {
		w.isBatch = false
		w.sessionRenewIntervalSeconds = 0
	}
}

func WithBatchSession(businessGroup string, session *entities.Session, business *entities.Business, config *entities.SparkConfig) WorkerOption {
	return func(w *Worker) {
		w.isBatch = session.IsBatch
		w.sessionRenewIntervalSeconds = session.RenewIntervalSeconds
		configuration := gohive.NewConnectConfiguration()
		// If it's not set it will be picked up from the logged user
		account := business.Account[businessGroup]
		configuration.Username = account.Name
		// This may not be necessary
		configuration.Password = account.Password
		// TODO: the sparkConf key need to add prefix `livy.session.conf.` and below config need to be added too
		// livy.session.name	         If set, will be used as YARN application name. Please note, Livy doesn't allow two session with same name exist.
		// livy.session.queue	         Yarn queue
		// livy.session.executorMemory	 Execute memory
		// livy.session.executorCores	 Executor CPU core
		// livy.session.driverMemory	 Driver memory
		// livy.session.driverCores      Driver CPU core

		// TODO: add dependencies to `spark.jars` and so on
		// deps := config.SparkDependency

		conf := config.SparkConf
		m := make(map[string]string, len(*conf))
		for k, v := range *conf {
			m[fmt.Sprintf("livy.session.conf.%s", k)] = v
		}
		configuration.HiveConfiguration = m

		thrift := config.Livy.Thrift
		connection, errConn := gohive.Connect(thrift.Host, thrift.Port, "NONE", configuration)

		// Concurrency control
		concurrency := business.Concurrency[businessGroup]
		w.concurrency = concurrency
		//for i := 0; i < concurrency; i++ {
		//	connection.Cursor()
		//}

		if errConn != nil {
			w.ctx.LE("Build interactive session error with: ", errConn)
			panic(fmt.Sprintf("Can not initialize interactive session with error %s", errConn.Error()))
		}
		w.conn = connection
	}
}

func NewWorker(namespace, workerID, groupID, poolID string, opts ...WorkerOption) *Worker {

	w := &Worker{
		WorkerID:  workerID,
		groupID:   groupID,
		poolID:    poolID,
		namespace: namespace,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	w.Observer = observe.NewObserver(w.ctx, namespace, workerID)
	return w
}

// UpdateMiddlewareAndJobTypes note: can't be called while the thing is started
func (w *Worker) UpdateMiddlewareAndJobTypes(middleware []*handler.MiddlewareHandler, jobType *task.JobType) {
	w.middleware = middleware
	sampler := models.PrioritySampler{}
	sampler.Add(jobType.Priority,
		models.RedisKey2Job(w.namespace, jobType.Name),
		models.RedisKey2JobInProgress(w.namespace, w.poolID, jobType.Name),
		models.RedisKey2JobPaused(w.namespace, jobType.Name),
		models.RedisKey2JobLock(w.namespace, jobType.Name),
		models.RedisKey2JobLockInfo(w.namespace, jobType.Name),
		models.RedisKey2JobConcurrency(w.namespace, jobType.Name))
	w.sampler = sampler
	w.jobType = jobType
	w.redisFetchScript = redis.NewScript(config.FetchKeysPerJobType, models.RedisLuaFetchJob)
}

func (w *Worker) start() {
	go w.loop()
	go w.Observer.Start()
	w.ctx.If("worker %s for %s started!", w.WorkerID, w.jobType.Name)
}

func (w *Worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
	w.Observer.Drain()
	w.Observer.Stop()
}

func (w *Worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
	w.Observer.Drain()
}

func (w *Worker) loop() {
	var drained bool
	var consequtiveNoJobs int64
	var sessionTicker *time.Ticker

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	if w.isBatch {
		sessionTicker = time.NewTicker(time.Duration(w.sessionRenewIntervalSeconds) * time.Second)
	}

	defer func() {
		timer.Stop()
		if w.isBatch {
			sessionTicker.Stop()
		}
	}()

	for {

		if w.isBatch {
			fmt.Println("this is a interactive session")
			select {
			case <-sessionTicker.C:
				w.renewSession()
			default:
				fmt.Println("skip...")
			}
		}

		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob()
			if err != nil {
				w.ctx.LE("worker.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if job != nil {
				w.processJob(job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(w.sleepBackoffs)) {
					idx = int64(len(w.sleepBackoffs)) - 1
				}
				timer.Reset(time.Duration(w.sleepBackoffs[idx]) * time.Millisecond)
			}
		}
	}
}

func (w *Worker) renewSession() {
	api := w.ctx.Api()
	api.Request("POST", "", nil)
	// TODO renew the LIVY session by REST API
	fmt.Println("the interactive session renewing")
}

func (w *Worker) fetchJob() (*task.Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.Sample()
	numKeys := len(w.sampler.Samples) * config.FetchKeysPerJobType
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, s := range w.sampler.Samples {
		scriptArgs = append(scriptArgs, s.RedisJobs, s.RedisJobsInProg, s.RedisJobsPaused, s.RedisJobsLock, s.RedisJobsLockInfo, s.RedisJobsMaxConcurrency) // KEYS[1-6 * N]
	}
	scriptArgs = append(scriptArgs, w.poolID) // ARGV[1]
	conn := w.ctx.Redis().Get()
	defer conn.Close()

	values, err := redis.Values(w.redisFetchScript.Do(conn, scriptArgs...))
	if err == redis.ErrNil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	rawJSON, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response msg not bytes")
	}

	dequeuedFrom, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("response queue not bytes")
	}

	inProgQueue, ok := values[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("response in prog not bytes")
	}

	job, err := task.NewJob(rawJSON, dequeuedFrom, inProgQueue)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (w *Worker) processJob(job *task.Job) {
	if job.Unique {
		updatedJob := w.getAndDeleteUniqueJob(job)
		// This is to support the old way of doing it, where we used the task off the queue and just deleted the unique key
		// Going forward the task on the queue will always be just a placeholder, and we will be replacing it with the
		// updated task extracted here
		if updatedJob != nil {
			job = updatedJob
		}
	}
	var runErr error
	jt := w.jobType
	if jt == nil {
		runErr = fmt.Errorf("stray task: no handler")
		w.ctx.LE("process_job.stray", runErr)
	} else {
		w.ObserveStarted(job.Name, job.ID, job.Args)
		job.Observer = w.Observer // for Checkin
		runErr = handler.Execute(w.ctx, w.middleware, jt, job)
		w.ObserveDone(job.Name, job.ID, runErr)
	}

	fate := terminateOnly
	if runErr != nil {
		job.Failed(runErr)
		fate = w.jobFate(jt, job)
	}
	w.removeJobFromInProgress(job, fate)
}

func (w *Worker) getAndDeleteUniqueJob(job *task.Job) *task.Job {
	var uniqueKey string
	var err error

	if job.UniqueKey != "" {
		uniqueKey = job.UniqueKey
	} else { // For jobs put in queue prior to this change. In the future this can be deleted as there will always be a UniqueKey
		uniqueKey, err = models.RedisKeyUniqueJob(w.namespace, job.Name, job.Args)
		if err != nil {
			w.ctx.LE("worker.delete_unique_job.key", err)
			return nil
		}
	}

	conn := w.ctx.Redis().Get()
	defer conn.Close()

	rawJSON, err := redis.Bytes(conn.Do("GET", uniqueKey))
	if err != nil {
		w.ctx.LE("worker.delete_unique_job.get", err)
		return nil
	}

	_, err = conn.Do("DEL", uniqueKey)
	if err != nil {
		w.ctx.LE("worker.delete_unique_job.del", err)
		return nil
	}

	// Previous versions did not support updated arguments and just set key to 1, so in these cases we should do nothing.
	// In the future this can be deleted, as we will always be getting arguments from here
	if string(rawJSON) == "1" {
		return nil
	}

	// The task pulled off the queue was just a placeholder with no args, so replace it
	jobWithArgs, err := task.NewJob(rawJSON, job.DequeuedFrom, job.InProgQueue)
	if err != nil {
		w.ctx.LE("worker.delete_unique_job.updated_job", err)
		return nil
	}

	return jobWithArgs
}

func (w *Worker) removeJobFromInProgress(job *task.Job, fate terminateOp) {
	conn := w.ctx.Redis().Get()
	defer conn.Close()

	conn.Send("MULTI")
	conn.Send("LREM", job.InProgQueue, 1, job.RawJSON)
	err := conn.Send("DECR", models.RedisKey2JobLock(w.namespace, job.Name))
	if err != nil {
		return
	}
	conn.Send("HINCRBY", models.RedisKey2JobLockInfo(w.namespace, job.Name), w.poolID, -1)
	fate(conn)
	if _, err := conn.Do("EXEC"); err != nil {
		w.ctx.LE("worker.remove_job_from_in_progress.lrem", err)
	}
}

type terminateOp func(conn redis.Conn)

func terminateOnly(_ redis.Conn) { return }
func terminateAndRetry(w *Worker, jt *task.JobType, job *task.Job) terminateOp {
	rawJSON, err := job.Serialize()
	if err != nil {
		w.ctx.LE("worker.terminate_and_retry.serialize", err)
		return terminateOnly
	}
	return func(conn redis.Conn) {
		conn.Send("ZADD", models.RedisKey2JobRetry(w.namespace), utils.NowEpochSeconds()+jt.CalcBackoff(job), rawJSON)
	}
}
func terminateAndDead(w *Worker, job *task.Job) terminateOp {
	rawJSON, err := job.Serialize()
	if err != nil {
		w.ctx.LE("worker.terminate_and_dead.serialize", err)
		return terminateOnly
	}
	return func(conn redis.Conn) {
		// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
		// The max # of jobs seems really horrible. Seems like operations should be on top of it.
		// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
		// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)

		conn.Send("ZADD", models.RedisKey2JobDead(w.namespace), utils.NowEpochSeconds(), rawJSON)
	}
}

func (w *Worker) jobFate(jt *task.JobType, job *task.Job) terminateOp {
	if jt != nil {
		failsRemaining := int64(jt.MaxFails) - job.Fails
		if failsRemaining > 0 {
			return terminateAndRetry(w, jt, job)
		}
		if jt.SkipDead {
			return terminateOnly
		}
	}
	return terminateAndDead(w, job)
}
