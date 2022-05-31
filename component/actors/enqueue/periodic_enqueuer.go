package enqueue

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	periodicEnqueuerSleep   = 2 * time.Minute
	periodicEnqueuerHorizon = 4 * time.Minute
)

type PeriodicEnqueuer struct {
	ctx                   context.Context
	namespace             string
	pool                  *redis.Pool
	periodicJobs          []*task.PeriodicJob
	scheduledPeriodicJobs []*task.ScheduledPeriodicJob
	stopChan              chan struct{}
	doneStoppingChan      chan struct{}
}

func NewPeriodicEnqueuer(ctx context.Context, namespace string, pool *redis.Pool, periodicJobs []*task.PeriodicJob) *PeriodicEnqueuer {
	return &PeriodicEnqueuer{
		ctx:              ctx,
		namespace:        namespace,
		pool:             pool,
		periodicJobs:     periodicJobs,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (pe *PeriodicEnqueuer) Start() {
	go pe.loop()
}

func (pe *PeriodicEnqueuer) Stop() {
	pe.stopChan <- struct{}{}
	<-pe.doneStoppingChan
}

func (pe *PeriodicEnqueuer) loop() {
	if len(pe.periodicJobs) == 0 {
		return
	}
	// Begin reaping periodically
	timer := time.NewTimer(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
	defer timer.Stop()

	if pe.shouldEnqueue() {
		err := pe.enqueue()
		if err != nil {
			pe.ctx.LE("periodic_enqueuer.loop.enqueue", err)
		}
	}

	for {
		select {
		case <-pe.stopChan:
			pe.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			timer.Reset(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
			if pe.shouldEnqueue() {
				err := pe.enqueue()
				if err != nil {
					pe.ctx.LE("periodic_enqueuer.loop.enqueue", err)
				}
			}
		}
	}
}

func (pe *PeriodicEnqueuer) enqueue() error {
	now := utils.NowEpochSeconds()
	nowTime := time.Unix(now, 0)
	horizon := nowTime.Add(periodicEnqueuerHorizon)

	conn := pe.pool.Get()
	defer conn.Close()

	for _, pj := range pe.periodicJobs {
		for t := pj.Schedule.Next(nowTime); t.Before(horizon); t = pj.Schedule.Next(t) {
			epoch := t.Unix()
			id := makeUniquePeriodicID(pj.JobName, pj.Spec, epoch)

			job := &task.Job{
				Name: pj.JobName,
				ID:   id,

				// This is technically wrong, but this lets the bytes be identical for the same periodic task instance. If we don't do this, we'd need to use a different approach -- probably giving each periodic task its own history of the past 100 periodic jobs, and only scheduling a task if it's not in the history.
				EnqueuedAt: epoch,
				Args:       nil,
			}

			rawJSON, err := job.Serialize()
			if err != nil {
				return err
			}

			_, err = conn.Do("ZADD", models.RedisKey2JobScheduled(pe.namespace), epoch, rawJSON)
			if err != nil {
				return err
			}
		}
	}

	_, err := conn.Do("SET", models.RedisKeyLastPeriodicEnqueue(pe.namespace), now)

	return err
}

func (pe *PeriodicEnqueuer) shouldEnqueue() bool {
	conn := pe.pool.Get()
	defer conn.Close()

	lastEnqueue, err := redis.Int64(conn.Do("GET", models.RedisKeyLastPeriodicEnqueue(pe.namespace)))
	if err == redis.ErrNil {
		return true
	} else if err != nil {
		pe.ctx.LE("periodic_enqueuer.should_enqueue", err)
		return true
	}

	return lastEnqueue < (utils.NowEpochSeconds() - int64(periodicEnqueuerSleep/time.Minute))
}

func makeUniquePeriodicID(name, spec string, epoch int64) string {
	return fmt.Sprintf("periodic:%s:%s:%d", name, spec, epoch)
}
