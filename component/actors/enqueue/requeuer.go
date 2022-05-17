package enqueue

import (
	"fmt"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/utils"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Requeuer struct {
	namespace string
	pool      *redis.Pool
	ctx       *context.Context

	redisRequeueScript *redis.Script
	redisRequeueArgs   []interface{}

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func NewRequeuer(ctx *context.Context, namespace string, pool *redis.Pool, requeueKey string, jobNames []string) *Requeuer {
	args := make([]interface{}, 0, len(jobNames)+2+2)
	args = append(args, requeueKey)                         // KEY[1]
	args = append(args, models.RedisKey2JobDead(namespace)) // KEY[2]
	for _, jobName := range jobNames {
		args = append(args, models.RedisKey2Job(namespace, jobName)) // KEY[3, 4, ...]
	}
	args = append(args, models.RedisKey2JobPrefix(namespace)) // ARGV[1]
	args = append(args, 0)                                    // ARGV[2] -- NOTE: We're going to change this one on every call

	return &Requeuer{
		namespace: namespace,
		pool:      pool,
		ctx:       ctx,

		redisRequeueScript: redis.NewScript(len(jobNames)+2, models.RedisLuaZremLpushCmd),
		redisRequeueArgs:   args,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (r *Requeuer) Start() {
	go r.loop()
}

func (r *Requeuer) Stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *Requeuer) drain() {
	r.drainChan <- struct{}{}
	<-r.doneDrainingChan
}

func (r *Requeuer) loop() {
	// Just do this simple thing for now.
	// If we have 100 processes all running requeuers,
	// there's probably too much hitting redi.
	// So later on we'l have to implement exponential backoff
	ticker := time.Tick(1000 * time.Millisecond)

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-r.drainChan:
			for r.process() {
			}
			r.doneDrainingChan <- struct{}{}
		case <-ticker:
			for r.process() {
			}
		}
	}
}

func (r *Requeuer) process() bool {
	conn := r.pool.Get()
	defer conn.Close()

	r.redisRequeueArgs[len(r.redisRequeueArgs)-1] = utils.NowEpochSeconds()

	res, err := redis.String(r.redisRequeueScript.Do(conn, r.redisRequeueArgs...))
	if err == redis.ErrNil {
		return false
	} else if err != nil {
		r.ctx.LE("requeuer.process", err)
		return false
	}

	if res == "" {
		return false
	} else if res == "dead" {
		r.ctx.LE("requeuer.process.dead", fmt.Errorf("no task name"))
		return true
	} else if res == "ok" {
		return true
	}

	return false
}
