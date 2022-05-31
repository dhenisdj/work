package handler

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"time"
)

var Handlers = map[bool]func(ctx context.Context, job *task.Job) error{
	true:  LivyHandlerBatch,
	false: LivyHandlerSingle,
}

func EpsilonHandler(ctx context.Context, job *task.Job) error {
	ctx.I("epsilon")
	time.Sleep(time.Second)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}

func LivyHandlerSingle(ctx context.Context, job *task.Job) error {
	ctx.If("task %s start processing", job.ID)
	api := ctx.Api()
	conn := ctx.Redis().Get()
	// Query session id for task
	querySessionId := func() (int, error) {
		// Get session id of job and check the state in case this is a recovered job from system fail over
		sessionId, err := redis.Int(conn.Do("GET", models.RedisKey2JobSessionId(ctx.CONF().NameSpace, job.ID)))
		if err == redis.ErrNil {
			ctx.Df("session id for task %s not exist", job.ID)
		} else if err != nil {
			ctx.Ef("get session id for task %s error %s", job.ID, err.Error())
			return 0, fmt.Errorf("get session id from redis for job %s error %s", job.ID, err.Error())
		}
		return sessionId, nil
	}
	// Persist session id for task
	persistSessionId := func(sessionId int) error {
		batchSessionKey := models.RedisKey2JobSessionId(ctx.CONF().NameSpace, job.ID)
		conn.Send("SET", batchSessionKey, sessionId)
		conn.Send("EXPIRE", batchSessionKey, 60*60*2)
		if err := conn.Flush(); err != nil {
			ctx.Ef("task %s write session id %d error %s", job.ID, sessionId, err.Error())
			return fmt.Errorf("persist session id %d of task %s error %s", sessionId, job.ID, err.Error())
		}
		return nil
	}
	// Remove session id correspond to task
	cleanSessionId := func() {
		if _, err := conn.Do("DEL", models.RedisKey2JobSessionId(ctx.CONF().NameSpace, job.ID)); err != nil {
			ctx.Wf("session id for %s removed failed", job.ID)
		} else {
			ctx.If("session id for %s removed", job.ID)
		}
	}

	defer conn.Close()

	sessionId, err := querySessionId()
	if err != nil {
		return err
	}

	if sessionId == 0 {
		// Submit task
		session, err := api.BatchSessionInit(&job.Account, job.Args)
		if err != nil {
			ctx.Ef("task %s init error %s", job.ID, err.Error())
			return err
		}
		sessionId = session.Id
		err = persistSessionId(sessionId)
		if err != nil {
			return err
		}
	}
	// Monitor task status
	ticker := time.NewTicker(20 * time.Second)
	for {
		select {
		case <-ticker.C:
			state, err := api.BatchSessionState(sessionId, &job.Account)
			if err != nil {
				return err
			}
			s := state.State
			if s == "" {
				return fmt.Errorf("error message `%s` when query state for task %s, may due to old session created 10 mins ago", state.Msg, job.ID)
			}
			job.Checkin(fmt.Sprintf("time %s state %s", time.Now().Format("2006-01-02 15:04:05"), s))
			ctx.If("task %s is in %s state", job.ID, s)
			if s == "starting" {
				continue
			} else if s == "running" {
				continue
			} else if s == "success" {
				cleanSessionId()
				return nil
			} else {
				// If in other state, will clean the session id relation and then fail to retry or finally fail
				cleanSessionId()
				return fmt.Errorf("task %s with state %s", job.ID, s)
			}
		}

	}
}

func LivyHandlerBatch(ctx context.Context, job *task.Job) error {
	b, _ := job.Serialize()
	ctx.If("start interactive LIVY task for %s with context %s", job.ID, b)
	panic("Livy batch interactive session task is not support for now.")

	return nil
}
