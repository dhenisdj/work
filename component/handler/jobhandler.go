package handler

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/context"
	"math/rand"
	"time"
)

var Handlers = map[bool]func(ctx *context.Context, job *task.Job) error{
	true:  LivyHandlerBatch,
	false: LivyHandlerSingle,
}

func EpsilonHandler(ctx *context.Context, job *task.Job) error {
	ctx.I("epsilon")
	time.Sleep(time.Second)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}

func LivyHandlerSingle(ctx *context.Context, job *task.Job) error {
	b, _ := job.Serialize()
	ctx.If("Start handle LIVY task for %s with context %s", job.Name, b)

	return nil
}

func LivyHandlerBatch(ctx *context.Context, job *task.Job) error {
	b, _ := job.Serialize()
	ctx.If("Start handle LIVY task for %s with context %s", job.Name, b)
	panic("Livy batch interactive session task is not support for now.")

	return nil
}
