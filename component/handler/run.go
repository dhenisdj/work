package handler

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/context"
	"reflect"
)

// RunJob returns an error if the task fails, or there's a panic, or we couldn't reflect correctly.
// if we return an error, it signals we want the task to be retried.
func RunJob(job *task.Job, ctxType reflect.Type, middleware []*MiddlewareHandler, jt *task.JobType) (returnCtx reflect.Value, returnError error) {
	if ctxType.Kind() == reflect.Ptr {
		ctxType = ctxType.Elem()
	}
	returnCtx = reflect.New(ctxType)
	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			if mw.IsGeneric {
				return mw.GenericMiddlewareHandler(job, next)
			}
			res := mw.DynamicMiddleware.Call([]reflect.Value{returnCtx, reflect.ValueOf(job), reflect.ValueOf(next)})
			x := res[0].Interface()
			if x == nil {
				return nil
			}
			return x.(error)
		}
		if jt.IsGeneric {
			return jt.GenericHandler(job)
		}
		res := jt.DynamicHandler.Call([]reflect.Value{returnCtx, reflect.ValueOf(job)})
		x := res[0].Interface()
		if x == nil {
			return nil
		}
		return x.(error)
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			returnCtx.MethodByName("LE").Call([]reflect.Value{reflect.ValueOf("runJob.panic"), reflect.ValueOf(errorishError)})
			returnError = errorishError
		}
	}()

	returnError = next()

	return
}

func Execute(ctx context.Context, middleware []*MiddlewareHandler, jt *task.JobType, job *task.Job) (returnError error) {
	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			if mw.IsGeneric {
				return mw.GenericMiddlewareHandler(job, next)
			}
			res := mw.DynamicMiddleware.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(job), reflect.ValueOf(next)})
			x := res[0].Interface()
			if x == nil {
				return nil
			}
			return x.(error)
		}
		if jt.IsGeneric {
			return jt.GenericHandler(job)
		}
		res := jt.DynamicHandler.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(job)})
		x := res[0].Interface()
		if x == nil {
			return nil
		}
		return x.(error)
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			ctx.LE("Execute.panic", errorishError)
			returnError = errorishError
		}
	}()

	returnError = next()

	return
}
