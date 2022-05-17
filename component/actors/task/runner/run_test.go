package runner

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/dhenisdj/scheduler/component/helper"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunBasicMiddleware(t *testing.T) {
	mw1 := func(j *task.Job, next handler.NextMiddlewareFunc) error {
		j.SetArg("mw1", "mw1")
		return next()
	}

	mw2 := func(c *helper.TstCtx, j *task.Job, next handler.NextMiddlewareFunc) error {
		c.Record(j.Args["mw1"].(string))
		c.Record("mw2")
		return next()
	}

	mw3 := func(c *helper.TstCtx, j *task.Job, next handler.NextMiddlewareFunc) error {
		c.Record("mw3")
		return next()
	}

	h1 := func(c *helper.TstCtx, j *task.Job) error {
		c.Record("h1")
		c.Record(j.Args["a"].(string))
		return nil
	}

	middleware := []*handler.MiddlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw2)},
		{IsGeneric: false, DynamicMiddleware: reflect.ValueOf(mw3)},
	}

	jt := &task.JobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &task.Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	v, err := RunJob(job, context.ContextType, middleware, jt)
	assert.NoError(t, err)
	c := v.Interface().(*helper.TstCtx)
	assert.Equal(t, "mw1mw2mw3h1foo", c.String())
}

func TestRunHandlerError(t *testing.T) {
	mw1 := func(j *task.Job, next handler.NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *helper.TstCtx, j *task.Job) error {
		c.Record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*handler.MiddlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &task.JobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &task.Job{
		Name: "foo",
	}

	v, err := RunJob(job, helper.TstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "h1_err", err.Error())

	c := v.Interface().(*helper.TstCtx)
	assert.Equal(t, "h1", c.String())
}

func TestRunMwError(t *testing.T) {
	mw1 := func(j *task.Job, next handler.NextMiddlewareFunc) error {
		return fmt.Errorf("mw1_err")
	}
	h1 := func(c *helper.TstCtx, j *task.Job) error {
		c.Record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*handler.MiddlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &task.JobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &task.Job{
		Name: "foo",
	}

	_, err := RunJob(job, helper.TstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "mw1_err", err.Error())
}

func TestRunHandlerPanic(t *testing.T) {
	mw1 := func(j *task.Job, next handler.NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *helper.TstCtx, j *task.Job) error {
		c.Record("h1")

		panic("dayam")
	}

	middleware := []*handler.MiddlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &task.JobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &task.Job{
		Name: "foo",
	}

	_, err := RunJob(job, helper.TstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

func TestRunMiddlewarePanic(t *testing.T) {
	mw1 := func(j *task.Job, next handler.NextMiddlewareFunc) error {
		panic("dayam")
	}
	h1 := func(c *helper.TstCtx, j *task.Job) error {
		c.Record("h1")
		return nil
	}

	middleware := []*handler.MiddlewareHandler{
		{IsGeneric: true, GenericMiddlewareHandler: mw1},
	}

	jt := &task.JobType{
		Name:           "foo",
		IsGeneric:      false,
		DynamicHandler: reflect.ValueOf(h1),
	}

	job := &task.Job{
		Name: "foo",
	}

	_, err := RunJob(job, helper.TstCtxType, middleware, jt)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}
