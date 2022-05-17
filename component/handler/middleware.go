package handler

import (
	"github.com/dhenisdj/scheduler/component/actors/task"
	"reflect"
)

// GenericMiddlewareHandler is a middleware without any custom context.
type GenericMiddlewareHandler func(*task.Job, NextMiddlewareFunc) error

// NextMiddlewareFunc is a function type (whose instances are named 'next') that you call to advance to the next middleware.
type NextMiddlewareFunc func() error

type MiddlewareHandler struct {
	IsGeneric                bool
	DynamicMiddleware        reflect.Value
	GenericMiddlewareHandler GenericMiddlewareHandler
}
