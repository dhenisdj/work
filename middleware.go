package work

import "reflect"

// You may provide your own backoff function for retrying failed jobs or use the builtin one.
// Returns the number of seconds to wait until the next attempt.
//
// The builtin backoff calculator provides an exponentially increasing wait function.
type BackoffCalculator func(job *Job) int64

// PoolOptions can be passed to NewWorkerPoolWithOptions.
type PoolOptions struct {
	SleepBackoffs []int64 // Sleep backoffs in milliseconds
}

// GenericHandler is a job handler without any custom context.
type GenericHandler func(*Job) error

// GenericMiddlewareHandler is a middleware without any custom context.
type GenericMiddlewareHandler func(*Job, NextMiddlewareFunc) error

// NextMiddlewareFunc is a function type (whose instances are named 'next') that you call to advance to the next middleware.
type NextMiddlewareFunc func() error

type middlewareHandler struct {
	IsGeneric                bool
	DynamicMiddleware        reflect.Value
	GenericMiddlewareHandler GenericMiddlewareHandler
}
