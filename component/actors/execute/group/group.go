package group

import (
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/gomodule/redigo/redis"
	"reflect"
)

type Group struct {
	Env         string
	Namespace   string
	GroupID     string
	PoolID      string
	Pool        *redis.Pool
	ctx         *context.Context
	contextType reflect.Type

	Started bool
}

type GroupOption func(g *Group)

func WithContext(ctx *context.Context) GroupOption {
	return func(g *Group) {
		g.ctx = ctx
		g.contextType = reflect.TypeOf(*ctx)
	}
}

func WithRedis(pool *redis.Pool) GroupOption {
	return func(g *Group) {
		g.Pool = pool
	}
}

func NewGroup(env, namespace, poolID string, opts ...GroupOption) *Group {
	g := &Group{
		Namespace: namespace,
		GroupID:   utils.MakeIdentifier(),
		PoolID:    poolID,
		Env:       env,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}
