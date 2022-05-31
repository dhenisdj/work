package group

import (
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/gomodule/redigo/redis"
	"reflect"
)

type Group struct {
	GroupName   string
	Env         string
	Namespace   string
	GroupID     string
	PoolID      string
	Pool        *redis.Pool
	Ctx         context.Context
	contextType reflect.Type

	Started bool
}

type GroupOption func(g *Group)

func WithContext(ctx context.Context) GroupOption {
	return func(g *Group) {
		g.Ctx = ctx
		g.contextType = reflect.TypeOf(ctx)
	}
}

func WithRedis(pool *redis.Pool) GroupOption {
	return func(g *Group) {
		g.Pool = pool
	}
}

func NewGroup(env, namespace, poolID, groupName string, opts ...GroupOption) *Group {
	g := &Group{
		GroupName: groupName,
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
