package context

import (
	"context"
	"fmt"
	"github.com/dhenisdj/scheduler/component/api"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"github.com/gomodule/redigo/redis"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

type ContextKeyType string

const (
	ContextKeyHost ContextKeyType = "@host"
)

type CancelFunc = context.CancelFunc

type Context interface {
	context.Context

	Api() *api.API
	CONF() *entities.Configuration
	Redis() *redis.Pool

	Sequence() string

	WithValue(key, val interface{}) Context
	WithDeadline(deadline time.Time) Context
	Cancel()

	WithLogFields(field ...utils.Field) Context
	WithLogSequence() Context

	Dff(msg string, fields ...utils.Field)
	Iff(msg string, fields ...utils.Field)
	Wff(msg string, fields ...utils.Field)
	Eff(msg string, fields ...utils.Field)
	Fff(msg string, fields ...utils.Field)

	Df(fmt string, args ...interface{})
	If(fmt string, args ...interface{})
	Wf(fmt string, args ...interface{})
	Ef(fmt string, args ...interface{})
	Ff(fmt string, args ...interface{})

	D(msg string)
	I(msg string)
	W(msg string)
	E(msg string)
	F(msg string)

	LE(msg string, err error)
	TestReflect()

	Clone() Context
	Source() Context
}

// AContext 通过With...的修改直接生效
type AContext struct {
	context.Context
	*api.API

	Log           *utils.Log
	redisPool     *redis.Pool
	configuration *entities.Configuration
	sequence      string
	cancelFunc    CancelFunc
	source        Context
	logHeader     string
}

func New(env string, cleanRedis bool) Context {
	if !utils.ValidateEnv(env) {
		panic(fmt.Sprintf("Initialization for context failed due to error env %s", env))
	}
	// Initialization for configuration
	conf := config.InitConfig(env)
	return NewContext(NewBackground(), conf, cleanRedis)
}

func NewContext(c context.Context, configuration *entities.Configuration, cleanRedis bool) Context {
	if c == nil {
		c = NewBackground()
	}
	c, cancelFunc := context.WithCancel(c)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	r := configuration.Redis

	red := &redis.Pool{
		MaxActive:   r.MaxActive,
		MaxIdle:     r.MaxIdle,
		IdleTimeout: time.Duration(r.IdleTimeoutSecond) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", r.Addr)
			if err != nil {
				return nil, err
			}
			return c, nil
			//return redis.NewLoggingConn(c, log.New(os.Stdout, "", 0), "redis"), err
		},
		Wait: true,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	if cleanRedis {
		utils.CleanKeyspace(configuration.NameSpace, red)
	}

	return (&AContext{
		Context:       c,
		Log:           utils.InitLog(),
		cancelFunc:    cancelFunc,
		API:           api.NewAPI(configuration.Spark.Livy),
		configuration: configuration,
		redisPool:     red,
		logHeader:     fmt.Sprintf("%s.%s ", strings.ToUpper(configuration.Env), strings.ToUpper(configuration.Region)),
	}).
		WithValue(ContextKeyHost, hostname).WithLogSequence()
}

func NewBackground() context.Context {
	return context.Background()
}

func (c *AContext) Api() *api.API {
	return c.API
}

func (c *AContext) CONF() *entities.Configuration {
	return c.configuration
}

func (c *AContext) Redis() *redis.Pool {
	return c.redisPool
}

func (c *AContext) Clone() Context {
	ctx := *c
	ctx.Log = utils.InitLog() // FIXME: 这里会丢失原先的日志字段，但是没办法，现在这个设计没办法只修改掉sequence字段
	ctx.WithLogSequence()
	ctx.source = c
	return &ctx
}

func (c *AContext) Source() Context {
	return c.source
}

func (c *AContext) WithLogFields(fields ...utils.Field) Context {
	//ctx := c.Clone().(*ctx)
	c.Log.With(fields...)
	return c
}

func (c *AContext) WithLogSequence() Context {
	//ctx := c.Clone().(*ctx)
	c.sequence = utils.Sequence()
	c.Log.With(zap.String("sequence", c.sequence))
	return c
}

func (c *AContext) Sequence() string { return c.sequence }

func (c *AContext) WithDeadline(deadline time.Time) Context {
	ctx, f := context.WithDeadline(c.Context, deadline)
	c.Context = ctx
	c.cancelFunc = f
	return c
}

func (c *AContext) WithValue(key, val interface{}) Context {
	c.Context = context.WithValue(c.Context, key, val)
	return c
}

func (c *AContext) Cancel() {
	c.cancelFunc()
}

func (c *AContext) Dff(msg string, fields ...utils.Field) { c.Log.Dff(c.logHeader+msg, fields...) }
func (c *AContext) Iff(msg string, fields ...utils.Field) { c.Log.Iff(c.logHeader+msg, fields...) }
func (c *AContext) Wff(msg string, fields ...utils.Field) { c.Log.Wff(c.logHeader+msg, fields...) }
func (c *AContext) Eff(msg string, fields ...utils.Field) { c.Log.Eff(c.logHeader+msg, fields...) }
func (c *AContext) Fff(msg string, fields ...utils.Field) { c.Log.Fff(c.logHeader+msg, fields...) }

func (c *AContext) Df(fmt string, args ...interface{}) { c.Log.Df(c.logHeader+fmt, args...) }
func (c *AContext) If(fmt string, args ...interface{}) { c.Log.If(c.logHeader+fmt, args...) }
func (c *AContext) Wf(fmt string, args ...interface{}) { c.Log.Wf(c.logHeader+fmt, args...) }
func (c *AContext) Ef(fmt string, args ...interface{}) { c.Log.Ef(c.logHeader+fmt, args...) }
func (c *AContext) Ff(fmt string, args ...interface{}) { c.Log.Ff(c.logHeader+fmt, args...) }

func (c *AContext) D(msg string) { c.Log.D(c.logHeader + msg) }
func (c *AContext) I(msg string) { c.Log.I(c.logHeader + msg) }
func (c *AContext) W(msg string) { c.Log.W(c.logHeader + msg) }
func (c *AContext) E(msg string) { c.Log.E(c.logHeader + msg) }
func (c *AContext) F(msg string) { c.Log.F(c.logHeader + msg) }

func (c *AContext) LE(msg string, err error) { c.Log.LogError(c.logHeader+msg, err) }

func (c *AContext) TestReflect() { fmt.Printf("Test for reflect done success.\n") }
