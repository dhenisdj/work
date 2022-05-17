package context

import (
	"context"
	"github.com/dhenisdj/scheduler/component/common"
	"github.com/dhenisdj/scheduler/component/utils"
	"os"
	"reflect"
	"time"

	"go.uber.org/zap"
)

type ContextKeyType string

const (
	ContextKeyHost ContextKeyType = "@host"
)

type CancelFunc = context.CancelFunc

var ContextType = reflect.TypeOf(Context{})

// Context 通过With...的修改直接生效
type Context struct {
	*common.Caller
	context.Context

	Log        *utils.Log
	sequence   string
	cancelFunc CancelFunc
	source     *Context
}

func New() *Context {
	return NewContext(context.Background()).WithLogSequence()
}

func NewContext(c context.Context) *Context {
	if c == nil {
		c = context.Background()
	}
	c, cancelFunc := context.WithCancel(c)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	return (&Context{
		Context:    c,
		Log:        utils.InitLog(),
		cancelFunc: cancelFunc,
		Caller:     common.NewCaller(),
	}).
		WithValue(ContextKeyHost, hostname)

}

func (c *Context) Clone() Context {
	ctx := *c
	ctx.Log = utils.InitLog() // FIXME: 这里会丢失原先的日志字段，但是没办法，现在这个设计没办法只修改掉sequence字段
	ctx.WithLogSequence()
	ctx.source = c
	return ctx
}

func (c *Context) Source() *Context {
	return c.source
}

func (c *Context) WithLogFields(fields ...utils.Field) *Context {
	//ctx := c.Clone().(*ctx)
	c.Log.With(fields...)
	return c
}

func (c *Context) WithLogSequence() *Context {
	//ctx := c.Clone().(*ctx)
	c.sequence = utils.Sequence()
	c.Log.With(zap.String("sequence", c.sequence))
	return c
}

func (c *Context) Sequence() string { return c.sequence }

func (c *Context) WithDeadline(deadline time.Time) *Context {
	ctx, f := context.WithDeadline(c.Context, deadline)
	c.Context = ctx
	c.cancelFunc = f
	return c
}

func (c *Context) WithValue(key, val interface{}) *Context {
	c.Context = context.WithValue(c.Context, key, val)
	return c
}

func (c *Context) Cancel() {
	c.cancelFunc()
}

func (c *Context) Dff(msg string, fields ...utils.Field) { c.Log.Dff(msg, fields...) }
func (c *Context) Iff(msg string, fields ...utils.Field) { c.Log.Iff(msg, fields...) }
func (c *Context) Wff(msg string, fields ...utils.Field) { c.Log.Wff(msg, fields...) }
func (c *Context) Eff(msg string, fields ...utils.Field) { c.Log.Eff(msg, fields...) }
func (c *Context) Fff(msg string, fields ...utils.Field) { c.Log.Fff(msg, fields...) }

func (c *Context) Df(fmt string, args ...interface{}) { c.Log.Df(fmt, args...) }
func (c *Context) If(fmt string, args ...interface{}) { c.Log.If(fmt, args...) }
func (c *Context) Wf(fmt string, args ...interface{}) { c.Log.Wf(fmt, args...) }
func (c *Context) Ef(fmt string, args ...interface{}) { c.Log.Ef(fmt, args...) }
func (c *Context) Ff(fmt string, args ...interface{}) { c.Log.Ff(fmt, args...) }

func (c *Context) D(msg string) { c.Log.D(msg) }
func (c *Context) I(msg string) { c.Log.I(msg) }
func (c *Context) W(msg string) { c.Log.W(msg) }
func (c *Context) E(msg string) { c.Log.E(msg) }
func (c *Context) F(msg string) { c.Log.F(msg) }

func (c *Context) LE(msg string, err error) { c.Log.LogError(msg, err) }
