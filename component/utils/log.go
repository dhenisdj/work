package utils

import (
	"git.garena.com/shopee/feed/comm_lib/logkit"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Field = zapcore.Field

type Logger interface {
	With(fields ...Field) Logger

	Dff(msg string, fields ...Field)
	Iff(msg string, fields ...Field)
	Wff(msg string, fields ...Field)
	Eff(msg string, fields ...Field)
	Fff(msg string, fields ...Field)

	Df(format string, args ...interface{})
	If(format string, args ...interface{})
	Wf(format string, args ...interface{})
	Ef(format string, args ...interface{})
	Ff(format string, args ...interface{})

	D(msg string)
	I(msg string)
	W(msg string)
	E(msg string)
	F(msg string)
}

type Log struct {
	Logger
	*logkit.LogkitLogger
	*zap.SugaredLogger
}

func InitLog() *Log {
	logger := logkit.GetLogger()
	return &Log{
		LogkitLogger:  logger,
		SugaredLogger: logger.Sugar(),
	}
}

func (l *Log) With(fields ...Field) {
	l.LogkitLogger.With(fields...)
}

func (l *Log) Dff(msg string, fields ...Field) {
	l.LogkitLogger.Debug(msg, fields...)
}

func (l *Log) Iff(msg string, fields ...Field) {
	l.LogkitLogger.Info(msg, fields...)
}

func (l *Log) Wff(msg string, fields ...Field) {
	l.LogkitLogger.Warn(msg, fields...)
}

func (l *Log) Eff(msg string, fields ...Field) {
	l.LogkitLogger.Error(msg, fields...)
}

func (l *Log) Fff(msg string, fields ...Field) {
	l.LogkitLogger.Fatal(msg, fields...)
}

func (l *Log) Df(format string, args ...interface{}) {
	l.SugaredLogger.Debugf(format, args...)
}

func (l *Log) If(format string, args ...interface{}) {
	l.SugaredLogger.Infof(format, args...)
}

func (l *Log) Wf(format string, args ...interface{}) {
	l.SugaredLogger.Warnf(format, args...)
}

func (l *Log) Ef(format string, args ...interface{}) {
	l.SugaredLogger.Errorf(format, args...)
}

func (l *Log) Ff(format string, args ...interface{}) {
	l.SugaredLogger.Fatalf(format, args...)
}

func (l *Log) D(msg string) {
	l.LogkitLogger.Debug(msg)
}

func (l *Log) I(msg string) {
	l.LogkitLogger.Info(msg)
}

func (l *Log) W(msg string) {
	l.LogkitLogger.Warn(msg)
}

func (l *Log) E(msg string) {
	l.LogkitLogger.Error(msg)
}

func (l *Log) F(msg string) {
	l.LogkitLogger.Fatal(msg)
}

func (l *Log) LogError(key string, err error) {
	l.LogkitLogger.WithError(err).Info(key)
}
