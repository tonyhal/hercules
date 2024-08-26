package binlog

import (
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/siddontang/go-log/loggers"
)

const (
	logKey = "msg"
)

func LogDebug(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelDebug, logKey, fmt.Sprint(args...))
}

func LogInfo(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelInfo, logKey, fmt.Sprint(args...))
}

func LogWarn(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelWarn, logKey, fmt.Sprint(args...))
}

func LogError(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelError, logKey, fmt.Sprint(args...))
}

func LogFatal(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelFatal, logKey, fmt.Sprint(args...))
}

func LogDebugf(format string, args ...interface{}) {
	//_ = log.GetLogger().Log(log.LevelDebug, logKey, fmt.Sprintf(format, args...))
}

func LogInfof(format string, args ...interface{}) {
	//_ = log.GetLogger().Log(log.LevelInfo, logKey, fmt.Sprintf(format, args...))
}

func LogWarnf(format string, args ...interface{}) {
	//_ = log.GetLogger().Log(log.LevelWarn, logKey, fmt.Sprintf(format, args...))
}

func LogErrorf(format string, args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelError, logKey, fmt.Sprintf(format, args...))
}

func LogFatalf(format string, args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelFatal, logKey, fmt.Sprintf(format, args...))
}

// logger
type logger struct {
	level log.Level
}

func newLogger(level log.Level) loggers.Advanced {
	return &logger{
		level: level,
	}
}

func (l *logger) Debug(args ...interface{}) {
	LogDebug(args)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	LogDebugf(format, args...)
}

func (l *logger) Debugln(args ...interface{}) {
	LogDebug(args)
}

func (l *logger) Error(args ...interface{}) {
	LogError(args)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	LogErrorf(format, args)
}

func (l *logger) Errorln(args ...interface{}) {
	LogError(args)
}

func (l *logger) Info(args ...interface{}) {
	LogInfo(args)
}

func (l *logger) Infof(format string, args ...interface{}) {
	LogInfof(format, args)
}

func (l *logger) Infoln(args ...interface{}) {
	LogInfo(args)
}

func (l *logger) Warn(args ...interface{}) {
	LogWarn(args)
}

func (l *logger) Warnf(format string, args ...interface{}) {
	LogWarnf(format, args)
}

func (l *logger) Warnln(args ...interface{}) {
	LogWarn(args)
}

func (l *logger) Fatal(args ...interface{}) {
	LogFatal(args...)
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	LogFatalf(format, args...)
}

func (l *logger) Fatalln(args ...interface{}) {
	LogFatal(args...)
}

func (l *logger) Panic(args ...interface{}) {
	LogError(args...)
}

func (l *logger) Panicf(format string, args ...interface{}) {
	LogErrorf(format, args...)
}

func (l *logger) Panicln(args ...interface{}) {
	LogError(args...)
}

func (l *logger) Print(args ...interface{}) {
	LogInfo(args...)
}

func (l *logger) Printf(format string, args ...interface{}) {
	LogInfof(format, args...)
}

func (l *logger) Println(args ...interface{}) {
	LogInfo(args...)
}
