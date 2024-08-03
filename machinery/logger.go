package machinery

import (
	"fmt"
	"github.com/RichardKnop/logging"
	"github.com/go-kratos/kratos/v2/log"
)

const (
	logKey = "machinery"
)

func LogInfo(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelInfo, logKey, fmt.Sprint(args...))
}

func LogError(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelError, logKey, fmt.Sprint(args...))
}

func LogFatal(args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelFatal, logKey, fmt.Sprint(args...))
}

func LogInfof(format string, args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelInfo, logKey, fmt.Sprintf(format, args...))
}

func LogErrorf(format string, args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelError, logKey, fmt.Sprintf(format, args...))
}

func LogFatalf(format string, args ...interface{}) {
	_ = log.GetLogger().Log(log.LevelFatal, logKey, fmt.Sprintf(format, args...))
}

///
/// logger
///

type logger struct {
	level log.Level
}

func newLogger() logging.LoggerInterface {
	return &logger{}
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
