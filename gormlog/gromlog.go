package gormlog

import (
	"context"
	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"time"

	"errors"
	"gorm.io/gorm/logger"
)

// gorm 源码
type Interface interface {
	LogMode(level logger.LogLevel) Interface
	Info(context.Context, string, ...interface{})
	Warn(context.Context, string, ...interface{})
	Error(context.Context, string, ...interface{})
	Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error)
}

type GormLogger struct {
	SlowThreshold time.Duration
}

func NewGormLogger() *GormLogger {
	return &GormLogger{
		SlowThreshold: 900 * time.Millisecond, // 一般超过200毫秒就算慢查所以不使用配置进行更改
	}
}

var _ logger.Interface = (*GormLogger)(nil)

func (l *GormLogger) LogMode(lev logger.LogLevel) logger.Interface {
	return &GormLogger{}
}

func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	log.Context(ctx).Infof(msg, data)
}

func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	log.Context(ctx).Errorf(msg, data)
}

func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	log.Context(ctx).Errorf(msg, data)
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	// 获取运行时间
	elapsed := time.Since(begin)
	// 获取 SQL 语句和返回条数
	sql, rows := fc()
	// Gorm 错误时打印
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Context(ctx).Errorf("SQL ERROR, | elapsed=%v, rows=%v, sql=%v ", elapsed, rows, sql)
	}
	// 慢查询日志
	if l.SlowThreshold != 0 && elapsed > l.SlowThreshold {
		log.Context(ctx).Warnf("Database Slow Log, | elapsed=%v, rows=%v, sql=%v", elapsed, rows, sql)
	}
}
