package zaplog

import (
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"

	zapLog "github.com/go-kratos/kratos/contrib/log/zap/v2"
)

func NewZapLog(serviceId, serviceName, serviceVersion string, maxBackups, maxAge int, mode string) log.Logger {
	// 分割日志文件
	lumberjackLogger := &lumberjack.Logger{
		Filename:   fmt.Sprintf("./logs/%s.log", serviceName),
		MaxSize:    500, // megabytes
		MaxBackups: maxBackups,
		MaxAge:     maxAge, //days
		Compress:   false,  // 禁用压缩
		LocalTime:  true,   // 使用本地时间作为文件名的时间戳
	}
	defer lumberjackLogger.Close()

	zapConfig := zap.NewProductionEncoderConfig()
	zapConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)

	// console
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zapConfig), zapcore.Lock(os.Stdout), zapcore.DebugLevel)
	if mode == "release" {
		// 输出到文件
		core = zapcore.NewCore(zapcore.NewJSONEncoder(zapConfig), zapcore.AddSync(lumberjackLogger), zap.InfoLevel)
	}

	zapLogger := zap.New(zapcore.NewTee(core))
	defer zapLogger.Sync()

	return log.With(zapLog.NewLogger(zapLogger),
		"caller", log.DefaultCaller,

		"service.id", serviceId,
		"service.name", serviceName,
		"service.version", serviceVersion,
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)
}
