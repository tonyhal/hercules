package zaplog

import (
	"fmt"
	zapLog "github.com/go-kratos/kratos/contrib/log/zap/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

func NewZapLog(serviceId, serviceName, serviceVersion string) log.Logger {
	// 分割日志文件
	lumberjackLogger := &lumberjack.Logger{
		Filename:   fmt.Sprintf("./logs/%s.log", serviceName),
		MaxSize:    500, // megabytes
		MaxBackups: 10,
		MaxAge:     7,     //days
		Compress:   false, // 禁用压缩
		LocalTime:  true,  // 使用本地时间作为文件名的时间戳
	}
	defer lumberjackLogger.Close()

	zapConfig := zap.NewProductionEncoderConfig()
	zapConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)
	zapLogger := zap.New(
		zapcore.NewTee(
			zapcore.NewCore(zapcore.NewJSONEncoder(zapConfig), zapcore.AddSync(lumberjackLogger), zap.InfoLevel), // 文件
			//zapcore.NewCore(zapcore.NewConsoleEncoder(zapConfig), zapcore.Lock(os.Stdout), zapcore.DebugLevel),   // console
		),
	)
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
