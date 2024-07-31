package machinery

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2"
	machineryLog "github.com/RichardKnop/machinery/v2/log"
	"sync"

	eagerBackend "github.com/RichardKnop/machinery/v2/backends/eager"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	amqpbroker "github.com/RichardKnop/machinery/v2/brokers/amqp"
	machineryConfig "github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
)

type resultBackendOption struct {
	Addr string
	Db   int
}

type TaskCenter struct {
	cfg                 *machineryConfig.Config
	resultBackendOption resultBackendOption
	registerTask        *sync.Map
}

func NewTaskCenter(opts ...TaskCenterOption) *machinery.Server {
	tc := &TaskCenter{
		cfg: &machineryConfig.Config{
			DefaultQueue:    "tasks.machinery.direct",
			ResultsExpireIn: 7 * 86400,
			AMQP: &machineryConfig.AMQPConfig{
				Exchange:     "machinery.direct",
				ExchangeType: "direct",
				BindingKey:   "tasks.machinery.direct",
				//PrefetchCount: 0, // 队列同时，消费多少个
			},
			Redis: &machineryConfig.RedisConfig{
				MaxIdle:        3,
				IdleTimeout:    150,
				ReadTimeout:    15,
				WriteTimeout:   15,
				ConnectTimeout: 15,
			},
		},
		resultBackendOption: resultBackendOption{},
	}

	// 初始化参数
	tc.init(opts...)

	// 自定义日志
	machineryLog.Set(newLogger())

	redisbackend.NewGR(tc.cfg, []string{tc.resultBackendOption.Addr}, tc.resultBackendOption.Db)
	// Create server instance
	server := machinery.NewServer(tc.cfg,
		amqpbroker.New(tc.cfg),
		eagerBackend.New(),
		eagerlock.New(),
	)
	// 注册任务
	tc.registerTask.Range(func(k, v any) bool {
		if key, ok := k.(string); ok {
			server.RegisterTask(key, v)
		}
		return true
	})

	return server
}

func (w *TaskCenter) init(opts ...TaskCenterOption) {
	for _, o := range opts {
		o(w)
	}
}

type TaskCenterOption func(o *TaskCenter)

func WithAmqpBroker(broker string) TaskCenterOption {
	return func(w *TaskCenter) {
		w.cfg.Broker = broker
	}
}

func WithRedisResultBackend(backend string, db int) TaskCenterOption {
	return func(w *TaskCenter) {
		w.resultBackendOption = resultBackendOption{Addr: backend, Db: db}
		w.cfg.ResultBackend = fmt.Sprintf("redis://%s/%d", backend, db)
	}
}

func WithRegisterTask(registerTask *sync.Map) TaskCenterOption {
	return func(w *TaskCenter) {
		w.registerTask = registerTask
	}
}
