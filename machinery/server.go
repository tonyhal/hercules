package machinery

import (
	"context"
	"github.com/RichardKnop/machinery/v2"
	"sync"

	"github.com/go-kratos/kratos/v2/transport"
)

var (
	_ transport.Server = (*Server)(nil)
)

type consumerOption struct {
	ConsumerTag string // 消费者的标记
	Concurrency int    // 并发数, 0表示不限制
}

type Server struct {
	sync.RWMutex
	err     error
	baseCtx context.Context

	machineryServer *machinery.Server
	consumerOption  consumerOption
}

func NewServer(opts ...TaskCenterOption) *Server {
	s := &Server{
		consumerOption: consumerOption{
			ConsumerTag: "machinery.worker",
			Concurrency: 0,
		},
	}
	s.machineryServer = NewTaskCenter(opts...)
	return s
}

func (s *Server) Name() string {
	return "machinery"
}

// 启动
func (s *Server) Start(ctx context.Context) (err error) {
	worker := s.machineryServer.NewWorker(s.consumerOption.ConsumerTag, s.consumerOption.Concurrency)
	err = worker.Launch()
	return
}

func (s *Server) Stop(ctx context.Context) error {
	return nil
}
