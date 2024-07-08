package rabbitmq

import (
	"150.fyi/internal/pkg/utils"
	"context"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/streadway/amqp"
	"strings"
	"sync"
)

var (
	_ transport.Server = (*Server)(nil)
)

type Message struct {
	key   string
	value []byte
}

func (m *Message) Set(key string, val []byte) {
	m.key = key
	m.value = val
}

func (m *Message) Key() string {
	return m.key
}

func (m *Message) Value() []byte {
	return m.value
}

type ReceiverHandle func(context.Context, Message) error

type Receiver struct {
	ExchangeName string
	QueueName    string
	Vhost        string
	Handle       ReceiverHandle
	Num          int
}

type Server struct {
	sync.RWMutex
	conn      map[string]*amqp.Connection
	channel   map[string]*amqp.Channel
	baseCtx   context.Context
	cancel    context.CancelFunc
	source    string
	err       error
	Receivers []Receiver
	vhost     []string
}

// ServerOption is an HTTP 150.fyi option.
type ServerOption func(*Server)

func WithSource(source string) ServerOption {
	return func(s *Server) {
		s.source = source
	}
}

func WithVhost(vhost []string) ServerOption {
	return func(s *Server) {
		s.vhost = vhost
	}
}

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx: context.Background(),
		conn:    make(map[string]*amqp.Connection),
		channel: make(map[string]*amqp.Channel),
	}
	srv.init(opts...)
	return srv
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

func (s *Server) Name() string {
	return "rabbitmq"
}

func (s *Server) Connect() error {
	s.Lock()
	defer s.Unlock()

	for _, vhost := range s.vhost {
		s.conn[utils.Md5(vhost)], s.err = amqp.Dial(fmt.Sprintf("%s/%s", s.source, strings.Trim(vhost, "/")))
		if s.err != nil {
			log.Errorf("failed opening connection to rabbitmq: %v\n", s.err)
			return s.err
		}
	}

	log.Infof("successfully connected to rabbitmq.")
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.err = s.Connect()
	if s.err != nil {
		log.Error(s.err)
		return s.err
	}

	s.Lock()
	defer s.Unlock()
	s.baseCtx, s.cancel = context.WithCancel(context.Background())
	for _, receiver := range s.Receivers {
		vhost := utils.Md5(receiver.Vhost)
		for i := 0; i < receiver.Num; i++ {
			// 获取通道
			if s.channel[vhost], s.err = s.conn[vhost].Channel(); s.err != nil {
				break
			}
			// 声明队列
			s.channel[vhost].QueueDeclare(receiver.QueueName, true, false, false, false, amqp.Table{"x-ha-policy": "all"})
			// 绑定队列
			s.err = s.channel[vhost].QueueBind(receiver.QueueName, receiver.QueueName, receiver.ExchangeName, true, nil)
			if s.err != nil {
				break
			}
			// 获取消费通道, 确保rabbitMQ一个一个发送消息
			s.channel[vhost].Qos(1, 0, true)
			deliveries, err := s.channel[vhost].Consume(receiver.QueueName, "", false, false, false, false, nil)
			if err != nil {
				break
			}
			// 协程处理
			go func(ctx context.Context, deliveries <-chan amqp.Delivery, receiver Receiver) {
				for {
					select {
					case <-ctx.Done():
						log.Infof("rabbitmq 0376.kim closed.")
						return
					case delivery := <-deliveries:
						// 循环读取消息
						if err := receiver.Handle(context.Background(), Message{key: receiver.QueueName, value: delivery.Body}); err != nil {
							delivery.Nack(false, true)
						} else {
							delivery.Ack(false)
						}
					}
				}
			}(s.baseCtx, deliveries, receiver)
		}
	}

	return s.err
}

func (s *Server) Stop(ctx context.Context) error {
	s.cancel()
	log.Infof("[%s] 150.fyi stopping\n", s.Name())

	for _, vhost := range s.vhost {
		s.conn[utils.Md5(vhost)].Close()
	}

	return nil
}

func (s *Server) RegisterReceiver(receiver Receiver) error {
	s.Lock()
	defer s.Unlock()

	s.Receivers = append(s.Receivers, receiver)

	return nil
}
