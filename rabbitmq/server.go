package rabbitmq

import (
	"context"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/rabbitmq/amqp091-go"
	"github.com/tonyhal/hercules/utils"
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

type Server struct {
	sync.RWMutex
	conn      map[string]*amqp091.Connection
	channel   map[string]*amqp091.Channel
	baseCtx   context.Context
	cancel    context.CancelFunc
	source    string
	err       error
	Consumers []Consumer
	vhosts    []string
}

// ServerOption is an HTTP 150.fyi option.
type ServerOption func(*Server)

func WithSource(source string) ServerOption {
	return func(s *Server) {
		s.source = source
	}
}

func WithVhost(vhosts []string) ServerOption {
	return func(s *Server) {
		s.vhosts = vhosts
	}
}

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx: context.Background(),
		conn:    make(map[string]*amqp091.Connection),
		channel: make(map[string]*amqp091.Channel),
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

	for _, vhost := range s.vhosts {
		s.conn[utils.Md5(vhost)], s.err = amqp091.Dial(fmt.Sprintf("%s/%s", s.source, strings.Trim(vhost, "/")))
		if s.err != nil {
			log.Errorf("failed opening connection to rabbitmq: %v\n", s.err)
			return s.err
		}
	}

	log.Infof("[%s] connected successfully.", s.Name())
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
	for _, consumer := range s.Consumers {
		vhost := utils.Md5(consumer.Vhost)
		for i := 0; i < consumer.Fork; i++ {
			// 获取通道
			if s.channel[vhost], s.err = s.conn[vhost].Channel(); s.err != nil {
				break
			}
			// 声明队列
			s.channel[vhost].QueueDeclare(consumer.QueueName, true, false, false, false, amqp091.Table{"x-ha-policy": "all"})
			// 绑定队列
			s.err = s.channel[vhost].QueueBind(consumer.QueueName, consumer.QueueName, consumer.ExchangeName, true, nil)
			if s.err != nil {
				break
			}
			// 获取消费通道, 确保rabbitMQ一个一个发送消息
			s.channel[vhost].Qos(1, 0, true)
			deliveries, err := s.channel[vhost].Consume(consumer.QueueName, "", false, false, false, false, nil)
			if err != nil {
				break
			}
			// 协程处理
			go func(ctx context.Context, deliveries <-chan amqp091.Delivery, consumer Consumer) {
				for {
					select {
					case <-ctx.Done():
						log.Infof("rabbitmq %s closed.", consumer.QueueName)
						return
					case delivery := <-deliveries:
						// 循环读取消息
						if err := consumer.Handle(context.Background(), Message{key: consumer.QueueName, value: delivery.Body}); err != nil {
							delivery.Nack(false, true)
						} else {
							delivery.Ack(false)
						}
					}
				}
			}(s.baseCtx, deliveries, consumer)
		}
	}
	return s.err
}

func (s *Server) Stop(ctx context.Context) error {
	s.cancel()
	log.Infof("[%s] server stopping", s.Name())

	for _, vhost := range s.vhosts {
		s.conn[utils.Md5(vhost)].Close()
	}

	return nil
}

func (s *Server) AddConsumer(consumer Consumer) error {
	s.Lock()
	defer s.Unlock()

	s.Consumers = append(s.Consumers, consumer)
	return nil
}
