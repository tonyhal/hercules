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

	conn    map[string]*amqp091.Connection
	channel map[string]*amqp091.Channel

	baseCtx context.Context
	cancel  context.CancelFunc
	source  map[string]string
	err     error

	Consumers []Consumer
}

// ServerOption is an HTTP 150.fyi option.
type ServerOption func(*Server)

func WithSource(source map[string]string) ServerOption {
	return func(s *Server) {
		s.source = source
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
	s.RLock()
	defer s.RUnlock()

	for identity, source := range s.source {
		s.conn[utils.Md5(identity)], s.err = amqp091.Dial(source)
		if s.err != nil {
			log.Errorf("failed opening connection to rabbitmq: %v\n", s.err)
			return s.err
		}
	}

	log.Infof("[%s] server starting.", s.Name())
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.RLock()
	defer s.RUnlock()

	s.err = s.Connect()
	if s.err != nil {
		log.Error(s.err)
		return s.err
	}

	s.baseCtx, s.cancel = context.WithCancel(context.Background())
	for _, consumer := range s.Consumers {

		identity := utils.Md5(consumer.Identity)

		// 声明交换机延时、以及延时交换机
		argv := amqp091.Table{}
		exchangeSplit := strings.Split(strings.Trim(consumer.Exchange, ""), ".")
		exchangeType := strings.ToLower(exchangeSplit[len(exchangeSplit)-1])
		// 延时队列处理
		if strings.Contains(consumer.Exchange, "delayed") {
			exchangeType = "x-delayed-message"
			argv["x-delayed-type"] = "direct"
		}

		// 验证交换机类型
		if !strings.Contains("|direct|fanout|headers|topic|x-delayed-message|", exchangeType) {
			return fmt.Errorf("%v, RabbitMQ不存在该类型交换机", consumer.Exchange)
		}

		for i := 0; i < consumer.Fork; i++ {
			// 获取通道
			if s.channel[identity], s.err = s.conn[identity].Channel(); s.err != nil {
				break
			}
			// 声明交换机
			if err := s.channel[identity].ExchangeDeclare(consumer.Exchange, exchangeType, true, false, false, false, argv); err != nil {
				fmt.Println("ExchangeDeclare:", err.Error())
			}
			// 声明队列
			s.channel[identity].QueueDeclare(consumer.Queue, true, false, false, false, amqp091.Table{"x-ha-policy": "all"})
			// 绑定队列
			s.err = s.channel[identity].QueueBind(consumer.Queue, consumer.Queue, consumer.Exchange, true, nil)
			if s.err != nil {
				break
			}
			// 获取消费通道, 确保rabbitMQ一个一个发送消息
			s.channel[identity].Qos(1, 0, true)
			deliveries, err := s.channel[identity].Consume(consumer.Queue, "", false, false, false, false, nil)
			if err != nil {
				break
			}
			// 协程处理
			go func(ctx context.Context, deliveries <-chan amqp091.Delivery, consumer Consumer) {
				for {
					select {
					case <-ctx.Done():
						log.Infof("rabbitmq %s closed.", consumer.Queue)
						return
					case delivery := <-deliveries:
						// 循环读取消息
						if err := consumer.Handle(context.Background(), Message{key: consumer.Queue, value: delivery.Body}); err != nil {
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

	// 关闭通道
	for _, channel := range s.channel {
		channel.Close()
	}
	// 关闭链接
	for _, conn := range s.conn {
		conn.Close()
	}

	log.Infof("[%s] server stopping", s.Name())
	return nil
}

func (s *Server) AddConsumer(consumer Consumer) error {
	s.RLock()
	defer s.RUnlock()

	s.Consumers = append(s.Consumers, consumer)
	return nil
}
