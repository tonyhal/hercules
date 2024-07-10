package rabbitmq

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/rabbitmq/amqp091-go"
	"github.com/tonyhal/hercules/utils"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Producer struct {
	sync.RWMutex

	conn    *amqp091.Connection
	channel *amqp091.Channel
	Source  string
}

func (c *Producer) Init() {
	c.Lock()
	defer c.Unlock()

	var err error
	// 连接
	if c.conn, err = amqp091.Dial(c.Source); err != nil {
		time.AfterFunc(time.Second*3, func() { go c.Init() })
		return
	}
	// 断线重连
	go func(conn *amqp091.Connection) {
		defer func() {
			if err := recover(); err != nil {
				log.Errorf("%s\n", string(debug.Stack()))
			}
		}()

		var chanErr chan *amqp091.Error = conn.NotifyClose(make(chan *amqp091.Error))
		select {
		case _, ok := <-chanErr:
			if !ok {
			}
			conn.Close()
			go c.Init()
		}
	}(c.conn)
}

// 推送消息
func (c *Producer) Publish(body []byte, queue, exchange, expiration string) error {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("recover: %v", string(debug.Stack()))
		}
	}()

	channel, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// 确认消息
	if err := channel.Confirm(false); err != nil {
		return err
	}
	confirm := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	defer func(confirm <-chan amqp091.Confirmation) {
		if confirmed := <-confirm; !confirmed.Ack {
			log.Errorf("failed delivery of delivery tag: %v\n", confirmed.DeliveryTag)
		}
	}(confirm)

	publishing := amqp091.Publishing{
		ContentType:  "text/plain", //application/json text/plain
		Body:         body,
		DeliveryMode: amqp091.Persistent, // 1=non-persistent, 2=persistent
		MessageId:    utils.Md5(string(body)),
		Timestamp:    time.Now(),
	}
	if len(expiration) > 0 && strings.Compare(exchange, "delayed") > 0 {
		publishing.Headers = amqp091.Table{"x-delay": expiration}
	}
	if err := channel.Publish(
		exchange, // publish to an exchange
		queue,    // routing to 0 or more queues
		false,    // mandatory
		false,    // immediate
		publishing,
	); err != nil {
		return err
	}
	return nil
}
