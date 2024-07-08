package rabbitmq

import (
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
	"hercules/utils"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Producer struct {
	sync.RWMutex
	conn    map[string]*amqp.Connection
	channel map[string]*amqp.Channel
	Source  string
	Vhost   []string
}

func (c *Producer) Init() {
	c.Lock()
	defer c.Unlock()
	c.conn = make(map[string]*amqp.Connection)
	c.channel = make(map[string]*amqp.Channel)

	var err error
	for _, vhost := range c.Vhost {
		// 连接
		if c.conn[utils.Md5(vhost)], err = amqp.Dial(fmt.Sprintf("%s/%s", c.Source, strings.Trim(vhost, "/"))); err != nil {
			time.AfterFunc(time.Second*3, func() { go c.Init() })
			return
		}
		// 断线重连
		go func(conn *amqp.Connection) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("%s\n", string(debug.Stack()))
				}
			}()

			var chanErr chan *amqp.Error = conn.NotifyClose(make(chan *amqp.Error))
			select {
			case _, ok := <-chanErr:
				if !ok {
				}
				conn.Close()
				go c.Init()
			}
		}(c.conn[utils.Md5(vhost)])
	}
}

// 推送消息
func (c *Producer) Publish(body []byte, queue, exchange, expiration, vhost string) error {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("recover: %v", string(debug.Stack()))
		}
	}()

	channel, err := c.conn[utils.Md5(vhost)].Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// 确认消息
	if err := channel.Confirm(false); err != nil {
		return err
	}
	confirm := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	defer func(confirm <-chan amqp.Confirmation) {
		if confirmed := <-confirm; !confirmed.Ack {
			log.Errorf("failed delivery of delivery tag: %v\n", confirmed.DeliveryTag)
		}
	}(confirm)

	publishing := amqp.Publishing{
		ContentType:  "text/plain", //application/json text/plain
		Body:         body,
		DeliveryMode: amqp.Persistent, // 1=non-persistent, 2=persistent
		MessageId:    utils.Md5(string(body)),
		Timestamp:    time.Now(),
	}
	if len(expiration) > 0 && strings.Compare(exchange, "delayed") > 0 {
		publishing.Headers = amqp.Table{"x-delay": expiration}
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
