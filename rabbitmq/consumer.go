package rabbitmq

import "context"

type ConsumerHandle func(context.Context, Message) error

type Consumer struct {
	ExchangeName string
	QueueName    string
	Vhost        string
	Handle       ConsumerHandle
	Fork         int
}
