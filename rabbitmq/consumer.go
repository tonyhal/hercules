package rabbitmq

import "context"

type ConsumerHandle func(context.Context, Message) error

type Consumer struct {
	Identity string
	Exchange string
	Queue    string
	Handle   ConsumerHandle
	Fork     int
}
