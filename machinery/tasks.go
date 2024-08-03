package machinery

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type TaskOption func(o *tasks.Signature)

func WithRetryCount(count int) TaskOption {
	return func(o *tasks.Signature) {
		o.RetryCount = count
	}
}

func WithArgument(typeName string, value interface{}) TaskOption {
	return func(o *tasks.Signature) {
		o.Args = append(o.Args, tasks.Arg{Type: typeName, Value: value})
	}
}

func NewTask(name string, opts ...TaskOption) (*tasks.Signature, error) {
	signature, err := tasks.NewSignature(name, nil)
	// 重新配置UUID
	signature.UUID = fmt.Sprintf("machinery:tasks:%s", signature.UUID)
	if err != nil {
		return nil, err
	}
	for _, o := range opts {
		o(signature)
	}
	return signature, nil
}
