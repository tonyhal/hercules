package sonyflake

import (
	"github.com/sony/sonyflake"
)

var (
	sf *sonyflake.Sonyflake
)

func NewSonyflake() *sonyflake.Sonyflake {
	if sf == nil {
		var st sonyflake.Settings
		sf, _ = sonyflake.New(st)
	}
	return sf
}
