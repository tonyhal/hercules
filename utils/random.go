package utils

import (
	"math/rand"
	"sync"
	"time"
)

var o *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var random_mux_ sync.Mutex

func RandInt64() (r int64) {
	random_mux_.Lock()
	defer random_mux_.Unlock()
	return (o.Int63())
}

func RandInt32() (r int32) {
	random_mux_.Lock()
	defer random_mux_.Unlock()
	return (o.Int31())
}

func RandUint32() (r uint32) {
	random_mux_.Lock()
	defer random_mux_.Unlock()
	return (o.Uint32())
}

func RandInt64N(n int64) (r int64) {
	random_mux_.Lock()
	defer random_mux_.Unlock()
	return (o.Int63n(n))
}

func RandInt32N(n int32) (r int32) {
	random_mux_.Lock()
	defer random_mux_.Unlock()
	return (o.Int31n(n))
}

var randomChan chan uint32

func randUint32() {
	randomChan = make(chan uint32, 1024)
	go func() {
		var numstr uint32
		for {
			numstr = RandUint32()
			select {
			case randomChan <- numstr:
			}
			<-time.After(time.Millisecond * 100)
		}
	}()
}

func GetRandUint32() uint32 {
	return <-randomChan
}
