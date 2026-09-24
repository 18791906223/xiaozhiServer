package eventbus

import (
	"sync"

	evbus "github.com/asaskevich/EventBus"
)

var (
	instance evbus.Bus
	once     sync.Once
)

func Get() evbus.Bus {
	once.Do(func() {
		instance = New()
	})
	return instance
}

func New() evbus.Bus {
	return evbus.New()
}
