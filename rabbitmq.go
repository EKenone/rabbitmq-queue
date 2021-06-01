package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

var Conn *amqp.Connection
var setUpOnce sync.Once
var connWaitTime = 15 * time.Second

const url = "amqp://root:123456@11.11.11.114:5672"

func SetUp() {
	if Conn == nil || Conn.IsClosed() {
		setUpOnce.Do(func() {
			var err error
			for i := 0; i < 2; i++ {
				Conn, err = amqp.Dial(url)
				if err != nil {
					time.Sleep(connWaitTime)
					continue
				}
				break
			}

			if err != nil {
				panic(fmt.Sprintf("rabbitmq 启动异常: %v", err))
			}
		})
	}
}

//重新加载
var reloadOnce sync.Once

func reload() bool {
	reloadIng := false
	reloadOnce.Do(func() {
		if Conn != nil {
			Conn.Close()
		}
		Conn, setUpOnce = nil, sync.Once{}
		SetUp()
		time.AfterFunc(5*time.Second, func() {
			reloadOnce = sync.Once{}
		})
		reloadIng = true
	})

	if !reloadIng {
		time.Sleep(connWaitTime)
	}

	return reloadIng
}

//获取mq链接
func GetConn() (conn *amqp.Connection, ch *amqp.Channel, err error) {
	if Conn == nil || Conn.IsClosed() {
		reload()
	}

	for i := 0; i < 2; i++ {
		ch, err = Conn.Channel()
		if errors.Is(err, amqp.ErrClosed) {
			reload()
		} else {
			break
		}
	}

	if err != nil {
		return
	}
	conn = Conn
	return
}
