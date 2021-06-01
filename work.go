package rabbitmq

import (
	"context"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"time"
)

type WorkMode struct {
	Conn      *amqp.Connection
	Ch        *amqp.Channel
	QueueName string
	Ctx       context.Context
}

func GetWorkConn(ctx context.Context, queueName string) (*WorkMode, error) {
	conn, ch, err := GetConn()
	if err != nil {
		return nil, err
	}
	return &WorkMode{
		Conn:      conn,
		Ch:        ch,
		QueueName: queueName,
		Ctx:       ctx,
	}, nil
}

func (mq *WorkMode) Public(msg string) (string, error) {
	//1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err := mq.Ch.QueueDeclare(
		mq.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return "", err
	}
	//2.发送消息到队列当中
	uid := uuid.New().String()
	err = mq.Ch.Publish(
		"",
		mq.QueueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			MessageId:    uid,
			Timestamp:    time.Now(),
			Body:         []byte(msg),
		},
	)
	return uid, err
}

func (mq *WorkMode) GetDelivery(qosCount int) (delivery <-chan amqp.Delivery, err error) {
	q, err := mq.Ch.QueueDeclare(
		//随机生产队列名称 这个地方一定要留空
		mq.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}

	// 设定消费者每次可获得mq的数量
	if qosCount == 0 {
		qosCount = 1
	} else if qosCount > 250 { // 限制每台机器一次最多拿250个，保证资源不被过度滥用
		qosCount = 250
	}

	err = mq.Ch.Qos(qosCount, 0, false)
	if err != nil {
		return
	}

	return mq.Ch.Consume(
		q.Name,
		"",
		false,
		//是否具有排他性
		false,
		false,
		false,
		nil,
	)

}
