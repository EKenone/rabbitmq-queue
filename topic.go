package rabbitmq

import (
	"context"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"time"
)

type TopicMode struct {
	Conn     *amqp.Connection
	Ch       *amqp.Channel
	Exchange string
	Key      string
	Ctx      context.Context
}

func GetTopicConn(ctx context.Context, exchange, key string) (*TopicMode, error) {
	conn, ch, err := GetConn()
	if err != nil {
		return nil, err
	}
	return &TopicMode{
		Conn:     conn,
		Ch:       ch,
		Exchange: exchange,
		Key:      key,
		Ctx:      ctx,
	}, nil
}

func (mq *TopicMode) Public(msg string) error {
	defer mq.Ch.Close()
	err := mq.Ch.ExchangeDeclare(
		mq.Exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	//2.发送消息
	err = mq.Ch.Publish(
		mq.Exchange,
		mq.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			MessageId:   uuid.New().String(),
			Timestamp:   time.Now(),
			Body:        []byte(msg),
		},
	)
	return err
}

func (mq *TopicMode) GetDelivery() (delivery <-chan amqp.Delivery, err error) {
	err = mq.Ch.ExchangeDeclare(
		mq.Exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}

	//2.试探性创建队列，这里注意队列名称不要写哦

	q, err := mq.Ch.QueueDeclare(
		//随机生产队列名称 这个地方一定要留空
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}
	//3.绑定队列到exchange中去
	err = mq.Ch.QueueBind(
		q.Name,
		mq.Key,
		mq.Exchange,
		false,
		nil,
	)

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
