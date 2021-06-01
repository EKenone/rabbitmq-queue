package rabbitmq

import (
	"context"
	"errors"
	"github.com/panjf2000/ants/v2"
	"github.com/streadway/amqp"
	"sync"
)

//消费操作
type Action func(ctx context.Context, delivery amqp.Delivery) error

//幂等处理
type Once func(ctx context.Context, delivery amqp.Delivery, queueName string) (isFirst bool, err error)

//日志处理
type Log func(ctx context.Context, delivery amqp.Delivery, queueName string, err error)

//队列名称
type QueueName string

//工作队列
type WorkQueue struct {
	Tasks map[QueueName]*Task
	Once  sync.Once
}

//队列配置
type Option struct {
	QueueName QueueName
	Action    Action
	QosCount  int
	Once      Once
	Log       Log
}

//实例一个work队列
func NewWorkQueue() *WorkQueue {
	return &WorkQueue{
		Tasks: map[QueueName]*Task{},
	}
}

//注册队列
func (at *WorkQueue) Registered(options ...Option) {
	if len(options) == 0 {
		return
	}

	for _, opt := range options {
		worker, _ := ants.NewPool(opt.QosCount + 1) //处理消息体的协程数 + 1个启动消费者的协程数

		task := &Task{
			worker:    worker,
			queueName: string(opt.QueueName),
			action:    opt.Action,
			qosCount:  opt.QosCount,
			once:      opt.Once,
			log:       opt.Log,
		}

		if _, ok := at.Tasks[opt.QueueName]; ok {
			panic("注册了重复队列")
		}
		at.Tasks[opt.QueueName] = task
	}
}

//监听注册的队列
func (at *WorkQueue) Listen() error {
	var err error
	at.Once.Do(func() {
		for _, task := range at.Tasks {
			err = task.worker.Submit(task.ListenQueue)
		}
	})
	return err
}

//队列消费者任务
type Task struct {
	worker    *ants.Pool
	queueName string
	action    Action
	qosCount  int
	once      Once
	log       Log
}

//启动消费者
func (q *Task) ListenQueue() {
	cumCtx := context.Background()
	mq, delivery, err := q.beforeListen(cumCtx)
	if err != nil {
		return
	}

	closeChan, exit := make(chan *amqp.Error, 1), make(chan bool)
	notifyClose := mq.Ch.NotifyClose(closeChan)

	for {
		select {
		case <-notifyClose:
			_ = q.worker.Submit(q.ListenQueue)
			exit <- true
		case d, ok := <-delivery:
			if ok {
				_ = q.worker.Submit(func() {
					q.RunAction(d, q.queueName, q.action)
				})
			} else {
				mq.Ch.Close()
				return
			}
		case <-exit:
			mq.Ch.Close()
			mq.Conn.Close()
			return
		}
	}
}

//执行操作
func (q *Task) RunAction(diy amqp.Delivery, queueName string, fc Action) {
	ctx := context.Background()
	var err error
	defer diy.Ack(false)
	defer func() {
		if q.log != nil {
			q.log(ctx, diy, queueName, err)
		}
	}()

	//验证幂等
	if q.once != nil {
		var isFirst bool
		isFirst, err = q.once(ctx, diy, queueName)
		if err != nil {
			return
		}

		//幂等
		if !isFirst {
			err = errors.New("幂等校验失败")
			return
		}
	}

	//处理逻辑
	err = fc(ctx, diy)
}

// 在启动消费者前做一些处理
func (q *Task) beforeListen(ctx context.Context) (mq *WorkMode, delivery <-chan amqp.Delivery, err error) {
	mq, err = GetWorkConn(ctx, q.queueName)
	if err != nil {
		return
	}

	delivery, err = mq.GetDelivery(q.qosCount)
	if err != nil {
		return
	}

	return
}
