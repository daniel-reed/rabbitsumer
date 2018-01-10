package rabbitsumer

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type Consume func(d *amqp.Delivery)

type Consumer interface {
	Consume()
}

type Consumers []Consumer

func (c Consumers) Consume() {
	for _, consumer := range c {
		go consumer.Consume()
	}
}

type ConsumerOptions struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type consumer struct {
	options  ConsumerOptions
	conn     *connection
	key      string
	consume  Consume
	messages <-chan amqp.Delivery
	restart  chan struct{}
}

//noinspection GoUnusedExportedFunction
func NewConsumer(conn *connection, key string, options ConsumerOptions, consume Consume) Consumer {
	return &consumer{
		conn:    conn,
		key:     key,
		options: options,
		consume: consume,
		restart: make(chan struct{}),
	}
}

func (c *consumer) Consume() {
Main:
	for {
		d, open := <-c.messages
		if !open {
			select {
			case _, open = <-c.restart:
				if !open {
					break Main
				} else {
					c.start()
					continue Main
				}
			}
		} else {
			c.consume(&d)
		}
	}
}

func (c *consumer) Close() {
	close(c.restart)
}

func (c *consumer) start() {
	for {
		pair, err := c.conn.CQPair(c.key)
		if err != nil {
			// TODO Log Error
			break
		}
		o := c.options
		c.messages, err = pair.Channel.Consume(pair.Queue.Name, o.Consumer, o.AutoAck, o.Exclusive, o.NoLocal, o.NoWait, o.Args)
		if err != nil {
			// TODO Log Error
			// TODO Log Retry
			// TODO Implement Exponential Retry And Jitter
			time.Sleep(1 * time.Second)
		}
	}
}

type Init func() error

type connection struct {
	Init      Init
	options   ConnectionOptions
	conn      *amqp.Connection
	channel   *amqp.Channel
	cqPairs   map[string]*CQPair
	consumers Consumers
	onClose   chan *amqp.Error
}

type CQPair struct {
	Queue   *amqp.Queue
	Channel *amqp.Channel
}

type ConnectionOptions struct {
	User     string
	Password string
	Host     string
	Port     string
}

type ExchangeOptions struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueOptions struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp.Table
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type BindOptions struct {
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

func (c *connection) CQPair(key string) (*CQPair, error) {
	q, ok := c.cqPairs[key]
	if !ok {
		return nil, errors.New("invalid key")
	}
	return q, nil
}

func (c *connection) AddConsumer(consumer Consumer) {
	c.consumers = append(c.consumers, consumer)
}

func (c *connection) Consume() {
	c.consumers.Consume()
}

//noinspection GoUnusedExportedFunction
func NewConnection(options ConnectionOptions, init Init) *connection {
	return &connection{
		Init:    init,
		options: options,
		cqPairs: make(map[string]*CQPair),
		onClose: make(chan *amqp.Error),
	}
}

func (c *connection) Start() {
	for {
		if err := c.connect(); err == nil {
			if err := c.Init; err == nil {
				break
			}
		}
		//TODO Log error
		//TODO Implement Exponential Retry And Jitter
		time.Sleep(5 * time.Second)
	}
}

func (c *connection) connect() error {
	var err error
	o := c.options
	c.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s", o.User, o.Password, o.Host, o.Port))
	if err != nil {
		return err
	}
	c.conn.NotifyClose(c.onClose)
	return nil
}

func (c *connection) handleClose() {
	for {
		<-c.onClose
		c.onClose = make(chan *amqp.Error)
		//TODO Log
		//TODO Add Jitter
		time.Sleep(5 * time.Second)
		c.Start()
		//TODO Add Flag to determine if consume should be called
		c.Consume()
	}
}

func (c *connection) CreateCQExchange(key string, options ExchangeOptions) error {
	pair, err := c.CQPair(key)
	if err != nil {
		return err
	}
	if err = pair.Channel.ExchangeDeclare(options.Name, options.Kind, options.Durable, options.AutoDelete, options.Internal, options.NoWait, options.Args); err != nil {
		return err
	}
	return nil
}

func (c *connection) CreateCQPair(key string, options QueueOptions) error {
	var err error
	pair := &CQPair{}
	pair.Channel, err = c.conn.Channel()
	if err != nil {
		return err
	}
	if err = pair.Channel.Qos(options.PrefetchCount, options.PrefetchSize, options.Global); err != nil {
		return err
	}
	q, err := pair.Channel.QueueDeclare(options.Name, options.Durable, options.AutoDelete, options.Exclusive, options.NoWait, options.Args)
	if err != nil {
		return err
	}
	pair.Queue = &q
	c.cqPairs[key] = pair
	return nil
}

func (c *connection) BindCQPairQueue(key string, options BindOptions) error {
	pair, err := c.CQPair(key)
	if err != nil {
		return err
	}

	err = pair.Channel.QueueBind(pair.Queue.Name, options.Key, options.Exchange, options.NoWait, options.Args)
	if err != nil {
		return err
	}

	return nil
}
