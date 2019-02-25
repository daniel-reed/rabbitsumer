package rabbitsumer

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"io/ioutil"
	"log"
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
	conn     *Connection
	key      string
	consume  Consume
	messages <-chan amqp.Delivery
	restart  chan struct{}
}

//noinspection GoUnusedExportedFunction
func NewConsumer(conn *Connection, key string, options ConsumerOptions, consume Consume) Consumer {
	return &consumer{
		conn:    conn,
		key:     key,
		options: options,
		consume: consume,
		restart: make(chan struct{}),
	}
}

func (c *consumer) Consume() {
	c.start()
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
			c.conn.Log.Printf("CQPair %q does not exist. Quiting consumer start: %q", c.key, err.Error())
			break
		}
		o := c.options
		c.messages, err = pair.Channel.Consume(pair.Queue.Name, o.Consumer, o.AutoAck, o.Exclusive, o.NoLocal, o.NoWait, o.Args)
		if err != nil {
			c.conn.Log.Printf("CQPair %q failed to consume: %q", c.key, err.Error())
			c.conn.Log.Printf("CQPair %q start retry in 1 second", c.key)
			// TODO Implement Exponential Retry And Jitter
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
}

type Init func() error

type Connection struct {
	Init      Init
	Log       *log.Logger
	options   ConnectionOptions
	conn      *amqp.Connection
	channel   *amqp.Channel
	cqPairs   map[string]*CQPair
	consumers Consumers
	onClose   chan *amqp.Error
	consuming bool
}

type CQPair struct {
	Queue   *amqp.Queue
	Channel *amqp.Channel
}

func (c *CQPair) NewWriter(exchange, contentType string, mandatory, immediate bool) io.Writer {
	return &CQPairWriter{
		CQPair:      c,
		Exchange:    exchange,
		Mandatory:   mandatory,
		Immediate:   immediate,
		ContentType: contentType,
	}
}

type CQPairWriter struct {
	CQPair      *CQPair
	Exchange    string
	Mandatory   bool
	Immediate   bool
	ContentType string
}

// Each call to Write is an individual AMQP message
func (c *CQPairWriter) Write(p []byte) (n int, err error) {
	err = c.CQPair.Channel.Publish(
		c.Exchange,
		c.CQPair.Queue.Name,
		c.Mandatory,
		c.Immediate,
		amqp.Publishing{ContentType: c.ContentType, Body: p},
	)
	if err != nil {
		n = len(p)
	}
	return
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

func (c *Connection) CQPair(key string) (*CQPair, error) {
	q, ok := c.cqPairs[key]
	if !ok {
		return nil, errors.New("invalid key")
	}
	return q, nil
}

func (c *Connection) AddConsumer(consumer Consumer) {
	c.consumers = append(c.consumers, consumer)
}

func (c *Connection) Consume() {
	c.consuming = true
	c.consumers.Consume()
}

//noinspection GoUnusedExportedFunction
func NewConnection(options ConnectionOptions, init Init) *Connection {
	return &Connection{
		Init:    init,
		Log:     log.New(ioutil.Discard, "rbt: ", 0),
		options: options,
		cqPairs: make(map[string]*CQPair),
		onClose: make(chan *amqp.Error),
	}
}

func (c *Connection) Start() {
	var err error
	for {
		if err = c.connect(); err == nil {
			if err = c.Init(); err == nil {
				break
			}
		}
		c.Log.Printf("Failed to connect: %q", err.Error())
		c.Log.Printf("Connect retry in 5 seconds")
		//TODO Implement Exponential Retry And Jitter
		time.Sleep(5 * time.Second)
	}
}

func (c *Connection) connect() error {
	var err error
	o := c.options
	c.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s", o.User, o.Password, o.Host, o.Port))
	if err != nil {
		return err
	}
	c.conn.NotifyClose(c.onClose)
	return nil
}

func (c *Connection) handleClose() {
	for {
		<-c.onClose
		c.onClose = make(chan *amqp.Error)
		c.Log.Println("Connection Interrupted. Connect retry in 5 seconds")
		//TODO Add Jitter
		time.Sleep(5 * time.Second)
		c.Start()
		if c.consuming {
			c.Consume()
		}
	}
}

func (c *Connection) CreateCQExchange(key string, options ExchangeOptions) error {
	pair, err := c.CQPair(key)
	if err != nil {
		return err
	}
	if err = pair.Channel.ExchangeDeclare(options.Name, options.Kind, options.Durable, options.AutoDelete, options.Internal, options.NoWait, options.Args); err != nil {
		return err
	}
	return nil
}

func (c *Connection) CreateCQPair(key string, options QueueOptions) (*CQPair, error) {
	var err error
	pair := &CQPair{}
	pair.Channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err = pair.Channel.Qos(options.PrefetchCount, options.PrefetchSize, options.Global); err != nil {
		return nil, err
	}
	q, err := pair.Channel.QueueDeclare(options.Name, options.Durable, options.AutoDelete, options.Exclusive, options.NoWait, options.Args)
	if err != nil {
		return nil, err
	}
	pair.Queue = &q
	c.cqPairs[key] = pair
	return pair, nil
}

func (c *Connection) BindCQPairQueue(key string, options BindOptions) error {
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
