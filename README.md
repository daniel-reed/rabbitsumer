# Quick Start

```go
package main

import "log"
import "os"
import "github.com/streadway/amqp"
import "github.com/daniel-reed/rabbitsumer"

func main() {
	// Provide our credentials
	rabbitConnOpts := rabbitsumer.ConnectionOptions{
        User:     "guest",
        Password: "guest",
        Host:     "localhost",
        Port:     "5672",
    }
	// Create an init function that does nothing
    rabbitInit := func () error {
        log.Println("Connecting")
        return nil
    }
    // Create our connection
    rabbitconn := rabbitsumer.NewConnection(rabbitConnOpts, rabbitInit)
    // Turn logging on to stdout
    rabbitconn.Log = log.New(os.Stdout, "rbt: ", 0)
    // Connect
    rabbitconn.Start()
    
    // Create a new Queue
	helloQueueOptions := rabbitsumer.QueueOptions{
		Name:          "hello",
		PrefetchCount: 2,
	}
	// Create a Connection/Queue Pair at "HelloCQPair"
	if err := rabbitconn.CreateCQPair("HelloCQPair", helloQueueOptions); err != nil {
		log.Fatal(err)
	}
	// Get the pair we just created
	helloQueue, err := rabbitconn.CQPair("HelloCQPair")
	if err != nil {
		log.Fatal(err)
	}
	// Publish to the queue
	err = helloQueue.Channel.Publish("", helloQueue.Queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("Hello World!")})
	if err != nil {
		log.Fatal(err)
	}

    // Create the Consumer function
	helloConsumerFunc := func (d *amqp.Delivery) {
		log.Printf("Received Message: %s\n", d.Body)
	}
	// Create the Consumer Options
	helloConsumerOpts := rabbitsumer.ConsumerOptions{
		AutoAck:   true,
	}
	// Create Our Consumer
	helloConsumer := rabbitsumer.NewConsumer(rabbitconn, "HelloCQPair", helloConsumerOpts, helloConsumerFunc)
	// Add it to our connection
	rabbitconn.AddConsumer(helloConsumer)
	// Begin Consuming
	rabbitconn.Consume()

    // Wait for ctrl+c to quit
	forever := make (chan bool)
	<- forever
}
```