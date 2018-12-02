package adapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Consumer implement rabbitroutine.Consumer interface.
type Consumer struct {
	IncomeExchangeName string
	OutcomeExchangeName string
	QueueName    string
}

func (c *Consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.IncomeExchangeName, // name
		"topic",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("failed to declare exchange %v: %v", c.IncomeExchangeName, err)

		return err
	}

	err = ch.ExchangeDeclare(
		c.OutcomeExchangeName, // name
		"topic",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("failed to declare exchange %v: %v", c.OutcomeExchangeName, err)

		return err
	}
	_, err = ch.QueueDeclare(
		c.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Printf("failed to declare queue %v: %v", c.QueueName, err)

		return err
	}

	err = ch.QueueBind(
		c.QueueName,    // queue name
		"#",    // routing key
		c.IncomeExchangeName, // exchange
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("failed to bind queue %v: %v", c.QueueName, err)

		return err
	}

	return nil
}

// Consume implement rabbitroutine.Consumer.(Consume) interface method.
func (c *Consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	defer log.Println("consume method finished")
	out := os.Getenv("OUT_URL")
	if out == "" {
		return errors.New("OUT URL not specified")
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: 2 * time.Minute,
	}
	

	msgs, err := ch.Consume(
		c.QueueName,  // queue
		"mq_adapter", // consumer name
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Printf("failed to consume %v: %v", c.QueueName, err)

		return err
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}
			//fmt.Println("New message:", content)
			resp,err:=client.Post(fmt.Sprintf("%s/%s", out, msg.RoutingKey), "application/json", bytes.NewReader(msg.Body))
			if err!=nil{
				log.Printf("failde to Send data: %v",err)
				msg.Nack(false,true)
				continue
			}
			defer resp.Body.Close()
			if resp.StatusCode == 200{
				 msg.Ack(false)
				 continue 
			}
			msg.Nack(false,true)
			
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}


