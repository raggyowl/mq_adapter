package adapter


import (
	"context"
	log "github.com/sirupsen/logrus"
	"crypto/tls"
	"net/http"
	"time"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/furdarius/rabbitroutine"
	"bytes"
	"fmt"
	"os"
)

const (
	ALL_ROUTES = "#"
	CONTENT_TYPE = "application/json"
)
//TODO: Write tests





type RabbitAdapter struct{
	Consumer *Consumer
	Publisher *Publisher
}

func NewRabbitAdapter(conn *rabbitroutine.Connector)*RabbitAdapter{
	income:=os.Getenv("INCOME_EXCHANGE")
	if income == ""{
		income = "income_exchange_mq_adapter"
	}
	outcome:=os.Getenv("OUTCOME_EXCHANGE")
	if outcome == ""{
		outcome = "outcome_exchange_mq_adapter"
	}
	outcomeQueue:="outcome_queue_mq_adapter"
	consumer:=&Consumer{
		ExchangeName: outcome,
		QueueName: outcomeQueue,
	}

	return &RabbitAdapter{
		Consumer:consumer,
		Publisher:NewPublisher(conn, income),
	}
}

//Publisher is wrapper around rabbitroutine.RetryPublisher
type Publisher struct{
	publisher *rabbitroutine.RetryPublisher
	ExchangeName
}

//Pusblish is a wrapper arounc rabbitroutine.RetryPublisher.(Publish) method.
func(p *Publisher)Publish(ctx context.Context, routingKey string, msg []byte) error {
	err := p.publisher.Publish(ctx, p.ExchangeName, routingKey,amqp.Publishing{
		ContentType: CONTENT_TYPE,
		Body: msg,
	})
	return errors.Wrapf(err, "Failed to publish to %s",p.ExchangeName)
}

func NewPublisher(conn *rabbitroutine.Connector,exchangeName string)*Publisher{
	pool:=rabbitroutine.NewPool(conn)
	ensurePub:=rabbitroutine.NewEnsurePublisher(pool)
	return &Publisher{
		publisher: rabbitroutine.NewRetryPublisher(
			ensurePub,
			rabbitroutine.PublishMaxAttemptsSetup(16),
			rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(100*time.Millisecond))
		),
		ExchangeName: exchangeName,
	}
}

func (p *Publisher)Declare(ctx context.Context,ch *amqp.Channel) error{
	err:=ch.ExchangeDeclare(p.ExchangeName, "topic", true, false, false, false, nil)
	if err!=nil{
		return errors.Wrapf(err, "Failed to declare exchange %s", p.ExchangeName)
	}
	return nil
}

// Consumer implement rabbitroutine.Consumer interface.
type Consumer struct {
	ExchangeName string
	QueueName    string
}

// Declare implement rabbitroutine.Consumer.(Declare) interface method.
func (c *Consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.ExchangeName, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return errors.Wrapf(err,"Failed to declare exchange %s", c.ExchangeName)
	}

	_, err = ch.QueueDeclare(
		c.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,
	)
	if err != nil {
		return errors.Wrapf(err,"Failed to declare queue %s", c.QueueName)
	}

	err = ch.QueueBind(
		c.QueueName,    // queue name
		c.QueueName,    // routing key
		c.ExchangeName, // exchange
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return errors.Wrapf(err,"Failed to bind queue %v: %v", c.QueueName)
	}

	return nil
}

// Consume implement rabbitroutine.Consumer.(Consume) interface method.
func (c *Consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	//clean
	defer log.Println("consume method finished")

	var url  = ctx.Value("url")
	var client = &http.Client{
		Transport:	&http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: ctx.Value("verify").(bool),
			},
		},
		Timeout: ctx.Value("timeout").(time.Duration),
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
		return errors.Wrapf(err,"Failed to consume %s", c.QueueName)
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}
			resp, err := client.Post(fmt.Sprintf("%s/%s",url,msg.RoutingKey),CONTENT_TYPE,bytes.NewReader(msg.Body))
			defer resp.Body.Close()
			if err!=nil{
				log.Warningf("Failed to dispatch message to %s: %v",url,err)
			}
			if resp.StatusCode == 200{
				msg.Ack(false)
			} else {
				msg.Nack(false, true)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}