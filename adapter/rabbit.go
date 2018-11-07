package adapter

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"net/http"
)

//TODO: Write tests

type RabbitAdapter struct {
	Channel *amqp.Channel
	Client  *http.Client
}

//CreateOrConnectExchange create topic exchange if does not exist or connect to exists exchange
func (r *RabbitAdapter) CreateOrConnectExchange(name string) error {
	err := r.Channel.ExchangeDeclare(name, "topic", false, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed create or connect to exchange %s", name)
	}
	return nil
}

//Fetch data from exchange by routing key and send it to url
func (r *RabbitAdapter) Fetch(routingKey, exchange, url string) error {
	queue, err := r.Channel.QueueDeclare(fmt.Sprintf("test_queue_%s", routingKey), false, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed declare queue %s", err)
	}
	err = r.Channel.QueueBind(queue.Name, routingKey, exchange, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed bind queue %s to %s", queue.Name, exchange)
	}
	msgs, err := r.Channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed consume messages from %s", queue.Name)
	}
	go func() {
		for m := range msgs {
			r.Client.Post(url, "application/json", bytes.NewReader(m.Body))
		}
	}()

	return nil
}

//Dispatch data to exchange by routing key
func (r *RabbitAdapter) Dispatch(routingKey, contentType, exchange string, data []byte) error {
	err := r.Channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		ContentType: contentType,
		Body:        data,
	})
	if err != nil {
		return errors.Wrapf(err, "Failed publish message to %s exchange", exchange)
	}
	return nil
}

//Close opened channel
func (r *RabbitAdapter) Close() {
	defer r.Channel.Close()
}
