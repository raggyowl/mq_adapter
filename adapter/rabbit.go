package adapter

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
	//ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Table)
	err := r.Channel.ExchangeDeclare(name, "topic", true, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed create or connect to exchange %s", name)
	}
	return nil
}

//Fetch data from exchange by routing key and send it to url
func (r *RabbitAdapter) Fetch(routingKey, exchange, url string) error {
	//QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Table)
	queue, err := r.Channel.QueueDeclare(fmt.Sprintf("test_mq_adapter_queue_%s", routingKey), true, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed declare queue %s", err)
	}
	//QueueBind(name, key, exchange string, noWait bool, args Table)
	err = r.Channel.QueueBind(queue.Name, routingKey, exchange, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed bind queue %s to %s", queue.Name, exchange)
	}
	//Consume(queue, consumer string, autoAsk, exclusive noLocal, noWait bool args Table)
	msgs, err := r.Channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed consume messages from %s", queue.Name)
	}
	go func() {
		for m := range msgs {
			if resp, err := r.Client.Post(fmt.Sprintf("%s/%s", url, routingKey), "application/json", bytes.NewReader(m.Body)); err != nil {
				log.Warningf("Dispatch message with key %s failed: %s", routingKey, err)
			} else {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					//Ask(multiple bool)
					m.Ack(true)
					continue
				}
			}
			//Nask(multipli, requeue bool)
			m.Nack(true, true)
		}
	}()

	return nil
}

//Dispatch data to exchange by routing key
func (r *RabbitAdapter) Dispatch(routingKey, contentType, exchange string, data []byte) error {
	//Publish(exchange, key, mandatory, immediate bool, msg Publishing)
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
