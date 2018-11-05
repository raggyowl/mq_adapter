package adapter

import (
	"github.com/streadway/amqp"
	"net/http"
	"github.com/pkg/errors"
	"fmt"
	"bytes"
)

type Adapter interface{
	Fetch(routingKey,exchange,url string) error
	Dispatch(rountigKey,exchange string,data []byte) error
}


type RabbitAdapter struct{
	Channel *amqp.Channel
	Client *http.Client
}


//Fetch implements Adapter
func(r *RabbitAdapter) Fetch(routingKey,exchange,url string) error{
	queue,err:=r.Channel.QueueDeclare(fmt.Sprintf("test_queue_%s", routingKey),false,false,false,false,nil)
	if err!=nil{
		return errors.Wrapf(err,"Failed declare queue %s",err)
	}
	err=r.Channel.QueueBind(queue.Name,routingKey,exchange,false,nil)
	if err != nil {
		return errors.Wrapf(err, "Failed bind queue %s to %s",queue.Name,exchange)
	}
	msgs,err:= r.Channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed consume messages from %s",queue.Name)
	}
	go func(){
		for m := range msgs {
			r.Client.Post(url,"application/json",bytes.NewReader(m.Body))
		}
	}()

	return nil
}

