package adapter

import (
	"net/http/httptest"
	"net/http"
	"crypto/tls"
	"testing"
	"github.com/streadway/amqp"
	"io/ioutil"
)

const rabbitHost = "amqp://localhost:5672/%2f"




func Test_Fetch(t *testing.T){
	conn,err:=amqp.Dial("amqp://guest:guest@localhost:5672")
	defer conn.Close()
	if err!=nil{
		t.Error(err)
	}

	ch,err:=conn.Channel()
	if err!=nil{
		t.Error(err)
	}

	a:=&RabbitAdapter{ch,&http.Client{Transport: &http.Transport{TLSClientConfig:&tls.Config{InsecureSkipVerify: true}}}}
	var exchange = "testExchange"

	ch.ExchangeDeclare(exchange,"topic",false,false,false,false,nil)


	

	ts:=httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r* http.Request){
		defer r.Body.Close()
		
		if data,err:=ioutil.ReadAll(r.Body); err!=nil{
			t.Error(err)
		} else {
			assert(t,string(data)=="test","Ahtung!!")
		}
		
	}))
	defer ts.Close()

	if err:=a.Fetch("#",exchange,ts.URL);err!=nil{
		t.Error(err)
	}

	a.Channel.Publish(exchange,"#",false,false,amqp.Publishing{
	ContentType: "application/json",
	Body: []byte("te2st"),
	})











}