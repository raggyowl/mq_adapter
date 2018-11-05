package main

import (
	"crypto/tls"
	"io/ioutil"
	"github.com/gorilla/mux"
	"net/http"
	"github.com/walkohm/mq_adapter/adapter"
	"log"
	"github.com/streadway/amqp"
	
)




func main(){
	conn,err:=amqp.Dial("amqp://guest:guest@localhost:5672")
	if err!=nil{
		log.Println(err)
	}

	ch,err:=conn.Channel()
	if err!=nil{
		log.Println(err)
	}

	a:=adapter.RabbitAdapter{ch,&http.Client{Transport: &http.Transport{TLSClientConfig:&tls.Config{InsecureSkipVerify: true}}}}
	var exchange = "testExchange"

	ch.ExchangeDeclare(exchange,"topic",false,false,false,false,nil)


	a.Fetch("#",exchange,"http://localhost:9091/test")

	router:=mux.NewRouter()


	router.HandleFunc("/test",func(rw http.ResponseWriter, r* http.Request){
		defer r.Body.Close()
		
		if data,err:=ioutil.ReadAll(r.Body); err!=nil{
			log.Println(err)
		} else {
			log.Println(string(data))
		}
		
	})

	log.Fatal(http.ListenAndServe(":9091",router))



	
}
