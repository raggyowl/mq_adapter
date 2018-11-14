package main

import (
	"crypto/tls"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/walkohm/mq_adapter/adapter"
	"io/ioutil"
	"net/http"
	"os"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})
}

func main() {
	var rabbitURL = os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		log.Fatal("Rabbit url not specified")
	}
	var inputExchange = os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		//default value
		inputExchange = "input_exchange_mq_adapter"
	}

	var outputExchange = os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		//default value
		outputExchange = "output_exchange_mq_adapter"
	}

	//test
	var client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Error(err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		log.Error(err)
	}
	ra := &adapter.RabbitAdapter{Channel: channel, Client: client}
	defer ra.Close()

	if err = ra.CreateOrConnectExchange(outputExchange); err != nil {
		log.Error(err)
	}
	if err = ra.CreateOrConnectExchange(inputExchange); err != nil {
		log.Error(err)
	}

	if outURL := os.Getenv("OUT_URL"); outURL != "" {
		//# -  listen all
		if err = ra.Fetch("#", outputExchange, outURL); err != nil {
			log.Error(err)
		}
	} else {
		log.Error("OUT_URL not specified")
	}

	router := mux.NewRouter()
	router.HandleFunc("/input/{route}", func(writer http.ResponseWriter, request *http.Request) {
		var contentType string
		if contentType = request.Header.Get("Content-Type"); contentType == "" {
			contentType = "application/json"
		}
		vars := mux.Vars(request)
		defer request.Body.Close()
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			log.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			writer.Write([]byte(err.Error()))
		}

		err = ra.Dispatch(vars["route"], contentType, inputExchange, body)
		if err != nil {
			log.Error(err)
			writer.WriteHeader(http.StatusInternalServerError)
			writer.Write([]byte(err.Error()))
			
		}

	})
	log.Info("Start server on port 9090")
	log.Error(http.ListenAndServe(":9090", router))

}
