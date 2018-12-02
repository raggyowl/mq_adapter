package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/furdarius/rabbitroutine"
	"github.com/gorilla/mux"
	"github.com/streadway/amqp"
	"github.com/walkohm/mq_adapter/adapter"
)

func main() {
	ctx := context.Background()

	income := os.Getenv("income")
	outcome := os.Getenv("outcome")
	if income == "" || outcome == "" {
		log.Fatal("Exchanges not specified")
	}

	url := os.Getenv("RABBIT_URL")
	if url == "" {
		log.Fatal("Rabbit url not specified")
	}

	conn := rabbitroutine.NewConnector(rabbitroutine.Config{
		// Max reconnect attempts
		ReconnectAttempts: 20,
		// How long wait between reconnect
		Wait: 5 * time.Second,
	})
	conn.AddRetriedListener(func(r rabbitroutine.Retried) {
		log.Printf("try to connect to RabbitMQ: attempt=%d, error=\"%v\"",
			r.ReconnectAttempt, r.Error)
	})

	conn.AddDialedListener(func(_ rabbitroutine.Dialed) {
		log.Printf("RabbitMQ connection successfully established")
	})

	conn.AddAMQPNotifiedListener(func(n rabbitroutine.AMQPNotified) {
		log.Printf("RabbitMQ error received: %v", n.Error)
	})

	consumer := &adapter.Consumer{
		IncomeExchangeName:  income,
		OutcomeExchangeName: outcome,
		QueueName:           fmt.Sprintf("%s_queue", income),
	}

	pool := rabbitroutine.NewPool(conn)
	ensurePub := rabbitroutine.NewEnsurePublisher(pool)
	pub := rabbitroutine.NewRetryPublisher(
		ensurePub,
		rabbitroutine.PublishMaxAttemptsSetup(16),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(10*time.Millisecond)),
	)
	fmt.Println(pub)

	go func() {
		err := conn.Dial(ctx, url)
		if err != nil {
			log.Println("failed to establish RabbitMQ connection:", err)
		}
	}()

	go func() {
		err := conn.StartMultipleConsumers(ctx, consumer, 5)
		if err != nil {
			log.Println("failed to start consumer:", err)
		}
	}()

	go func() {
		router := mux.NewRouter()
		router.HandleFunc("/{c}", func(rw http.ResponseWriter, r *http.Request) {
			_, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
			defer cancel()
			defer r.Body.Close()
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(mux.Vars(r)["c"]))
		})
		router.HandleFunc("/input/{route}", func(rw http.ResponseWriter, r *http.Request) {
			timeoutCtx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
			defer cancel()
			rk := mux.Vars(r)
			defer r.Body.Close()
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("failed to parse: %v", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			err = pub.Publish(timeoutCtx, outcome, rk["route"], amqp.Publishing{
				Body: data,
			})
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				log.Println("failed to publish:", err)
				return
			}

			rw.WriteHeader(http.StatusOK)

		})

		log.Fatal(http.ListenAndServe(":9090", router))
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, os.Interrupt, syscall.SIGTERM)

	// Wait for OS termination signal
	<-sigc
}
