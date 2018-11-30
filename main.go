package main

import (
	"crypto/tls"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/walkohm/mq_adapter/adapter"
	"github.com/furdarius/rabbitroutine"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"net/http"
	"context"
	"os"
)

var ErrTermSig = errors.New("Termination signal caught")


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
	g,ctx:=errgroup.WithContext(context.Background())

	if outURL := os.Getenv("OUT_URL"); outURL != "" {
		ctx = context.WithValue(ctx, "url", outURL)		
	} else {
		log.Error("OUT_URL not specified")
	}
	ctx = initContextHTTP(ctx)

	
	conn:=rabbitroutine.NewConnector(rabbitroutine.Config{
		Wait: 2*time.Second
	})

	setLiseners(conn)

	ra:=adapter.NewRabbitAdapter(conn)

	g.Go(func() error{
		err:=conn.Dial(ctx, rabbitURL)
		if err != nil {
			log.Fatalf("Failed to establish RabbitMQ connection: %v", err)
			return err
		}
		ch,err:=conn.Channel(ctx)
		if err != nil {
			log.Fatalf("Failed to create channel: %v", err)
			return err
		}
		ra.Publisher.Declare(ctx, ch)
		err = ch.Close()
		if err != nil {
			log.Printf("Failed to close channel: %v", err)
			return err
		}

		return nil
	})


	g.Go(func()error{
		log.Println("Consumers starting")
		defer log.Println("Consumers finished")
		return conn.StartMultipleConsumers(ctx, ra.Consumer, 5)
		
	})

	g.Go(func()error{
		log.Println("Signal notifier starting")
		defer log.Println("signal notifier finished")

		return termSignal(ctx)
	}	
	})

	g.Go(func()error{
		log.Info("Start server on port 9090")
		return http.ListenAndServe(":9090", initRouter(ra))
	})

	if err:=g.Wait();err!=nil && err!=ErrTermSig{
		log.Fatal(
			"Failde to wait goroutine group: ",err
		)
	}

	
	

}

//Create gorilla.mux.Router and add handlefunc for /input/{route} pattern
func initRouter(ra *adapter.RabbitAdapter) *mux.Router {
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
}

//Add Listener which listen main events from rabbitroutine.Connector
func setLiseners(conn *rabbitroutine.Connector){
	conn.AddRetriedListener(func(r rabbitroutine.Retried){
		log.Printf("Try to connect to RabbitMQ attempt=%d, error=\"%v\"", r.ReconnectAttempt,r.Error)
	})

	conn.AddDialedListener(func(_ rabbitroutine.Dialed){
		log.Printf("RabbitMQ connetion successfully established")
	})

	conn.AddAMQPNotifiedListener(func(n rabbitroutine.AMQPNotified){
		log.Printf("RabbitMQ error received: %v", n.Error)
	})
}
//Set settings for http.Client to context
func initContextHTTP(parent context.Context) context.Context{
	//set timeout and ssl-verify to context
	ctx = context.WithValue(parent, "timeout", 10*time.Minutes)
	return context.WithValue(ctx, "verify", false)
}

func termSingal(ctx context.Context) error{
	sigc:=make(chan os.Signal,1)
	signal.Notify(sigc,syscall.SIGINT,os.Interrupt,syscall.SIGTERM)
	select{
	case<-sigc:
		log.Println("Termination signal caught")
		return ErrTermSig
	}
	case<-ctx.Done():
		return ctx.Err()
}