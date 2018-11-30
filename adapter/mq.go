package adapter


import(
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
)

const(
	ALL_ROUTES = "#"
	CONTENT_TYPE = "application/json"
)

//Adapter is an interface that describes behavior to an object that consumes
 //and publishes messages from/to a queues and delivers them to recipients.
type Adapter interface {
	Fetch(url string) error
	Dispatch(routingKey, data []byte) error
}



