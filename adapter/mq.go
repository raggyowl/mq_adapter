package adapter


//Adapter is an interface that describes behavior to an object that consumes
 //and publishes messages from/to a queues and delivers them to recipients.
type Adapter interface {
	Fetch(routingKey, exchange, url string) error
	Dispatch(routingKey, contentType, exchange string, data []byte) error
	Close()
}
