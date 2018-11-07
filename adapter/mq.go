package adapter

type Adapter interface {
	Fetch(routingKey, exchange, url string) error
	Dispatch(routingKey, contentType, exchange string, data []byte) error
	Close()
}
