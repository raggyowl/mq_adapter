package adapter


type Adapter interface{
	Fetch(routingKey,exchange,url string) error
	Dispatch(rountigKey,exchange string,data []byte) error
	Close()
}


