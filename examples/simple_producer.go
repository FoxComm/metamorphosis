package main

import "github.com/FoxComm/metamorphosis"

const (
	broker         = "127.0.0.1:9092"
	zookeeper      = "127.0.0.1:2181"
	schemaRegistry = "http://127.0.0.1:8081"
)

func main() {
	p, err := metamorphosis.NewProducer(broker, zookeeper, schemaRegistry)
	if err != nil {
		panic(err)
	}

	topic := "metamorphosis_hello"
	message := "Hello, World!"

	if err := p.Emit(topic, message); err != nil {
		panic(err)
	}
}
