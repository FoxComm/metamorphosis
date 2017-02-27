package metamorphosis

import (
	"fmt"

	kafkaavro "github.com/FoxComm/go-kafka-avro"
	"github.com/FoxComm/siesta"
	siestaProducer "github.com/FoxComm/siesta-producer"
)

// Producer is the interface for emitting data to a Kafka topic.
type Producer interface {
	Emit(topic string, value interface{}) error
}

type defaultProducer struct {
	kafkaProducer *siestaProducer.KafkaProducer
}

func NewProducer(broker, schemaRegistry string) (Producer, error) {
	encoder := kafkaavro.NewKafkaAvroEncoder(schemaRegistry)

	config := siesta.NewConnectorConfig()
	config.BrokerList = []string{broker}

	connector, err := siesta.NewDefaultConnector(config)
	if err != nil {
		return nil, err
	}

	fmt.Printf("%v\n", connector)

	producerConfig := siestaProducer.NewProducerConfig()
	producerConfig.BatchSize = 1

	p := siestaProducer.NewKafkaProducer(
		producerConfig,
		siestaProducer.ByteSerializer,
		encoder.Encode,
		connector,
	)

	return &defaultProducer{p}, nil
}

func (p *defaultProducer) Emit(topic string, value interface{}) error {
	record := siestaProducer.ProducerRecord{Topic: topic, Value: value}
	resultChannel := p.kafkaProducer.Send(&record)
	res := <-resultChannel

	// Disappointing that a string is returned when everything works successfully.
	// This is a limitation of the library that we're using under the hood.
	if res.Error.Error() != "No error - it worked!" {
		return res.Error
	}

	return nil
}
