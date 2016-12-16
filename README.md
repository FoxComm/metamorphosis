# metamorphosis #

metamorphosis is a Go client for easily interacting with Kafka. It works best
when used to handle a Kafka setup that's clustered with Zookeeper and whose
messages are encoded with Avro.

## Usage ##

```go
import "github.com/FoxComm/metamorphosis"
```

Construct a new consumer by creating a Consumer that connects to Zookeeper and
the Avro schema registry and provide offset reset strategy value ("largest" or "smallest").

For example:

```go
zookeeper := "localhost:2181"
schemaRepo := "http://localhost:8081"
resetOffsetStrategy := "smallest" // or "largest"

consumer, err := metamorphosis.NewConsumer(zookeeper, schemaRepo, resetOffsetStrategy)
```

To handle messages, define a handler and run against a topic:

```go
handler := func(message AvroMessage) error {
  bytes := message.Bytes()
  fmt.Println(string(bytes))
  return nil
}

consumer.RunTopic("my_topic", handler)
```

## License ##

MIT
