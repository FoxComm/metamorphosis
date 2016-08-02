# metamorphosis #

metamorphosis is a Go client for easily interacting with Kafka. It works best
when used to handle a Kafka setup that's clustered with Zookeeper and whose
messages are encoded with Avro.

## Usage ##

```go
import "github.com/FoxComm/metamorphosis
```

Construct a new consumer by pointing defining a message handler, as well as the
connection settings for Zookeeper and the Avro schema repository.  

For example:

```go
zookeeper := "localhost:2181"
schemaRepo := "http://localhost:8081"
handler := func(message *[]byte) error {
  fmt.Println(string(message))
  return nil
}

consumer, err := metamorphosis.NewConsumer(zookeeper, schemaRepo, handler)
```

To handle messages, just run against a topic:

```go
consumer.RunTopic("my_topic")
```

## License ##

MIT
