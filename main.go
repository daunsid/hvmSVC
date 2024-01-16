package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer   *kafka.Producer
	topic      string
	deliverych chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	deliverch := make(chan kafka.Event, 10000)
	return &OrderPlacer{
		producer:   p,
		topic:      topic,
		deliverych: deliverch,
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {

	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          payload},
		op.deliverych,
	)
	if err != nil {
		log.Fatal(err)
	}

	<-op.deliverych
	fmt.Printf("placed order on the queue: %s\n", format)
	return nil
}

func main() {
	//topic := "HVSE"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)

	}

	op := NewOrderPlacer(p, "HVSE")
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}

	// deliverch := make(chan kafka.Event, 10000)
	// for {
	// 	err = p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte("FOO")},
	// 		deliverch,
	// 	)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	<-deliverch
	// 	time.Sleep(time.Second * 10)
	// }

	// fmt.Printf("%+v\n", e.String())

	// fmt.Printf("%+v\n", p)
}
