package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

const TestTopic = "test-topic"

type Producer struct {
	p *kafka.Producer
}

func NewProducer() *Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "local",
		"acks":              "all",
	})
	if err != nil {
		panic(err)
	}
	return &Producer{producer}
}

func (p *Producer) Produce(topic string, message string) {
	deliveryChan := make(chan kafka.Event)
	err := p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, deliveryChan)
	if err != nil {
		log.Fatal(err)
	}

	e := <-deliveryChan

	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Printf("Failed to deliver message: %v\n", m.TopicPartition)
	} else {
		log.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
