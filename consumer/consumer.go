package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ukrainskiys/go-kafka-example/producer"
	"log"
)

type Consumer struct {
	c *kafka.Consumer
}

func NewConsumer() *Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		panic(err)
	}

	return &Consumer{c}
}

func (c *Consumer) Listen() {
	err := c.c.SubscribeTopics([]string{producer.TestTopic}, nil)
	if err != nil {
		log.Fatal(err)
	}

	run := true
	for run == true {
		ev := c.c.Poll(-1)
		switch e := ev.(type) {
		case *kafka.Message:
			log.Println(string(e.Value))
		case kafka.Error:
			log.Printf("%% Error: %v\n", e)
			run = false
		}
	}

	_ = c.c.Close()
}
