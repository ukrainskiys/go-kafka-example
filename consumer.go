package main

import "github.com/ukrainskiys/go-kafka-example/consumer"

func main() {
	c := consumer.NewConsumer()
	c.Listen()
}
