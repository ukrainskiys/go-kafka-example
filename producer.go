package main

import (
	"github.com/ukrainskiys/go-kafka-example/producer"
	"net/http"
)

func main() {
	p := producer.NewProducer()
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		message := request.URL.Query().Get("message")
		if message != "" {
			p.Produce(producer.TestTopic, message)
			_, _ = writer.Write([]byte(message))
		}
	})

	_ = http.ListenAndServe(":8080", nil)
}
