package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/dev-hack95/go-kafka/utilities/logs"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.43.163:9092",
	})

	if err != nil {
		logs.Error("Error creating producer: ", err)
		return
	}

	topic := "Kafkatopic1"

	go func() {

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "192.168.43.163:9092",
			"group.id":          "test-group",
		})

		if err != nil {
			logs.Error("Error creating consumer: ", err)
			return
		}

		err = consumer.SubscribeTopics([]string{topic}, nil)

		if err != nil {
			logs.Error("Error subscribing to topic: ", err)
			return
		}

		for {
			// Erro point
			event := consumer.Poll(100)
			fmt.Println(event)
			switch e := event.(type) {
			case *kafka.Message:
				fmt.Printf("%s\n", string(e.Value))
			case *kafka.Error:
				fmt.Printf("%s\n", e)
			}
		}

	}()

	deliverCh := make(chan kafka.Event, 10000)
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("foo"),
		}, deliverCh)

		if err != nil {
			logs.Error("Error producing message: ", err)
		}
		<-deliverCh
		time.Sleep(time.Second * 3)
	}

}
