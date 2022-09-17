package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topicA", 0)
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))

	//message, _ := conn.ReadMessage(1e6) //only one message 1e3 =1000
	batch := conn.ReadBatch(1e3, 1e9)
	bytes := make([]byte, 1e3) //locate bytes 1 kb

	for {
		_, err := batch.Read(bytes)

		if err != nil {
			break
		}
		fmt.Println(string(bytes))
	}
	//conn.ReadBatchWith()
}
