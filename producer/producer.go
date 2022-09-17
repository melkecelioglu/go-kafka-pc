package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"gopkg.in/resty.v0"
	"log"
	"time"
)

type CoinapiStruct struct {
	Time         time.Time `json:"time"`
	AssetIDBase  string    `json:"asset_id_base"`
	AssetIDQuote string    `json:"asset_id_quote"`
	Rate         float64   `json:"rate"`
}

//var top1, top2, top3 string = "topicA", "topicX", "topicC"

func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topicX", 0)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	for {
		res, err := resty.R().
			SetHeader("X-CoinAPI-Key", "BC9EC132-C27F-4A65-8F63-E8F64F20B2D3").
			Get("https://rest.coinapi.io/v1/exchangerate/ETH/USD")
		//res, err := http.Get("https://rest.coinapi.io/v1/exchangerate/ETH/USD")

		if err != nil {
			log.Fatal(err)
		}

		var response CoinapiStruct
		json.Unmarshal(res.Body, &response)
		//fmt.Println(response.Rate)

		name := fmt.Sprint(response.Rate)

		conn.WriteMessages(kafka.Message{Value: []byte(name)})

		<-time.After(time.Second)
		//	time.Sleep(2 * time.Second) same as 42.
	}

}
