package portgoeventdriven

import (
	"bytes"
	"context"
	"encoding/csv"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tidwall/sjson"
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

func Test_publisher(t *testing.T) {
	slog.Debug("start publisher()")
	now := time.Now()
	defer func() {
		slog.Debug("end publisher()", "time", time.Since(now))
	}()

	// read csv
	csvRaw, err := os.ReadFile("data/MOCK_DATA.csv")
	if err != nil {
		slog.Error(err.Error())
		return
	}

	csvReader := csv.NewReader(bytes.NewBuffer(csvRaw))
	data, err := csvReader.ReadAll()
	if err != nil {
		slog.Error(err.Error())
		return
	}

	// build messages
	topic := "users"
	dataToInsert := []kafka.Message{}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(os.Getenv("KAFKA_BROKER")),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	header := []string{}
	for index, datum := range data {
		if index == 0 {
			// register header
			header = datum
			continue
		}

		var asJson []byte
		for indexDatum, value := range datum {
			asJson, err = sjson.SetBytes(asJson, header[indexDatum], value)
			if err != nil {
				slog.Error(err.Error())
				continue
			}
		}

		dataToInsert = append(dataToInsert, kafka.Message{
			Key:   []byte(time.Now().Format(time.DateTime)),
			Value: asJson,
		})
	}

	// send messages
	err = writer.WriteMessages(context.Background(), dataToInsert...)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	if err := writer.Close(); err != nil {
		slog.Error(err.Error())
		return
	}
}
