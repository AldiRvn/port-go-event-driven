package portgoeventdriven

import (
	"bytes"
	"context"
	"encoding/csv"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tidwall/sjson"

	_ "github.com/joho/godotenv/autoload"
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

var kafkaTopic = "users"

func Test_publisher(t *testing.T) {
	slog.Debug("start publisher()")
	now := time.Now()
	defer func() {
		slog.Debug("end publisher()", "time", time.Since(now))
	}()

	// read csv
	csvRaw, err := os.ReadFile("data/10000.synthetic_data_2026-02-11.csv")
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
	dataToInsert := []kafka.Message{}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(os.Getenv("KAFKA_BROKER"), ",")...),
		Topic:                  kafkaTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
		<-exit
		if errClose := writer.Close(); errClose != nil {
			slog.Error(errClose.Error())
		}
		slog.Debug("Done")
	}()

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
	slog.Debug("Insert done", "len data", len(dataToInsert))
}
