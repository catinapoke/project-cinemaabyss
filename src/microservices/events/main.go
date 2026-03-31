package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/lib/pq"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type State struct {
	KafkaProducer *kafka.Producer
	KafkaConsumer *kafka.Consumer
}

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"security.protocol": "PLAINTEXT",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"group.id":          "events-group",
		"auto.offset.reset": "earliest",
		"security.protocol": "PLAINTEXT",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{movieTopic, userTopic, paymentTopic}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := &State{
		KafkaProducer: producer,
		KafkaConsumer: consumer,
	}

	go state.consumeEvents(ctx)

	// Set up HTTP routes
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", state.handleSendMovieEvent)
	http.HandleFunc("/api/events/user", state.handleSendUserEvent)
	http.HandleFunc("/api/events/payment", state.handleSendPaymentEvent)
	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	data := map[string]bool{"status": true}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

type MovieEventRequest struct {
	MovieID int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int    `json:"user_id"`
}

func (s *State) handleSendMovieEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var event MovieEventRequest
	err = json.Unmarshal(body, &event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.sendEvent(event, movieTopic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)

	data := map[string]string{"status": "success"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

type UserEventRequest struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

func (s *State) handleSendUserEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var event UserEventRequest
	err = json.Unmarshal(body, &event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.sendEvent(event, userTopic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	data := map[string]string{"status": "success"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

type PaymentEventRequest struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

func (s *State) handleSendPaymentEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var event PaymentEventRequest
	err = json.Unmarshal(body, &event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.sendEvent(event, paymentTopic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	data := map[string]string{"status": "success"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *State) sendEvent(event any, topic string) error {
	json, err := json.Marshal(event)
	if err != nil {
		return err
	}

	fmt.Printf("Sending event %v to topic: %s", event, topic)
	err = s.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          json,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *State) consumeEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := s.KafkaConsumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Error reading message: %v\n", err)
				continue
			}

			topic := msg.TopicPartition.Topic
			if topic == nil {
				continue
			}

			switch *topic {
			case movieTopic:
				var event MovieEventRequest
				err = json.Unmarshal(msg.Value, &event)
				if err != nil {
					log.Printf("Error unmarshalling message: %v\n", err)
					continue
				}
				fmt.Println("Received movie event:", event)
			case userTopic:
				var event UserEventRequest
				err = json.Unmarshal(msg.Value, &event)
				if err != nil {
					log.Printf("Error unmarshalling message: %v\n", err)
					continue
				}
				fmt.Println("Received user event:", event)
			case paymentTopic:
				var event PaymentEventRequest
				err = json.Unmarshal(msg.Value, &event)
				if err != nil {
					log.Printf("Error unmarshalling message: %v\n", err)
					continue
				}
				fmt.Println("Received payment event:", event)
			default:
				fmt.Println("Received unknown event in topic:", *topic)
			}

			s.KafkaConsumer.CommitMessage(msg)
		}
	}
}
