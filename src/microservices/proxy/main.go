package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	monolithURL := os.Getenv("MONOLITH_URL")
	gradualMigration := os.Getenv("GRADUAL_MIGRATION") == "true"
	moviesServiceURL := os.Getenv("MOVIES_SERVICE_URL")
	eventsServiceURL := os.Getenv("EVENTS_SERVICE_URL")

	moviesMigrationPercent := os.Getenv("MOVIES_MIGRATION_PERCENT")
	if moviesMigrationPercent == "" {
		moviesMigrationPercent = "0"
	}
	moviesMigrationPercentInt, err := strconv.Atoi(moviesMigrationPercent)
	if err != nil {
		log.Fatal(err)
	}

	// Set up HTTP routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/", routeRequest(monolithURL, moviesServiceURL, eventsServiceURL, gradualMigration, moviesMigrationPercentInt))

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	data := "{}"
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(data))
}

// Маршрутизация запросов
func routeRequest(
	monolithURL string,
	moviesServiceURL string,
	eventsServiceURL string,
	gradualMigration bool,
	moviesMigrationPercentInt int,
) func(w http.ResponseWriter, r *http.Request) {

	return func(rw http.ResponseWriter, req *http.Request) {
		// берем путь из запроса
		url := req.URL.Path
		query := req.URL.Query().Encode()
		method := req.Method

		// читаем параметры запроса
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		// определяем куда кидать запрос
		var targetURL string = monolithURL
		if strings.HasPrefix(url, "/api/movies") && gradualMigration && rand.Intn(100) < moviesMigrationPercentInt {
			targetURL = moviesServiceURL
		} else if strings.HasPrefix(url, "/api/events") {
			targetURL = eventsServiceURL
		}

		targetURL = targetURL + url
		if query != "" {
			targetURL = targetURL + "?" + query
		}

		// кидаем запрос на сервис
		request, err := http.NewRequest(method, targetURL, bytes.NewBuffer(body))
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		request.Header = req.Header
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		defer response.Body.Close()

		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		// передаем ответ и хедеры дальше
		rw.WriteHeader(response.StatusCode)
		rw.Write(responseBody)
		for key, values := range response.Header {
			for _, value := range values {
				rw.Header().Add(key, value)
			}
		}
	}
}
