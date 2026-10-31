package handlers

import (
	"encoding/json"
	"time"
	// "my-go-service/internal/ratelimit"
	"net/http"
)

type Status struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

func checkHealth(name, url string) Status {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/health")
	if err != nil {
		return Status{Name: name, Healthy: false, Error: err.Error()}
	}
	defer resp.Body.Close()
	// можно дополнительно проверить код ответа и тело
	return Status{Name: name, Healthy: true}
}

func ServiceStausApilHandler(w http.ResponseWriter, r *http.Request) {
	services := []struct {
		name string
		url  string
	}{
		{"Service Relay", "http://localhost:8081"},
		{"Service Worker", "http://localhost:8082"},
		// {"Service C", "http://localhost:8083"},
	}

	var statuses []Status
	for _, s := range services {
		statuses = append(statuses, checkHealth(s.name, s.url))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// func main() {
// 	// 5 запросов в секунду, всплеск до 10
// 	tb := ratelimit.NewTokenBucket(5, 10)

// 	mux := http.NewServeMux()
// 	mux.HandleFunc("/hello", helloHandler)
// 	mux.HandleFunc("/", testPageHandler)

// 	handler := ratelimit.RateLimitMiddleware(tb)(mux)

// 	log.Println("Server started on :8080")
// 	log.Fatal(http.ListenAndServe(":8080", handler))
// }
