package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"net/url"
)

func startServer(ctx context.Context, rdb *redis.Client) error {
	r := mux.NewRouter()

	r.HandleFunc("/api/v1/statistics/player/{player}/season/{season}", handle(ctx, "player", rdb)).Methods("GET")
	r.HandleFunc("/api/v1/statistics/team/{team}/season/{season}", handle(ctx, "team", rdb)).Methods("GET")

	log.Println("NBA Players/Teams Statistics server is running")
	if err := http.ListenAndServe(":8080", r); err != nil {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}

	return nil
}

func handle(ctx context.Context, subject string, rdb *redis.Client) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		name, err := url.PathUnescape(vars[subject])
		if err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("failed to unescape '%s' parameter: %w", subject, err))
			return
		}

		season, err := url.PathUnescape(vars["season"])
		if err != nil {
			respondError(w, http.StatusBadRequest, fmt.Errorf("failed to unescape 'season' parameter: %w", err))
			return
		}

		key := fmt.Sprintf("%s:%s:%s", subject, name, season)
		val, err := rdb.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				respondError(w, http.StatusNotFound, fmt.Errorf("statistics for %s %q on season %s not found", subject, name, season))
				return
			}

			respondError(w, http.StatusInternalServerError, fmt.Errorf("failed to GET %q key from Redis: %w", key, err))
			return
		}

		// Success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprint(w, val); err != nil {
			log.Printf("Error writing response: %v", err)
		}
	}
}

// respondError logs the error and write it to http.ResponseWriter with the given statusCode
func respondError(w http.ResponseWriter, statusCode int, err error) {
	log.Println(err)
	http.Error(w, fmt.Sprintf("ERROR: %s", err.Error()), statusCode)
}
