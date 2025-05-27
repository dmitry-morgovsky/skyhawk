package internal

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Releases resources from signal.NotifyContext

	// Set up the Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func(closer io.Closer) {
		if err := closer.Close(); err != nil {
			log.Println(fmt.Errorf("failed to close Redis: %w", err))
		}
	}(rdb)

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis at %q: %v", redisAddr, err)
	}
	log.Println(fmt.Sprintf("Successfully connected to Redis at %q", redisAddr))

	if err := startServer(ctx, rdb); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}
