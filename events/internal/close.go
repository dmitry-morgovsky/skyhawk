package internal

import (
	"fmt"
	"io"
	"log"
)

// closeIt closes given io.Closer and logs error if any
func closeIt(message string, closer io.Closer) {
	if err := closer.Close(); err != nil {
		log.Println(fmt.Errorf("failed to close %s: %w", message, err))
	}
}
