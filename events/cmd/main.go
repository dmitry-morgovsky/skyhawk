package main

import (
	"events/internal"
	"log"
)

func main() {
	if err := internal.Run(); err != nil {
		log.Fatal(err)
	}
}
