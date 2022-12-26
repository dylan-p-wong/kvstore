package main

import (
	"bufio"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/dylan-p-wong/kvstore/client/cli"
	"github.com/dylan-p-wong/kvstore/client/config"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	config, err := config.LoadConfig()
	if err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}

	client, err := cli.NewClient(config)
	if err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")

		if len(s) == 0 {
			log.Printf("invalid command")
			continue
		}

		op := s[0]

		if op == "GET" {
			if len(s) != 2 {
				log.Printf("invalid number of args")
				continue
			}

			key := s[1]

			value, err := client.Get(key)

			if err != nil {
				log.Printf("GET error: %v", err)
			} else {
				log.Printf("GET value: %s", value)
			}
		} else if op == "PUT" {
			if len(s) != 3 {
				log.Printf("invalid number of args")
				continue
			}
			key := s[1]
			value := s[2]

			err := client.Put(key, value)

			log.Printf("PUT error: %v", err)
		} else if op == "DELETE" {
			if len(s) != 2 {
				log.Printf("invalid number of args")
				continue
			}
			key := s[1]
			err := client.Delete(key)

			log.Printf("DELETE error: %v", err)
		} else {
			log.Printf("invalid operation")
			continue
		}
	}
}
