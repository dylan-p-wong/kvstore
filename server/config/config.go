package config

import (
	"errors"
	"flag"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Id    int
	URL   string
	Peers map[int]string
}

func LoadConfig() (ServerConfig, error) {
	id := flag.Int("name", 0, "The node id")
	url := flag.String("url", "127.0.0.1:50051", "The server port")
	peers := flag.String("peers", "", "peer ports")

	flag.Parse()

	m := make(map[int]string)
	for _, peer := range strings.Split(*peers, ",") {
		ps := strings.Split(peer, "=")

		if len(ps) != 2 {
			return ServerConfig{}, errors.New("invalid config.")
		}

		pid, err := strconv.Atoi(ps[0])
		if err != nil {
			return ServerConfig{}, err
		}

		m[pid] = ps[1]
	}

	return ServerConfig{
		Id:    *id,
		URL:   *url,
		Peers: m,
	}, nil
}
