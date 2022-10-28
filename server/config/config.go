package config

import (
	"errors"
	"flag"
	"strings"
)

type ServerConfig struct {
	Name  string
	URL   string
	Peers map[string]string
}

func LoadConfig() (ServerConfig, error) {
	name := flag.String("name", "node0", "The node name")
	url := flag.String("url", "127.0.0.1:50051", "The server port")
	peers := flag.String("peers", "", "peer ports")

	flag.Parse()

	m := make(map[string]string)
	for _, peer := range strings.Split(*peers, ",") {
		ps := strings.Split(peer, "=")

		if len(ps) != 2 {
			return ServerConfig{}, errors.New("invalid config.")
		}

		m[ps[0]] = ps[1]
	}

	return ServerConfig{
		Name:  *name,
		URL:   *url,
		Peers: m,
	}, nil
}
