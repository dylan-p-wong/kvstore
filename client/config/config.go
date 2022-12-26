package config

import (
	"errors"
	"flag"
	"strconv"
	"strings"
)

type ClientConfig struct {
	Servers map[int]string
}

func parseServers(serversString *string) (map[int]string, error) {
	m := make(map[int]string)
	for _, server := range strings.Split(*serversString, ",") {
		ss := strings.Split(server, "=")

		if len(ss) != 2 {
			return nil, errors.New("invalid config.")
		}

		sid, err := strconv.Atoi(ss[0])
		if err != nil {
			return nil, err
		}

		m[sid] = ss[1]
	}

	return m, nil
}

func LoadConfig() (ClientConfig, error) {
	servers := flag.String("servers", "", "server ports")
	flag.Parse()

	m, err := parseServers(servers)

	if err != nil {
		return ClientConfig{}, err
	}

	return ClientConfig{
		Servers: m,
	}, nil
}
