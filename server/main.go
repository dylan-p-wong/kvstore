package main

import (
	"fmt"
	"net"
	"os"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/config"
	"github.com/dylan-p-wong/kvstore/server/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()

	cfg, err := config.LoadConfig()
	if err != nil {
		sugar.Infow("failed to load config", "err", err)
	}

	listen, err := net.Listen("tcp", fmt.Sprintf("%s", cfg.URL))
	if err != nil {
		sugar.Infow("failed to listen", "err", err)
	}

	server := service.NewServer(cfg.Id, cfg.URL, sugar)

	for k, v := range cfg.Peers {
		server.AddPeer(k, v)
	}

	err = server.Start()

	if err != nil {
		os.Exit(1)
	}

	s := grpc.NewServer()
	pb.RegisterKVServer(s, server)

	sugar.Infow("server listening", "port", listen.Addr())

	if err := s.Serve(listen); err != nil {
		sugar.Infow("failed to server", "err", err)
	}
}
