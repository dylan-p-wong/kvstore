package main

import (
	"fmt"
	"net"
	"os"

	pb "github.com/dylan-p-wong/kvstore/api"
	"github.com/dylan-p-wong/kvstore/server/config"
	"github.com/dylan-p-wong/kvstore/server/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		os.Exit(1)
	}

	logger := setupLogger()
	defer logger.Sync()
	sugar := logger.Sugar().With("node", cfg.Id)

	listen, err := net.Listen("tcp", fmt.Sprintf("%s", cfg.URL))
	if err != nil {
		sugar.Infow("failed to listen", "err", err)
		os.Exit(1)
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

func setupLogger() *zap.Logger {
	consoleDebugging := zapcore.Lock(os.Stdout)
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	core := zapcore.NewTee(zapcore.NewCore(consoleEncoder, consoleDebugging, zapcore.DebugLevel))
	logger := zap.New(core)

	return logger
}
