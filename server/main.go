package main

import (
	"math/rand"
	"os"
	"time"

	"github.com/dylan-p-wong/kvstore/server/config"
	"github.com/dylan-p-wong/kvstore/server/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// for random election timeouts
	rand.Seed(time.Now().UnixNano())

	cfg, err := config.LoadConfig()
	if err != nil {
		os.Exit(1)
	}

	logger := setupLogger()
	defer logger.Sync()
	sugar := logger.Sugar().With("node", cfg.Id)

	server, err := service.NewServer(cfg.Id, cfg.URL, cfg.Directory, sugar)

	if err != nil {
		sugar.Infow("failed to create server", "err", err)
		os.Exit(1)
	}

	for k, v := range cfg.Peers {
		server.AddPeer(k, v)
	}

	err = server.StartServer()

	if err != nil {
		sugar.Infow("failed to start server", "err", err)
		os.Exit(1)
	}
}

func setupLogger() *zap.Logger {
	consoleDebugging := zapcore.Lock(os.Stdout)
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	core := zapcore.NewTee(zapcore.NewCore(consoleEncoder, consoleDebugging, zapcore.DebugLevel))
	logger := zap.New(core)

	return logger
}
