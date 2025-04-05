package main

import (
	"context"
	"crypto/ed25519"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Seoulana-DePing/gping/internal/config"
	"github.com/Seoulana-DePing/gping/internal/handlers"
	"github.com/Seoulana-DePing/gping/internal/p2p"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.toml", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// Load private key
	privateKey, err := loadPrivateKey(cfg.Key.KeyPath)
	if err != nil {
		log.Fatalf("Error loading private key: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create P2P network
	p2pNetwork := p2p.NewP2PNetwork(cfg, privateKey)

	// Start P2P network
	if err := p2pNetwork.Start(ctx); err != nil {
		log.Fatalf("Error starting P2P network: %v", err)
	}

	// Create and start handler
	handler := handlers.NewHandler(cfg, p2pNetwork)

	// Start the HTTP server
	go func() {
		if err := handler.StartServer(ctx); err != nil {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	log.Printf("Gping server started on port %d", cfg.Server.Port)
	log.Printf(`
  ____        ____  _             
 / ___|      |  _ \(_)_ __   __ _ 
 | |  _ _____| |_) | | '_ \ / _` + "`" + ` |
 | |_| |_____|  __/| | | | | (_| |
  \____|     |_|   |_|_| |_|\____|
                             |___/ 
	`)

	log.Printf("🟣  Solana address: %s", cfg.Key.Address)

	// Wait for termination signal
	waitForTermination(cancel)
}

// LoadPrivateKey loads the private key from the specified file
func loadPrivateKey(keyPath string) (ed25519.PrivateKey, error) {
	// In a real application, the private key should be securely stored
	// For simplicity, we'll just read it from a file
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	// Assuming the key is in raw binary format
	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: expected %d bytes, got %d bytes", ed25519.PrivateKeySize, len(keyBytes))
	}

	return ed25519.PrivateKey(keyBytes), nil
}

// WaitForTermination waits for a termination signal
func waitForTermination(cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	log.Println("Received termination signal. Shutting down...")
	cancel()
}
