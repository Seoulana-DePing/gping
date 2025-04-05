package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gagliardetto/solana-go"
)

func main() {
	// Generate a new random account
	account := solana.NewWallet()
	privateKey := account.PrivateKey
	publicKey := account.PublicKey()

	// Display the public key (Solana address)
	fmt.Printf("Generated Solana address: %s\n", publicKey.String())

	// Save the private key to a file
	keyDir := "keys"
	if err := os.MkdirAll(keyDir, 0700); err != nil {
		fmt.Printf("Error creating key directory: %v\n", err)
		os.Exit(1)
	}

	keyPath := filepath.Join(keyDir, "private_key.bin")
	if err := os.WriteFile(keyPath, privateKey, 0600); err != nil {
		fmt.Printf("Error writing private key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Private key saved to: %s\n", keyPath)
	fmt.Println("\nUpdate your config.toml with these values:")
	fmt.Printf("[key]\nkey_path = \"%s\"\naddress = \"%s\"\n", keyPath, publicKey.String())
}
