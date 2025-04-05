package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Config represents the overall configuration structure
type Config struct {
	Server ServerConfig  `toml:"server"`
	Key    KeyConfig     `toml:"key"`
	Vault  VaultConfig   `toml:"vault"`
	Gpings []GpingConfig `toml:"gpings"`
	Tpings TpingConfig   `toml:"tpings"`
}

// ServerConfig contains server-related configuration
type ServerConfig struct {
	Port       int    `toml:"port"`
	Router     string `toml:"router"`
	SolanaRpc  string `toml:"solana_rpc"`
	IsProposal bool   `toml:"is_proposal"`
}

// KeyConfig contains key-related configuration
type KeyConfig struct {
	KeyPath string `toml:"key_path"`
	Address string `toml:"address"`
}

// VaultConfig contains vault-related configuration
type VaultConfig struct {
	Address  string `toml:"address"`
	FeeRatio string `toml:"fee_ratio"`
}

// GpingConfig contains information about other Gping nodes
type GpingConfig struct {
	URL     string `toml:"url"`
	Address string `toml:"address"`
}

// TpingConfig contains tping-related configuration
type TpingConfig struct {
	Addresses []string `toml:"addresses"`
}

// LoadConfig loads configuration from the specified file path
func LoadConfig(configPath string) (*Config, error) {
	var config Config
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		return nil, fmt.Errorf("error decoding config file: %w", err)
	}

	// Validate the configuration
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// validateConfig performs basic validation on the loaded configuration
func validateConfig(config *Config) error {
	if config.Server.Port <= 0 {
		return fmt.Errorf("server port must be greater than 0")
	}

	if config.Server.Router == "" {
		return fmt.Errorf("router URL is required")
	}

	if config.Server.SolanaRpc == "" {
		return fmt.Errorf("Solana RPC URL is required")
	}

	if config.Key.KeyPath == "" {
		return fmt.Errorf("key path is required")
	}

	// Check if the key file exists
	if _, err := os.Stat(config.Key.KeyPath); os.IsNotExist(err) {
		return fmt.Errorf("key file does not exist: %s", config.Key.KeyPath)
	}

	if config.Key.Address == "" {
		return fmt.Errorf("key address is required")
	}

	if config.Vault.Address == "" {
		return fmt.Errorf("vault address is required")
	}

	if config.Vault.FeeRatio == "" {
		return fmt.Errorf("fee ratio is required")
	}

	if len(config.Tpings.Addresses) == 0 {
		return fmt.Errorf("at least one tping address is required")
	}

	return nil
}
