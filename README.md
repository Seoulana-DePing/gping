# Gping

Gping is a P2P network node for determining the geographic location of IP addresses using WebSocket-based RPC communication.

## Overview

Gping operates in a P2P network with other Gping nodes and collaborates with Tping clients to determine the geographic location of IP addresses. When a router server requests the location of an IP address, Gping collects data from Tping clients, processes it, and reaches a consensus with other Gping nodes to determine the most accurate location.

## Features

- P2P network communication between Gping nodes using WebSockets
- RPC endpoints for getting IP locations and receiving Tping data
- Consensus mechanism for determining accurate locations
- Integration with Solana blockchain for verification and incentives

## Requirements

- Go 1.24 or later
- Access to Solana network (for blockchain integration)
- Private key for signing messages

## Installation

```bash
git clone https://github.com/Seoulana-DePing/gping.git
cd gping
go build -o gping cmd/gping/main.go
```

## Configuration

Gping uses a TOML configuration file with the following structure:

```toml
[server]
port = 1111 # Gping's port
router = "http://example.com/api" # Router server URL
solana_rpc = "https://api.mainnet-beta.solana.com" # Solana RPC URL

[key]
key_path = "key.bin" # Path to the private key file
address = "ExampleSolanaAddress123456789" # Corresponding Solana address

[vault]
address = "ExampleVaultAddress123456789" # Vault contract address
fee_ratio = "0.05" # 5% fee ratio

# List of other Gping nodes in the P2P network
[[gpings]]
url = "ws://gping1.example.com/ws"
address = "GpingNode1SolanaAddress123456789"

[tpings]
addresses = ["TpingAddress1123456789", "TpingAddress2123456789"]
```

## Usage

1. Create a configuration file (config.toml) with your settings
2. Generate or obtain a private key file
3. Run the Gping server:

```bash
./gping -config=config.toml
```

## API Endpoints

### GET /get_location

Request the location of an IP address.

**Request:**
```json
{
  "ip": "192.168.1.1"
}
```

**Response:**
```json
{
  "location": "Seoul, South Korea",
  "vault": "VaultContractAddress123"
}
```

### POST /guess_location

Send Tping data for an IP address.

**Request:**
```json
{
  "gps": "Seoul, South Korea",
  "time": 100,
  "address": "TpingAddress123"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Data received"
}
```

## Architecture

Gping operates as follows:

1. The router server sends an IP address to a Gping node.
2. Gping creates a new goroutine to process the request and polls the IP.
3. Tping clients send their GPS information and response times for the IP.
4. Gping selects the best location based on the response times.
5. Gping broadcasts the answer to the P2P network and waits for consensus.
6. After reaching consensus, Gping returns the location to the router.

## License

MIT 