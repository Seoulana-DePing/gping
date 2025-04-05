# Gping

<p align="center">
  <img src="gping.png" alt="Gping Logo" width="300">
</p>

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

---

# Gping (한국어)

<p align="center">
  <img src="gping.png" alt="Gping 로고" width="300">
</p>

Gping은 WebSocket 기반 RPC 통신을 사용하여 IP 주소의 지리적 위치를 결정하는 P2P 네트워크 노드입니다.

## 개요

Gping은 다른 Gping 노드들과 P2P 네트워크로 운영되며, Tping 클라이언트와 협력하여 IP 주소의 지리적 위치를 결정합니다. 라우터 서버가 IP 주소의 위치를 요청하면, Gping은 Tping 클라이언트로부터 데이터를 수집하고 처리한 후, 다른 Gping 노드들과 합의를 통해 가장 정확한 위치를 결정합니다.

## 특징

- WebSocket을 사용한 Gping 노드 간 P2P 네트워크 통신
- IP 위치 조회 및 Tping 데이터 수신을 위한 RPC 엔드포인트
- 정확한 위치 결정을 위한 합의 메커니즘
- 검증 및 인센티브를 위한 Solana 블록체인 연동

## 요구사항

- Go 1.24 이상
- Solana 네트워크 접근 (블록체인 연동용)
- 메시지 서명용 개인 키

## 설치

```bash
git clone https://github.com/Seoulana-DePing/gping.git
cd gping
go build -o gping cmd/gping/main.go
```

## 구성

Gping은 다음과 같은 구조의 TOML 구성 파일을 사용합니다:

```toml
[server]
port = 1111 # Gping의 포트
router = "http://example.com/api" # 라우터 서버 URL
solana_rpc = "https://api.mainnet-beta.solana.com" # Solana RPC URL

[key]
key_path = "key.bin" # 개인 키 파일 경로
address = "ExampleSolanaAddress123456789" # 해당 Solana 주소

[vault]
address = "ExampleVaultAddress123456789" # 금고 컨트랙트 주소
fee_ratio = "0.05" # 5% 수수료 비율

# P2P 네트워크의 다른 Gping 노드 목록
[[gpings]]
url = "ws://gping1.example.com/ws"
address = "GpingNode1SolanaAddress123456789"

[tpings]
addresses = ["TpingAddress1123456789", "TpingAddress2123456789"]
```

## 사용법

1. 설정으로 구성 파일(config.toml)을 생성합니다
2. 개인 키 파일을 생성하거나 획득합니다
3. Gping 서버를 실행합니다:

```bash
./gping -config=config.toml
```

## API 엔드포인트

### GET /get_location

IP 주소의 위치를 요청합니다.

**요청:**
```json
{
  "ip": "192.168.1.1"
}
```

**응답:**
```json
{
  "location": "서울, 대한민국",
  "vault": "VaultContractAddress123"
}
```

### POST /guess_location

IP 주소에 대한 Tping 데이터를 전송합니다.

**요청:**
```json
{
  "gps": "서울, 대한민국",
  "time": 100,
  "address": "TpingAddress123"
}
```

**응답:**
```json
{
  "status": "success",
  "message": "Data received"
}
```

## 아키텍처

Gping은 다음과 같이 작동합니다:

1. 라우터 서버가 Gping 노드에 IP 주소를 전송합니다.
2. Gping은 요청을 처리하기 위한 새 고루틴을 생성하고 IP를 폴링합니다.
3. Tping 클라이언트는 IP에 대한 GPS 정보와 응답 시간을 전송합니다.
4. Gping은 응답 시간을 기준으로 최적의 위치를 선택합니다.
5. Gping은 P2P 네트워크에 답변을 브로드캐스트하고 합의를 기다립니다.
6. 합의에 도달한 후, Gping은 라우터에 위치를 반환합니다.

## 라이센스

MIT 