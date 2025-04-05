# Gping

<p align="center">
  <img src="gping.png" alt="Gping Logo" width="300">
</p>

Gping is a P2P network node for determining the geographic location of IP addresses using WebSocket-based RPC communication and REST API.

## Overview

Gping operates in a P2P network with other Gping nodes and collaborates with Tping clients to determine the geographic location of IP addresses. When a client requests the location of an IP address, Gping collects data from Tping clients, processes it, and reaches a consensus with other Gping nodes to determine the most accurate location.

## Features

- P2P network communication between Gping nodes using WebSockets
- Dual API support: WebSocket RPC and REST API
- Consensus mechanism for determining accurate locations (2/3 majority voting)
- Integration with Solana blockchain for verification and incentives
- Efficient request handling with background processing

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
port = 1111        # Gping's WebSocket port (REST API uses port+1)
is_proposal = true # Whether this node is a proposal node

[key]
key_path = "key.bin"                           # Path to the private key file
address = "ExampleSolanaAddress123456789"      # Corresponding Solana address

[vault]
address = "ExampleVaultAddress123456789"       # Vault contract address
fee_ratio = "0.05"                             # 5% fee ratio

# List of other Gping nodes in the P2P network
[[gpings]]
url = "ws://gping1.example.com:1111/ws/p2p"
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

### WebSocket RPC API (Port 1111)

#### get_location

Request the location of an IP address.

**Request:**
```json
{
  "id": "request-123",
  "method": "get_location",
  "params": {
    "ip": "8.8.8.8",
    "request_id": "client-req-123"
  }
}
```

**Response:**
```json
{
  "id": "request-123",
  "result": {
    "latitude": "37.422000",
    "longitude": "-122.084000",
    "sp_address": "GpingSolanaAddress123",
    "request_id": "client-req-123"
  }
}
```

#### guess_location

Send Tping data for an IP address.

**Request:**
```json
{
  "id": "request-456",
  "method": "guess_location",
  "params": {
    "gps": "37.422000,-122.084000",
    "time": 100,
    "address": "TpingAddress123"
  }
}
```

**Response:**
```json
{
  "id": "request-456",
  "result": {
    "status": "success",
    "message": "Data received"
  }
}
```

### REST API (Port 1112)

#### POST /api/v1/location

Request the location of an IP address.

**Request:**
```json
{
  "ip": "8.8.8.8",
  "request_id": "client-req-123"
}
```

**Response:**
```json
{
  "ip": "8.8.8.8",
  "latitude": "37.422000",
  "longitude": "-122.084000",
  "status": "success",
  "cached": true,
  "request_id": "client-req-123"
}
```

#### POST /rpc/get-location

Alternative REST endpoint that matches the WebSocket RPC interface.

**Request:**
```json
{
  "ip": "8.8.8.8",
  "request_id": "client-req-123"
}
```

**Response:**
```json
{
  "latitude": "37.422000",
  "longitude": "-122.084000",
  "sp_address": "GpingSolanaAddress123",
  "request_id": "client-req-123"
}
```

#### POST /api/v1/guess

Send Tping data for an IP address.

**Request:**
```json
{
  "gps": "37.422000,-122.084000",
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

#### POST /result

Submit IP geolocation results from Tping clients.

**Request:**
```json
{
  "wallet_address": "TpingAddress123",
  "response_time": 150.5,
  "latitude": 37.422000,
  "longitude": -122.084000,
  "ip": "8.8.8.8"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Result received"
}
```

#### GET /polling

Poll for IP addresses that need geolocation.

**Response:**
```json
{
  "ip": "8.8.8.8"
}
```

## WebSocket Client Example

The repository includes a sample WebSocket client in the `examples` directory that demonstrates how to:
- Connect to a Gping node using WebSockets
- Request location information
- Submit Tping data
- Poll for IP addresses that need geolocation

You can run it with:

```bash
cd examples
go run websocket_client.go
```

To use the REST API interface instead:

```bash
go run websocket_client.go rest
```

## Architecture

Gping operates as follows:

1. The client sends an IP address to a Gping node through WebSocket RPC or REST API.
2. Gping creates a new goroutine to process the request.
3. If the Gping node is a proposal node, it:
   - Polls for data from Tping clients
   - Selects the best location based on response times
   - Broadcasts the answer to the P2P network 
   - Waits for consensus (2/3 of nodes must agree)
4. If the Gping node is not a proposal node:
   - It waits for proposals from other nodes
   - Validates and signs valid proposals
5. Once consensus is reached, the location is stored and returned to the client.

## Consensus Mechanism

Gping uses a consensus mechanism to ensure accuracy:
- A proposal node broadcasts a location answer to all other nodes
- Each receiving node checks if it agrees with the answer
- If a node agrees, it signs the message and broadcasts its signature
- Consensus is achieved when 2/3 of all nodes have signed the message
- If consensus can't be reached within 30 seconds, the request times out

## License

MIT

---

# Gping (한국어)

<p align="center">
  <img src="gping.png" alt="Gping 로고" width="300">
</p>

Gping은 WebSocket 기반 RPC 통신과 REST API를 사용하여 IP 주소의 지리적 위치를 결정하는 P2P 네트워크 노드입니다.

## 개요

Gping은 다른 Gping 노드들과 P2P 네트워크로 운영되며, Tping 클라이언트와 협력하여 IP 주소의 지리적 위치를 결정합니다. 클라이언트가 IP 주소의 위치를 요청하면, Gping은 Tping 클라이언트로부터 데이터를 수집하고 처리한 후, 다른 Gping 노드들과 합의를 통해 가장 정확한 위치를 결정합니다.

## 특징

- WebSocket을 사용한 Gping 노드 간 P2P 네트워크 통신
- 이중 API 지원: WebSocket RPC와 REST API
- 정확한 위치 결정을 위한 합의 메커니즘 (2/3 다수결 투표)
- 검증 및 인센티브를 위한 Solana 블록체인 연동
- 백그라운드 처리를 통한 효율적인 요청 처리

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
port = 1111        # Gping의 WebSocket 포트 (REST API는 port+1 사용)
is_proposal = true # 이 노드가 제안 노드인지 여부

[key]
key_path = "key.bin"                           # 개인 키 파일 경로
address = "ExampleSolanaAddress123456789"      # 해당 Solana 주소

[vault]
address = "ExampleVaultAddress123456789"       # 금고 컨트랙트 주소
fee_ratio = "0.05"                             # 5% 수수료 비율

# P2P 네트워크의 다른 Gping 노드 목록
[[gpings]]
url = "ws://gping1.example.com:1111/ws/p2p"
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

### WebSocket RPC API (포트 1111)

#### get_location

IP 주소의 위치를 요청합니다.

**요청:**
```json
{
  "id": "request-123",
  "method": "get_location",
  "params": {
    "ip": "8.8.8.8",
    "request_id": "client-req-123"
  }
}
```

**응답:**
```json
{
  "id": "request-123",
  "result": {
    "latitude": "37.422000",
    "longitude": "-122.084000",
    "sp_address": "GpingSolanaAddress123",
    "request_id": "client-req-123"
  }
}
```

#### guess_location

IP 주소에 대한 Tping 데이터를 전송합니다.

**요청:**
```json
{
  "id": "request-456",
  "method": "guess_location",
  "params": {
    "gps": "37.422000,-122.084000",
    "time": 100,
    "address": "TpingAddress123"
  }
}
```

**응답:**
```json
{
  "id": "request-456",
  "result": {
    "status": "success",
    "message": "Data received"
  }
}
```

### REST API (포트 1112)

#### POST /api/v1/location

IP 주소의 위치를 요청합니다.

**요청:**
```json
{
  "ip": "8.8.8.8",
  "request_id": "client-req-123"
}
```

**응답:**
```json
{
  "ip": "8.8.8.8",
  "latitude": "37.422000",
  "longitude": "-122.084000",
  "status": "success",
  "cached": true,
  "request_id": "client-req-123"
}
```

#### POST /rpc/get-location

WebSocket RPC 인터페이스와 일치하는 대체 REST 엔드포인트입니다.

**요청:**
```json
{
  "ip": "8.8.8.8",
  "request_id": "client-req-123"
}
```

**응답:**
```json
{
  "latitude": "37.422000",
  "longitude": "-122.084000",
  "sp_address": "GpingSolanaAddress123",
  "request_id": "client-req-123"
}
```

#### POST /api/v1/guess

IP 주소에 대한 Tping 데이터를 전송합니다.

**요청:**
```json
{
  "gps": "37.422000,-122.084000",
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

#### POST /result

Tping 클라이언트로부터 IP 지리 위치 결과를 제출합니다.

**요청:**
```json
{
  "wallet_address": "TpingAddress123",
  "response_time": 150.5,
  "latitude": 37.422000,
  "longitude": -122.084000,
  "ip": "8.8.8.8"
}
```

**응답:**
```json
{
  "status": "success",
  "message": "Result received"
}
```

#### GET /polling

지리 위치가 필요한 IP 주소를 폴링합니다.

**응답:**
```json
{
  "ip": "8.8.8.8"
}
```

## WebSocket 클라이언트 예제

이 저장소는 `examples` 디렉토리에 다음 기능을 보여주는 샘플 WebSocket 클라이언트를 포함하고 있습니다:
- WebSocket을 사용하여 Gping 노드에 연결
- 위치 정보 요청
- Tping 데이터 제출
- 지리 위치가 필요한 IP 주소 폴링

다음과 같이 실행할 수 있습니다:

```bash
cd examples
go run websocket_client.go
```

REST API 인터페이스를 대신 사용하려면:

```bash
go run websocket_client.go rest
```

## 아키텍처

Gping은 다음과 같이 작동합니다:

1. 클라이언트는 WebSocket RPC 또는 REST API를 통해 Gping 노드에 IP 주소를 전송합니다.
2. Gping은 요청을 처리하기 위한 새 고루틴을 생성합니다.
3. Gping 노드가 제안 노드인 경우:
   - Tping 클라이언트로부터 데이터를 폴링합니다
   - 응답 시간을 기준으로 최적의 위치를 선택합니다
   - P2P 네트워크에 답변을 브로드캐스트합니다
   - 합의를 기다립니다 (노드의 2/3가 동의해야 함)
4. Gping 노드가 제안 노드가 아닌 경우:
   - 다른 노드로부터의 제안을 기다립니다
   - 유효한 제안을 검증하고 서명합니다
5. 합의에 도달하면, 위치가 저장되고 클라이언트에게 반환됩니다.

## 합의 메커니즘

Gping은 정확성을 보장하기 위해 합의 메커니즘을 사용합니다:
- 제안 노드는 모든 다른 노드에 위치 답변을 브로드캐스트합니다
- 각 수신 노드는 답변에 동의하는지 확인합니다
- 노드가 동의하면, 메시지에 서명하고 서명을 브로드캐스트합니다
- 모든 노드의 2/3가 메시지에 서명하면 합의가 이루어집니다
- 30초 내에 합의에 도달할 수 없으면 요청은 타임아웃됩니다

## 라이센스

MIT 