package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Seoulana-DePing/gping/internal/config"
	"github.com/Seoulana-DePing/gping/internal/models"
	"github.com/gorilla/websocket"
)

// P2PNetwork represents the P2P network of Gping nodes
type P2PNetwork struct {
	config           *config.Config
	privateKey       ed25519.PrivateKey
	connections      map[string]*websocket.Conn
	connectionsMu    sync.RWMutex
	connWriteMutexes map[string]*sync.Mutex // Mutex per connection for write operations
	messageChan      chan models.SignedMessage
	broadcastChan    chan models.SignedMessage
	signaturesMu     sync.RWMutex
	signatures       map[string][]string // Maps message to list of signatures
	pendingResults   map[string]chan bool
	pendingResultsMu sync.RWMutex
}

// NewP2PNetwork creates a new P2P network
func NewP2PNetwork(cfg *config.Config, privateKey ed25519.PrivateKey) *P2PNetwork {
	return &P2PNetwork{
		config:           cfg,
		privateKey:       privateKey,
		connections:      make(map[string]*websocket.Conn),
		connWriteMutexes: make(map[string]*sync.Mutex),
		messageChan:      make(chan models.SignedMessage, 100),
		broadcastChan:    make(chan models.SignedMessage, 100),
		signatures:       make(map[string][]string),
		pendingResults:   make(map[string]chan bool),
	}
}

// Start starts the P2P network
func (p *P2PNetwork) Start(ctx context.Context) error {
	// Connect to other Gping nodes
	for _, gping := range p.config.Gpings {
		go p.connectWithRetry(ctx, gping)
	}

	// Start message handler
	go p.handleMessages(ctx)

	// Start broadcast handler
	go p.handleBroadcasts(ctx)

	return nil
}

// Connect to another Gping node with retry
func (p *P2PNetwork) connectWithRetry(ctx context.Context, gping config.GpingConfig) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.isConnected(gping.Address) {
				// Check if all nodes are connected
				p.checkAllNodesConnected()
				return
			}

			if err := p.connectToGping(gping); err == nil {
				log.Printf("✅   Success connection %s", gping.URL)
				// Check if all nodes are connected
				p.checkAllNodesConnected()
				return
			}
		}
	}
}

// Check if a Gping node is already connected
func (p *P2PNetwork) isConnected(address string) bool {
	p.connectionsMu.RLock()
	defer p.connectionsMu.RUnlock()
	_, exists := p.connections[address]
	return exists
}

// Check if all Gping nodes are connected
func (p *P2PNetwork) checkAllNodesConnected() {
	p.connectionsMu.RLock()

	// 구성 파일의 모든 Gping 노드 주소를 맵에 저장
	configuredNodes := make(map[string]bool)
	for _, gping := range p.config.Gpings {
		configuredNodes[gping.Address] = true
	}

	// 실제 연결된 노드들이 구성 파일에 있는 노드인지 확인
	allNodesConnected := true
	connectedConfigNodes := 0

	// 연결된 모든 노드를 확인
	for addr := range p.connections {
		// 임시 연결은 건너뜀 (temp-로 시작하는 연결)
		if strings.HasPrefix(addr, "temp-") {
			continue
		}

		// 이 주소가 구성 파일에 있는 노드인지 확인
		if configuredNodes[addr] {
			connectedConfigNodes++
		}
	}

	// 모든 구성된 노드에 연결되었는지 확인
	allNodesConnected = connectedConfigNodes == len(p.config.Gpings)

	totalGpings := len(p.config.Gpings)
	p.connectionsMu.RUnlock()

	if allNodesConnected {
		log.Printf("🎉 All the GPings are connected (%d/%d nodes).", connectedConfigNodes, totalGpings)

		// If this node is a proposal, wait a moment for all connections to stabilize then broadcast
		if p.config.Server.IsProposal {
			log.Printf("🔄 This node is configured as the proposal node. Preparing to broadcast proposal notification...")

			// Add a short delay to ensure all connections are fully established
			go func() {
				time.Sleep(1 * time.Second)
				p.broadcastProposalInfo()
			}()
		} else {
			log.Printf("ℹ️ This node is not the proposal. Waiting for proposal notification...")
		}
	} else {
		log.Printf("⏳ Waiting for connections... (%d/%d Gpings connected)", connectedConfigNodes, totalGpings)
	}
}

// Connect to another Gping node
func (p *P2PNetwork) connectToGping(gping config.GpingConfig) error {
	log.Printf("🔍 DEBUG: Connecting to Gping node at URL: %s (Address: %s)", gping.URL, gping.Address)

	dialer := websocket.Dialer{}
	conn, resp, err := dialer.Dial(gping.URL, nil)
	if err != nil {
		if resp != nil {
			log.Printf("🔍 DEBUG: Response status: %s, Headers: %v", resp.Status, resp.Header)
		}
		log.Printf("Failed to connect to Gping node at %s: %v", gping.URL, err)
		return err
	}

	log.Printf("🔍 DEBUG: Successfully connected to %s", gping.URL)

	p.connectionsMu.Lock()
	p.connections[gping.Address] = conn
	p.connWriteMutexes[gping.Address] = &sync.Mutex{} // Create a mutex for this connection
	p.connectionsMu.Unlock()

	go p.receiveMessages(conn, gping.Address)

	return nil
}

// Receive messages from a connected Gping node
func (p *P2PNetwork) receiveMessages(conn *websocket.Conn, address string) {
	defer func() {
		log.Printf("🔌 Closing connection with node: %s", address)
		err := conn.Close()
		if err != nil {
			log.Printf("⚠️ Error closing WebSocket connection with %s: %v", address, err)
		}

		p.connectionsMu.Lock()
		delete(p.connections, address)
		delete(p.connWriteMutexes, address) // Remove the mutex when connection is closed
		p.connectionsMu.Unlock()

		log.Printf("❌ Connection removed from P2P network: %s", address)
	}()

	log.Printf("🔄 Started receiving messages from node: %s", address)

	// Setup ping/pong handlers to keep the connection alive
	conn.SetPingHandler(func(data string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
	})

	conn.SetPongHandler(func(data string) error {
		return nil
	})

	// Start a ticker to send pings periodically
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		for range pingTicker.C {
			p.connectionsMu.RLock()
			_, exists := p.connections[address]
			p.connectionsMu.RUnlock()

			if !exists {
				return // Connection no longer exists, stop the ticker
			}

			p.connectionsMu.RLock()
			mutex, exists := p.connWriteMutexes[address]
			p.connectionsMu.RUnlock()

			if !exists {
				return // Connection was removed
			}

			mutex.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(10*time.Second))
			mutex.Unlock()

			if err != nil {
				log.Printf("⚠️ Error sending ping to %s: %v", address, err)
				// Don't close the connection here, let the read loop detect the error
			}
		}
	}()

	for {
		var message models.SignedMessage
		if err := conn.ReadJSON(&message); err != nil {
			log.Printf("Error reading message from %s: %v", address, err)
			return
		}

		log.Printf("📩  INFO: Received message from %s", address)

		// Check if this is a proposal info message first
		var proposalInfo models.ProposalInfo
		if err := json.Unmarshal(message.Message, &proposalInfo); err == nil && proposalInfo.IsProposal {
			// Handle the proposal info immediately
			p.handleProposalInfoBroadcast(proposalInfo)
			continue
		}

		// For other message types
		var locationAnswer [2]string
		if json.Unmarshal(message.Message, &locationAnswer) == nil {
			log.Printf("📌 Received location answer for IP: %s", locationAnswer[0])
		}

		// Validate the signature
		if !p.validateSignature(message, address) {
			log.Printf("Invalid signature from %s", address)
			continue
		}

		// Send to message channel for processing
		p.messageChan <- message
	}
}

// ValidateSignature validates a signature from a Gping node
func (p *P2PNetwork) validateSignature(message models.SignedMessage, address string) bool {
	// TODO: Implement signature validation with Solana public key
	// For now, just return true for simplicity
	return true
}

// HandleMessages processes incoming messages
func (p *P2PNetwork) handleMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-p.messageChan:
			p.processMessage(message)
		}
	}
}

// ProcessMessage processes an incoming message
func (p *P2PNetwork) processMessage(message models.SignedMessage) {
	// First try to unmarshal as a location answer
	var locationAnswer [2]string
	if err := json.Unmarshal(message.Message, &locationAnswer); err == nil {
		// This is a location answer broadcast
		p.handleLocationAnswerBroadcast(message, locationAnswer)
		return
	}

	// Try to unmarshal as a proposal info
	var proposalInfo models.ProposalInfo
	if err := json.Unmarshal(message.Message, &proposalInfo); err == nil && proposalInfo.IsProposal {
		// This is a proposal info broadcast
		p.handleProposalInfoBroadcast(proposalInfo)
		return
	}

	// This might be a signature response to our broadcast
	p.handleSignatureResponse(message)
}

// HandleLocationAnswerBroadcast handles a broadcast of a location answer
func (p *P2PNetwork) handleLocationAnswerBroadcast(message models.SignedMessage, locationAnswer [2]string) {
	ip := locationAnswer[0]
	location := locationAnswer[1]

	log.Printf("📥 위치 응답 브로드캐스트 수신: IP=%s, Location=%s", ip, location)

	// Check if we've already processed this exact message
	messageStr := string(message.Message)

	p.signaturesMu.RLock()
	signatures, alreadyProcessed := p.signatures[messageStr]
	p.signaturesMu.RUnlock()

	// 받은 메시지에 대한 서명 생성
	ourSignature := p.signMessage(message.Message)

	if alreadyProcessed {
		// Check if our signature is already in the list
		isAlreadySigned := false
		for _, sig := range signatures {
			if sig == ourSignature {
				isAlreadySigned = true
				break
			}
		}

		if isAlreadySigned {
			log.Printf("✅ 이미 이 메시지에 대한 서명이 완료됨 IP=%s", ip)
			return
		} else {
			log.Printf("🔍 이 메시지를 이전에 받았으나 아직 서명되지 않음 IP=%s", ip)
		}
	} else {
		log.Printf("🆕 새로운 위치 응답 메시지 수신 IP=%s", ip)
	}

	// If tpings array is empty, we automatically agree with any answer
	if len(p.config.Tpings.Addresses) == 0 {
		log.Printf("ℹ️ Tping 없음 - 자동 승인 모드")

		// We agree with this answer, sign the message and send back
		signature := p.signMessage(message.Message)
		response := models.SignedMessage{
			Message:   message.Message,
			Signature: signature,
		}

		// Initialize the signatures map if not already done
		p.signaturesMu.Lock()
		if _, exists := p.signatures[messageStr]; !exists {
			p.signatures[messageStr] = []string{}
			log.Printf("🗂️ 새 서명 배열 초기화")
		}

		// 서명이 이미 포함되어 있는지 확인
		alreadyIncluded := false
		for _, sig := range p.signatures[messageStr] {
			if sig == signature {
				alreadyIncluded = true
				break
			}
		}

		// Add our signature to the list to prevent duplicates
		if !alreadyIncluded {
			p.signatures[messageStr] = append(p.signatures[messageStr], signature)
			log.Printf("✍️ 서명 추가됨 (현재 서명 개수: %d)", len(p.signatures[messageStr]))
		} else {
			log.Printf("⚠️ 서명이 이미 포함되어 있음 (현재 서명 개수: %d)", len(p.signatures[messageStr]))
		}
		p.signaturesMu.Unlock()

		// Send the response to all nodes
		log.Printf("📤 승인 응답 브로드캐스트 시작")
		p.broadcastChan <- response
		return
	}

	// Otherwise, check if we have a matching answer
	storedLocation, exists := models.GetAnswer(ip)
	if exists {
		log.Printf("✅ 저장된 위치와 일치하는 응답 IP=%s", ip)

		// We agree with this answer, sign the message and send back
		signature := p.signMessage(message.Message)
		response := models.SignedMessage{
			Message:   message.Message,
			Signature: signature,
		}

		// Initialize the signatures map if not already done
		p.signaturesMu.Lock()
		if _, exists := p.signatures[messageStr]; !exists {
			p.signatures[messageStr] = []string{}
			log.Printf("🗂️ 새 서명 배열 초기화")
		}

		// 서명이 이미 포함되어 있는지 확인
		alreadyIncluded := false
		for _, sig := range p.signatures[messageStr] {
			if sig == signature {
				alreadyIncluded = true
				break
			}
		}

		// Add our signature to the list to prevent duplicates
		if !alreadyIncluded {
			p.signatures[messageStr] = append(p.signatures[messageStr], signature)
			log.Printf("✍️ 서명 추가됨 (현재 서명 개수: %d)", len(p.signatures[messageStr]))
		} else {
			log.Printf("⚠️ 서명이 이미 포함되어 있음 (현재 서명 개수: %d)", len(p.signatures[messageStr]))
		}
		p.signaturesMu.Unlock()

		// Find the originator by signature (would be better with a message ID in practice)
		// For simplicity, we'll broadcast the response to all nodes
		log.Printf("📤 승인 응답 브로드캐스트 시작")
		p.broadcastChan <- response
	} else {
		if !exists {
			log.Printf("⚠️ IP=%s에 대한 저장된 위치 정보 없음", ip)
		} else {
			log.Printf("❌ 위치 불일치: 저장됨=%s, 수신됨=%s", storedLocation, location)
		}
	}
}

// HandleSignatureResponse handles a signature response to our broadcast
func (p *P2PNetwork) handleSignatureResponse(message models.SignedMessage) {
	messageStr := string(message.Message)

	// 메시지 내용을 해석해 디버그 정보 출력
	var locationAnswer [2]string
	if err := json.Unmarshal(message.Message, &locationAnswer); err == nil {
		log.Printf("📨 서명 응답 수신: IP=%s, Location=%s", locationAnswer[0], locationAnswer[1])
	} else {
		log.Printf("📨 서명 응답 수신: 메시지 형식 확인 불가")
	}

	// 먼저 현재 서명 수 확인
	p.signaturesMu.RLock()
	currentSignatures, exists := p.signatures[messageStr]
	p.signaturesMu.RUnlock()

	if !exists {
		// 이 메시지에 대한 서명 맵이 아직 초기화되지 않았음
		log.Printf("🆕 새로운 메시지에 대한 서명 응답, 서명 배열 초기화")
		p.signaturesMu.Lock()
		p.signatures[messageStr] = []string{message.Signature}
		p.signaturesMu.Unlock()

		// 이 시점에서는 컨센서스에 필요한 서명이 부족하므로 리턴
		log.Printf("⏳ 더 많은 서명 대기 중... (현재: 1개)")
		return
	}

	// 이미 이 서명이 추가되었는지 확인
	for _, sig := range currentSignatures {
		if sig == message.Signature {
			// 이미 이 서명은 처리됨
			log.Printf("🔄 중복된 서명 감지됨, 무시합니다")
			return
		}
	}

	// 서명 추가
	p.signaturesMu.Lock()
	p.signatures[messageStr] = append(p.signatures[messageStr], message.Signature)
	currentSignatureCount := len(p.signatures[messageStr])

	// 서명의 일부만 출력 (디버깅 목적)
	shortSig := ""
	if len(message.Signature) > 10 {
		shortSig = message.Signature[:10] + "..."
	} else {
		shortSig = message.Signature
	}
	log.Printf("➕ 새 서명 추가됨: %s (총 %d개)", shortSig, currentSignatureCount)

	// 서명한 모든 노드 목록 출력
	log.Printf("📋 현재 서명 목록:")
	for i, sig := range p.signatures[messageStr] {
		if len(sig) > 10 {
			log.Printf("  %d. %s...", i+1, sig[:10])
		} else {
			log.Printf("  %d. %s", i+1, sig)
		}
	}

	// 필요한 서명 수 계산 (2/3 이상)
	requiredSignatures := (len(p.config.Gpings) * 2) / 3
	hasEnoughSignatures := currentSignatureCount >= requiredSignatures
	p.signaturesMu.Unlock()

	// 충분한 서명을 받았으면 컨센서스 달성으로 간주
	if hasEnoughSignatures {
		log.Printf("🎉 컨센서스 달성! 서명 %d개 받음 (필요: %d개)",
			currentSignatureCount, requiredSignatures)

		// resultChan으로 결과 전달
		p.pendingResultsMu.Lock()
		if resultChan, exists := p.pendingResults[messageStr]; exists {
			// 채널이 이미 닫히지 않았는지 확인 후 전송 시도
			select {
			case resultChan <- true:
				log.Printf("✅ 컨센서스 결과 전송 성공")
			default:
				log.Printf("⚠️ 결과 채널이 이미 닫혔거나 가득 참")
			}
			delete(p.pendingResults, messageStr)
		} else {
			// 해당 메시지에 대한 대기 중인 결과 채널이 없음 - 이미 처리되었거나 다른 노드가 발신자
			log.Printf("ℹ️ 이 메시지에 대한 대기 중인 결과 채널 없음 (현재 pendingResults 크기: %d)",
				len(p.pendingResults))
		}
		p.pendingResultsMu.Unlock()
	} else {
		log.Printf("⏳ 서명 수집 중: %d/%d개 (필요 서명까지 %d개 더 필요)",
			currentSignatureCount, requiredSignatures, requiredSignatures-currentSignatureCount)
	}
}

// HandleBroadcasts handles outgoing broadcasts
func (p *P2PNetwork) handleBroadcasts(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-p.broadcastChan:
			p.broadcastMessage(message)
		}
	}
}

// BroadcastMessage sends a message to all connected Gping nodes
func (p *P2PNetwork) broadcastMessage(message models.SignedMessage) {
	// 브로드캐스트 되는 메시지 내용 확인
	var locationAnswer [2]string
	if err := json.Unmarshal(message.Message, &locationAnswer); err == nil {
		log.Printf("📤 브로드캐스트 시작: IP=%s, Location=%s", locationAnswer[0], locationAnswer[1])
	} else {
		log.Printf("📤 브로드캐스트 시작: 메시지 형식 확인 불가")
	}

	p.connectionsMu.RLock()
	addresses := make([]string, 0, len(p.connections))
	conns := make([]*websocket.Conn, 0, len(p.connections))

	// First, collect all addresses and connections
	for addr, conn := range p.connections {
		addresses = append(addresses, addr)
		conns = append(conns, conn)
	}

	nodeCount := len(addresses)
	log.Printf("📡 브로드캐스트 대상: %d개 노드", nodeCount)
	p.connectionsMu.RUnlock()

	if nodeCount == 0 {
		log.Printf("⚠️ 연결된 노드가 없어 브로드캐스트 불가")
		return
	}

	// 메시지 전송 결과를 추적하기 위한 채널
	resultChan := make(chan bool, nodeCount)

	// Then, send to each connection with its own mutex
	for i, addr := range addresses {
		go func(address string, c *websocket.Conn, idx int) {
			// Get the write mutex for this connection
			p.connectionsMu.RLock()
			mutex, exists := p.connWriteMutexes[address]
			p.connectionsMu.RUnlock()

			if !exists {
				log.Printf("❌ 노드 %s에 대한 연결이 이미 종료됨", address)
				resultChan <- false
				return
			}

			// Lock the mutex for this specific connection
			mutex.Lock()
			defer mutex.Unlock()

			if err := c.WriteJSON(message); err != nil {
				log.Printf("❌ 노드 %s에 메시지 전송 실패: %v", address, err)
				resultChan <- false
			} else {
				log.Printf("✅ 노드 %s에 메시지 전송 성공", address)
				resultChan <- true
			}
		}(addr, conns[i], i)
	}

	// 모든 goroutine의 결과 수집 (최대 1초 대기)
	go func() {
		sentCount := 0
		failCount := 0
		timeout := time.After(1 * time.Second)

		for i := 0; i < nodeCount; i++ {
			select {
			case success := <-resultChan:
				if success {
					sentCount++
				} else {
					failCount++
				}
			case <-timeout:
				// 시간 초과
				log.Printf("⚠️ 일부 브로드캐스트 결과 수집 시간 초과")
				i = nodeCount // 루프 종료
			}
		}

		log.Printf("📊 브로드캐스트 결과: 성공=%d, 실패=%d, 총=%d", sentCount, failCount, nodeCount)
	}()
}

// BroadcastAnswer broadcasts a location answer and waits for consensus
func (p *P2PNetwork) BroadcastAnswer(ip, location string) (bool, error) {
	// 위치 정보는 "위도,경도" 형식으로 저장되었다고 가정
	// Create the message array [ip, location]
	message := [2]string{ip, location}

	// Marshal the message
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return false, fmt.Errorf("error marshaling message: %w", err)
	}

	// Check if we've already broadcast this message
	messageStr := string(messageBytes)
	p.signaturesMu.RLock()
	signatures, alreadyBroadcast := p.signatures[messageStr]
	p.signaturesMu.RUnlock()

	if alreadyBroadcast {
		log.Printf("📍 Already broadcast location for IP %s, not sending again", ip)
		log.Printf("🔄 현재 서명 개수: %d", len(signatures))

		// 이미 충분한 서명을 가지고 있는지 확인
		p.signaturesMu.RLock()
		requiredSignatures := (len(p.config.Gpings) * 2) / 3
		hasEnoughSignatures := len(signatures) >= requiredSignatures
		p.signaturesMu.RUnlock()

		if hasEnoughSignatures {
			log.Printf("✅ Already have consensus for IP %s with %d signatures (required: %d)",
				ip, len(signatures), requiredSignatures)
			return true, nil
		}
	}

	// Sign the message
	signature := p.signMessage(messageBytes)

	// Create the signed message
	signedMessage := models.SignedMessage{
		Message:   messageBytes,
		Signature: signature,
	}

	// Create a channel to wait for consensus
	resultChan := make(chan bool, 1)

	// Register the pending result
	p.pendingResultsMu.Lock()
	p.pendingResults[messageStr] = resultChan
	p.pendingResultsMu.Unlock()

	// Initialize the signatures map for this message
	p.signaturesMu.Lock()

	// 서명 배열 초기화 - 이미 있는 경우 덮어쓰지 말고 확인
	if _, exists := p.signatures[messageStr]; !exists {
		// 배열이 없는 경우에만 새로 초기화
		p.signatures[messageStr] = []string{signature} // Include our own signature
		log.Printf("🔐 새로운 서명 배열 생성: 현재 1개")
	} else if !contains(p.signatures[messageStr], signature) {
		// 배열이 있고 내 서명이 없는 경우에만 추가
		p.signatures[messageStr] = append(p.signatures[messageStr], signature)
		log.Printf("🔐 기존 서명 배열에 내 서명 추가: 현재 %d개", len(p.signatures[messageStr]))
	} else {
		log.Printf("🔐 이미 내 서명이 포함됨: 현재 %d개", len(p.signatures[messageStr]))
	}

	// 서명이 이미 충분한지 확인 (이 경우는 1개 노드만 있을 때)
	requiredSignatures := (len(p.config.Gpings) * 2) / 3
	log.Printf("📊 필요한 서명 개수: %d, 현재 서명 개수: %d", requiredSignatures, len(p.signatures[messageStr]))

	if requiredSignatures <= 1 {
		// 자신의 서명만으로 충분한 경우 (1개 노드 구성)
		log.Printf("✅ Single node network, consensus achieved with own signature for IP %s", ip)
		resultChan <- true
		p.signaturesMu.Unlock()
		return true, nil
	}

	// 다른 노드 서명 개수 출력
	for i, sig := range p.signatures[messageStr] {
		// 서명의 일부만 출력 (디버깅 목적)
		shortSig := ""
		if len(sig) > 10 {
			shortSig = sig[:10] + "..."
		} else {
			shortSig = sig
		}
		log.Printf("📝 서명[%d]: %s", i, shortSig)
	}

	p.signaturesMu.Unlock()

	// Check if we already have enough signatures (this can happen if we received signatures
	// while preparing this broadcast)
	p.signaturesMu.RLock()
	signatures = p.signatures[messageStr] // Get the latest signatures
	signatureCount := len(signatures)
	requiredSigs := requiredSignatures // 로컬 변수로 복사
	p.signaturesMu.RUnlock()

	log.Printf("🔍 브로드캐스트 전 최종 서명 확인: %d/%d개", signatureCount, requiredSigs)

	if signatureCount >= requiredSigs {
		log.Printf("✅ 브로드캐스트 전 이미 충분한 서명 확보 (%d/%d) - IP=%s",
			signatureCount, requiredSigs, ip)

		// Clean up the pending result
		p.pendingResultsMu.Lock()
		delete(p.pendingResults, messageStr)
		p.pendingResultsMu.Unlock()

		return true, nil
	}

	// 모든 gping 노드 목록 출력 (디버깅용)
	log.Printf("📋 현재 gping 노드 목록 (총 %d개):", len(p.config.Gpings))
	for i, gping := range p.config.Gpings {
		log.Printf("  %d. %s (%s)", i+1, gping.Address, gping.URL)
	}

	// Broadcast the message
	log.Printf("📣 위치 응답 브로드캐스트 시작 - IP=%s, Location=%s", ip, location)
	p.broadcastChan <- signedMessage

	select {
	case result := <-resultChan:
		log.Printf("🏁 Finished waiting for consensus: result=%v", result)
		return result, nil
	case <-time.After(3 * time.Second):
		log.Printf("🏁 Finished waiting for consensus")
		return true, nil
	case <-time.After(30 * time.Second): // 명시적인 타임아웃 추가
		// Remove the pending result
		p.pendingResultsMu.Lock()
		delete(p.pendingResults, messageStr)
		p.pendingResultsMu.Unlock()

		log.Printf("⚠️ 컨센서스 타임아웃 발생 (30초)")

		// 현재 서명 상태 확인
		p.signaturesMu.RLock()
		currentSigs := p.signatures[messageStr]
		sigCount := len(currentSigs)
		p.signaturesMu.RUnlock()

		log.Printf("📊 타임아웃 시점 서명 상태: %d/%d개", sigCount, requiredSigs)

		return false, fmt.Errorf("timeout waiting for consensus")
	}
}

// contains checks if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// SignMessage signs a message with the private key
func (p *P2PNetwork) signMessage(message []byte) string {
	signature := ed25519.Sign(p.privateKey, message)
	return base64.StdEncoding.EncodeToString(signature)
}

// SendTpingData sends data to a Gping node
func SendTpingData(url string, data models.TpingData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling tping data: %w", err)
	}

	resp, err := http.Post(url+"/guess_location", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error sending tping data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error response from gping: %s", resp.Status)
	}

	return nil
}

// BroadcastProposalInfo broadcasts this node's proposal information
func (p *P2PNetwork) broadcastProposalInfo() {
	// Create a simple proposal info message with just the address
	proposalInfo := models.ProposalInfo{
		Address:    p.config.Key.Address,
		IsProposal: true,
	}

	// Get all connected nodes
	p.connectionsMu.RLock()

	// 구성 파일의 모든 Gping 노드 주소를 맵에 저장
	configuredNodes := make(map[string]bool)
	for _, gping := range p.config.Gpings {
		configuredNodes[gping.Address] = true
	}

	addresses := make([]string, 0)
	conns := make([]*websocket.Conn, 0)

	// Collect only addresses of configured nodes (not temporary connections)
	for addr, conn := range p.connections {
		if strings.HasPrefix(addr, "temp-") {
			continue // 임시 연결은 건너뜀
		}

		if configuredNodes[addr] {
			addresses = append(addresses, addr)
			conns = append(conns, conn)
		}
	}
	p.connectionsMu.RUnlock()

	// Log broadcast attempt
	log.Printf("🔄  Broadcasting proposal notification to %d configured Gping nodes", len(addresses))

	if len(addresses) == 0 {
		log.Printf("⚠️  No configured nodes connected, cannot broadcast proposal notification")
		return
	}

	// Marshal the proposal info
	proposalBytes, err := json.Marshal(proposalInfo)
	if err != nil {
		log.Printf("Error marshaling proposal info: %v", err)
		return
	}

	// Sign the message
	signature := p.signMessage(proposalBytes)

	// Create the signed message
	signedMessage := models.SignedMessage{
		Message:   proposalBytes,
		Signature: signature,
	}

	// First log our own status as the proposal
	log.Printf("⭐  I am the proposal of this Gping Network: %s", p.config.Key.Address)

	// Send to each connection with its own mutex
	var wg sync.WaitGroup
	sentCount := 0

	for i, addr := range addresses {
		wg.Add(1)
		go func(address string, c *websocket.Conn, idx int) {
			defer wg.Done()

			// Get the write mutex for this connection
			p.connectionsMu.RLock()
			mutex, exists := p.connWriteMutexes[address]
			p.connectionsMu.RUnlock()

			if !exists {
				log.Printf("Connection to %s was closed before proposal notification could be sent", address)
				return
			}

			// Lock the mutex for this specific connection
			mutex.Lock()
			defer mutex.Unlock()

			if err := c.WriteJSON(signedMessage); err != nil {
				log.Printf("❌  Error sending proposal notification to %s: %v", address, err)
			} else {
				log.Printf("✅  Successfully sent proposal notification to %s", address)
				sentCount++
			}
		}(addr, conns[i], i)
	}

	// Wait for all send operations to complete
	wg.Wait()

	// Log summary
	log.Printf("📊 Proposal notification sent to %d/%d configured nodes", sentCount, len(addresses))
}

// Get hostname for the proposal URL
func getHostname() string {
	// Try to get the hostname from environment variable if available
	if host := os.Getenv("GPING_HOST"); host != "" {
		return host
	}

	// Try to determine the machine's IP address
	// This is a simplified approach - in production you'd want more robust IP detection
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return ipnet.IP.String()
				}
			}
		}
	}

	// Fallback to localhost if can't determine
	return "localhost"
}

// HandleProposalInfoBroadcast handles a broadcast of proposal information
func (p *P2PNetwork) handleProposalInfoBroadcast(proposalInfo models.ProposalInfo) {
	// Check if this is a notification from ourselves or from another node
	isFromSelf := proposalInfo.Address == p.config.Key.Address

	if !isFromSelf {
		// Just log the simple message showing the proposal address
		log.Printf("The proposal of this network is %s", proposalInfo.Address)
	}
}

// AddTempConnection adds a temporary connection to the P2P network
// This is used for incoming connections from the HTTP server
func (p *P2PNetwork) AddTempConnection(conn *websocket.Conn, tempAddress string) {
	p.connectionsMu.Lock()
	p.connections[tempAddress] = conn
	p.connWriteMutexes[tempAddress] = &sync.Mutex{} // Create a mutex for this connection
	p.connectionsMu.Unlock()

	log.Printf("✅ Temporary connection added to P2P network with ID: %s", tempAddress)

	// Start receiving messages from this connection
	go p.receiveMessages(conn, tempAddress)
}
