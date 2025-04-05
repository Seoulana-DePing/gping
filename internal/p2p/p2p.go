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
	allConnected := len(p.connections) == len(p.config.Gpings)
	p.connectionsMu.RUnlock()

	if allConnected {
		log.Printf("🎉   All the GPings are connected (%d nodes total).", len(p.config.Gpings))

		// If this node is a proposal, wait a moment for all connections to stabilize then broadcast
		if p.config.Server.IsProposal {
			log.Printf("🔄   This node is configured as the proposal node. Preparing to broadcast proposal notification...")

			// Add a short delay to ensure all connections are fully established
			go func() {
				time.Sleep(1 * time.Second)
				p.broadcastProposalInfo()
			}()
		} else {
			log.Printf("ℹ️   This node is not the proposal. Waiting for proposal notification...")
		}
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

	// For temporary connections, try to read a handshake message first
	if strings.HasPrefix(address, "temp-") {
		log.Printf("🔍 DEBUG: Waiting for identification from temporary connection: %s", address)
	}

	// Setup ping/pong handlers to keep the connection alive
	conn.SetPingHandler(func(data string) error {
		log.Printf("🔍 DEBUG: Received ping from %s", address)
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
	})

	conn.SetPongHandler(func(data string) error {
		log.Printf("🔍 DEBUG: Received pong from %s", address)
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

			log.Printf("🔍 DEBUG: Sending ping to %s", address)

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
			// Check if this is a normal closure
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("🔍 DEBUG: Normal WebSocket closure from %s", address)
			} else {
				log.Printf("⚠️ Unexpected connection error from %s: %v", address, err)
			}
			return
		}

		log.Printf("📥 Received message from %s", address)

		// Check if this is a proposal info message first
		var proposalInfo models.ProposalInfo
		if err := json.Unmarshal(message.Message, &proposalInfo); err == nil && proposalInfo.IsProposal {
			// This is a proposal notification from another node
			log.Printf("🔔 Received proposal notification from node: %s", address)

			// Handle the proposal info immediately
			p.handleProposalInfoBroadcast(proposalInfo)

			// Skip further processing of this message
			continue
		}

		// For other message types
		var locationAnswer [2]string
		if json.Unmarshal(message.Message, &locationAnswer) == nil {
			log.Printf("📌 Received location answer for IP: %s", locationAnswer[0])
		} else {
			// Only log other message types if needed
			log.Printf("📌 Received other message type from: %s", address)
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

	// Try to unmarshal as a proposal info - this should have been handled in receiveMessages already
	// but we keep it for older versions compatibility
	var proposalInfo models.ProposalInfo
	if err := json.Unmarshal(message.Message, &proposalInfo); err == nil {
		// This is a proposal info broadcast that somehow wasn't processed in receiveMessages
		log.Printf("⚠️ Late processing of proposal info in processMessage from: %s", proposalInfo.URL)
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

	// If tpings array is empty, we automatically agree with any answer
	if len(p.config.Tpings.Addresses) == 0 {
		// We agree with this answer, sign the message and send back
		signature := p.signMessage(message.Message)
		response := models.SignedMessage{
			Message:   message.Message,
			Signature: signature,
		}

		// Send the response to all nodes
		p.broadcastChan <- response
		return
	}

	// Otherwise, check if we have a matching answer
	storedLocation, exists := models.GetAnswer(ip)
	if exists && storedLocation == location {
		// We agree with this answer, sign the message and send back
		signature := p.signMessage(message.Message)
		response := models.SignedMessage{
			Message:   message.Message,
			Signature: signature,
		}

		// Find the originator by signature (would be better with a message ID in practice)
		// For simplicity, we'll broadcast the response to all nodes
		p.broadcastChan <- response
	}
}

// HandleSignatureResponse handles a signature response to our broadcast
func (p *P2PNetwork) handleSignatureResponse(message models.SignedMessage) {
	messageStr := string(message.Message)

	p.signaturesMu.Lock()
	p.signatures[messageStr] = append(p.signatures[messageStr], message.Signature)

	// Check if we have enough signatures (2/3 of the network)
	requiredSignatures := (len(p.config.Gpings) * 2) / 3
	if len(p.signatures[messageStr]) >= requiredSignatures {
		p.signaturesMu.Unlock()

		// Notify any waiting goroutines that we have consensus
		p.pendingResultsMu.Lock()
		if resultChan, exists := p.pendingResults[messageStr]; exists {
			resultChan <- true
			delete(p.pendingResults, messageStr)
		}
		p.pendingResultsMu.Unlock()
	} else {
		p.signaturesMu.Unlock()
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
	p.connectionsMu.RLock()
	addresses := make([]string, 0, len(p.connections))
	conns := make([]*websocket.Conn, 0, len(p.connections))

	// First, collect all addresses and connections
	for addr, conn := range p.connections {
		addresses = append(addresses, addr)
		conns = append(conns, conn)
	}
	p.connectionsMu.RUnlock()

	// Then, send to each connection with its own mutex
	for i, addr := range addresses {
		go func(address string, c *websocket.Conn) {
			// Get the write mutex for this connection
			p.connectionsMu.RLock()
			mutex, exists := p.connWriteMutexes[address]
			p.connectionsMu.RUnlock()

			if !exists {
				log.Printf("Connection to %s was closed before message could be sent", address)
				return
			}

			// Lock the mutex for this specific connection
			mutex.Lock()
			defer mutex.Unlock()

			if err := c.WriteJSON(message); err != nil {
				log.Printf("Error broadcasting message to %s: %v", address, err)
			}
		}(addr, conns[i])
	}
}

// BroadcastAnswer broadcasts a location answer and waits for consensus
func (p *P2PNetwork) BroadcastAnswer(ip, location string) (bool, error) {
	// Create the message array [ip, location]
	message := [2]string{ip, location}

	// Marshal the message
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return false, fmt.Errorf("error marshaling message: %w", err)
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
	messageStr := string(messageBytes)
	p.pendingResultsMu.Lock()
	p.pendingResults[messageStr] = resultChan
	p.pendingResultsMu.Unlock()

	// Broadcast the message
	p.broadcastChan <- signedMessage

	// Wait for consensus with timeout
	select {
	case result := <-resultChan:
		return result, nil
	case <-context.Background().Done():
		// Remove the pending result
		p.pendingResultsMu.Lock()
		delete(p.pendingResults, messageStr)
		p.pendingResultsMu.Unlock()
		return false, fmt.Errorf("timeout waiting for consensus")
	}
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
	// Create a proposal info message with additional details
	proposalInfo := models.ProposalInfo{
		URL:        fmt.Sprintf("ws://%s:%d/ws/p2p", getHostname(), p.config.Server.Port),
		Address:    p.config.Key.Address,
		IsProposal: true,
		Message:    "I am the proposal node for this Gping network",
		Timestamp:  time.Now().Unix(),
	}

	// Log detailed debug information
	log.Printf("🔍 DEBUG: Proposal URL to broadcast: %s", proposalInfo.URL)
	log.Printf("🔍 DEBUG: My proposal address: %s", proposalInfo.Address)

	// Log broadcast attempt
	log.Printf("🔄 Broadcasting proposal notification to %d nodes", len(p.connections))

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
	log.Printf("⭐ I am the proposal of this Gping Network: %s", proposalInfo.URL)

	// Get all connected nodes
	p.connectionsMu.RLock()
	addresses := make([]string, 0, len(p.connections))
	conns := make([]*websocket.Conn, 0, len(p.connections))

	// Collect all addresses and connections
	for addr, conn := range p.connections {
		addresses = append(addresses, addr)
		conns = append(conns, conn)
		log.Printf("🔍 DEBUG: Will send to node: %s", addr)
	}
	p.connectionsMu.RUnlock()

	if len(addresses) == 0 {
		log.Printf("⚠️ No nodes connected, cannot broadcast proposal notification")
		return
	}

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

			log.Printf("📤 Sending proposal notification to node: %s", address)
			log.Printf("🔍 DEBUG: Using connection %p to send to %s", c, address)

			// Lock the mutex for this specific connection
			mutex.Lock()
			defer mutex.Unlock()

			if err := c.WriteJSON(signedMessage); err != nil {
				log.Printf("❌ Error sending proposal notification to %s: %v", address, err)
			} else {
				log.Printf("✅ Successfully sent proposal notification to %s", address)
				sentCount++
			}
		}(addr, conns[i], i)
	}

	// Wait for all send operations to complete
	wg.Wait()

	// Log summary
	log.Printf("📊 Proposal notification sent to %d/%d nodes", sentCount, len(addresses))
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
	// Store the proposal information for future reference
	// In a real implementation, this might update a registry of proposals

	// Check if this is a notification from ourselves or from another node
	isFromSelf := proposalInfo.Address == p.config.Key.Address

	if !isFromSelf {
		// Log the detailed proposal notification in a very visible way
		log.Printf("=========================================================================")
		log.Printf("📣 PROPOSAL NOTIFICATION RECEIVED FROM ANOTHER NODE")
		log.Printf("=========================================================================")
		log.Printf("⭐ The proposal of this Gping Network is %s (address: %s)",
			proposalInfo.URL, proposalInfo.Address)

		if proposalInfo.IsProposal {
			log.Printf("🔰 Node confirmed as proposal: %t", proposalInfo.IsProposal)
		}

		if proposalInfo.Message != "" {
			log.Printf("📝 Message from proposal: %s", proposalInfo.Message)
		}

		if proposalInfo.Timestamp > 0 {
			log.Printf("🕒 Notification timestamp: %s",
				time.Unix(proposalInfo.Timestamp, 0).Format(time.RFC3339))
		}

		// For debugging, log our own address to verify nodes are properly identified
		log.Printf("📊 My node address: %s", p.config.Key.Address)

		// Acknowledge receipt of proposal notification
		log.Printf("✅ Successfully processed proposal notification from %s", proposalInfo.Address)
		log.Printf("=========================================================================")
	} else {
		// This is our own proposal info being processed locally - just log basic info
		log.Printf("📝 Processed local proposal info - I am the proposal of this network")
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
