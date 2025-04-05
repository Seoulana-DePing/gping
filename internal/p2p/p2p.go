package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
		config:         cfg,
		privateKey:     privateKey,
		connections:    make(map[string]*websocket.Conn),
		messageChan:    make(chan models.SignedMessage, 100),
		broadcastChan:  make(chan models.SignedMessage, 100),
		signatures:     make(map[string][]string),
		pendingResults: make(map[string]chan bool),
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
	defer p.connectionsMu.RUnlock()

	if len(p.connections) == len(p.config.Gpings) {
		log.Printf("🎉   All the GPings are connected.")
	}
}

// Connect to another Gping node
func (p *P2PNetwork) connectToGping(gping config.GpingConfig) error {
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(gping.URL, nil)
	if err != nil {
		log.Printf("Failed to connect to Gping node at %s: %v", gping.URL, err)
		return err
	}

	p.connectionsMu.Lock()
	p.connections[gping.Address] = conn
	p.connectionsMu.Unlock()

	go p.receiveMessages(conn, gping.Address)

	return nil
}

// Receive messages from a connected Gping node
func (p *P2PNetwork) receiveMessages(conn *websocket.Conn, address string) {
	defer func() {
		conn.Close()
		p.connectionsMu.Lock()
		delete(p.connections, address)
		p.connectionsMu.Unlock()
	}()

	for {
		var message models.SignedMessage
		if err := conn.ReadJSON(&message); err != nil {
			log.Printf("Error reading message from %s: %v", address, err)
			return
		}

		// Validate the signature
		if !p.validateSignature(message, address) {
			log.Printf("Invalid signature from %s", address)
			continue
		}

		// Process the message
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
	// Check if this is a location answer broadcast or a signature response
	var locationAnswer [2]string
	if err := json.Unmarshal(message.Message, &locationAnswer); err == nil {
		// This is a location answer broadcast
		p.handleLocationAnswerBroadcast(message, locationAnswer)
	} else {
		// This might be a signature response to our broadcast
		p.handleSignatureResponse(message)
	}
}

// HandleLocationAnswerBroadcast handles a broadcast of a location answer
func (p *P2PNetwork) handleLocationAnswerBroadcast(message models.SignedMessage, locationAnswer [2]string) {
	ip := locationAnswer[0]
	location := locationAnswer[1]

	// Check if we have a matching answer
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
	for _, conn := range p.connections {
		go func(c *websocket.Conn) {
			if err := c.WriteJSON(message); err != nil {
				log.Printf("Error broadcasting message: %v", err)
			}
		}(conn)
	}
	p.connectionsMu.RUnlock()
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
