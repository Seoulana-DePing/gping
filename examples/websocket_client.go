package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RPCMessage represents a WebSocket RPC message
type RPCMessage struct {
	ID     string          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

// RPCError represents an error in a WebSocket RPC message
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// LocationRequest is the request structure for the get_location RPC method
type LocationRequest struct {
	IP        string `json:"ip"`
	RequestId string `json:"request_id"`
}

// LocationResponse is the response structure for the get_location RPC method
type LocationResponse struct {
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	SPAddress string `json:"sp_address"` // SP contract address
	RequestId string `json:"request_id"`
}

// TpingData represents the data sent by Tpings
type TpingData struct {
	GPS     string `json:"gps"`
	Time    uint   `json:"time"`
	Address string `json:"address"`
}

// ResponseHandler is a structure to handle WebSocket responses
type ResponseHandler struct {
	Resp  chan json.RawMessage
	Error chan *RPCError
}

// GpingWebSocketClient is a WebSocket client for Gping
type GpingWebSocketClient struct {
	URL            string
	Conn           *websocket.Conn
	mu             sync.Mutex
	pendingRequest map[string]*ResponseHandler
}

// NewGpingWebSocketClient creates a new Gping WebSocket client
func NewGpingWebSocketClient(url string) *GpingWebSocketClient {
	return &GpingWebSocketClient{
		URL:            url,
		pendingRequest: make(map[string]*ResponseHandler),
	}
}

// Connect connects to the WebSocket server
func (c *GpingWebSocketClient) Connect() error {
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(c.URL, nil)
	if err != nil {
		return fmt.Errorf("error connecting to WebSocket: %w", err)
	}
	c.Conn = conn

	// Start listening for messages
	go c.listenForMessages()

	return nil
}

// Close closes the WebSocket connection
func (c *GpingWebSocketClient) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

// GenerateMessageID generates a unique message ID
func (c *GpingWebSocketClient) GenerateMessageID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// RegisterRequest registers a request for response handling
func (c *GpingWebSocketClient) RegisterRequest(id string) *ResponseHandler {
	handler := &ResponseHandler{
		Resp:  make(chan json.RawMessage, 1),
		Error: make(chan *RPCError, 1),
	}

	c.mu.Lock()
	c.pendingRequest[id] = handler
	c.mu.Unlock()

	return handler
}

// UnregisterRequest removes a request from handling
func (c *GpingWebSocketClient) UnregisterRequest(id string) {
	c.mu.Lock()
	delete(c.pendingRequest, id)
	c.mu.Unlock()
}

// ListenForMessages listens for incoming WebSocket messages
func (c *GpingWebSocketClient) listenForMessages() {
	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WebSocket message: %v", err)
			return
		}

		var response RPCMessage
		if err := json.Unmarshal(message, &response); err != nil {
			log.Printf("Error unmarshaling response: %v", err)
			continue
		}

		log.Printf("Received response: %s", message)

		// Handle the response
		c.mu.Lock()
		handler, exists := c.pendingRequest[response.ID]
		c.mu.Unlock()

		if exists {
			if response.Error != nil {
				handler.Error <- response.Error
			} else {
				handler.Resp <- response.Result
			}
		} else {
			log.Printf("Received response for unknown request ID: %s", response.ID)
		}
	}
}

// GetLocation gets the location for an IP address
func (c *GpingWebSocketClient) GetLocation(ctx context.Context, ip string) (*LocationResponse, error) {
	id := c.GenerateMessageID()
	handler := c.RegisterRequest(id)
	defer c.UnregisterRequest(id)

	// 요청 ID도 추가
	requestId := fmt.Sprintf("req-%s", id)

	// Create the request
	req := LocationRequest{
		IP:        ip,
		RequestId: requestId,
	}
	params, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling params: %w", err)
	}

	message := RPCMessage{
		ID:     id,
		Method: "get_location",
		Params: params,
	}

	// Send the request
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("error marshaling message: %w", err)
	}

	log.Printf("Sending request: %s", messageBytes)
	if err := c.Conn.WriteMessage(websocket.TextMessage, messageBytes); err != nil {
		return nil, fmt.Errorf("error sending message: %w", err)
	}

	// Wait for the response
	select {
	case result := <-handler.Resp:
		// 응답을 LocationResponse 구조체로 변환
		var response LocationResponse
		if err := json.Unmarshal(result, &response); err != nil {
			return nil, fmt.Errorf("error unmarshaling location response: %w", err)
		}
		return &response, nil
	case rpcErr := <-handler.Error:
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcErr.Message, rpcErr.Code)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SendTpingData sends Tping data
func (c *GpingWebSocketClient) SendTpingData(ctx context.Context, gps string, time uint, address string) (json.RawMessage, error) {
	id := c.GenerateMessageID()
	handler := c.RegisterRequest(id)
	defer c.UnregisterRequest(id)

	// Create the request
	req := TpingData{
		GPS:     gps,
		Time:    time,
		Address: address,
	}
	params, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling params: %w", err)
	}

	message := RPCMessage{
		ID:     id,
		Method: "guess_location",
		Params: params,
	}

	// Send the request
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("error marshaling message: %w", err)
	}

	log.Printf("Sending request: %s", messageBytes)
	if err := c.Conn.WriteMessage(websocket.TextMessage, messageBytes); err != nil {
		return nil, fmt.Errorf("error sending message: %w", err)
	}

	// Wait for the response
	select {
	case result := <-handler.Resp:
		return result, nil
	case rpcErr := <-handler.Error:
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcErr.Message, rpcErr.Code)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func main() {
	client := NewGpingWebSocketClient("ws://localhost:1111/ws")
	if err := client.Connect(); err != nil {
		log.Fatalf("Error connecting to Gping: %v", err)
	}
	defer client.Close()

	log.Println("Connected to Gping WebSocket server")

	// Create a context that will be canceled on Ctrl+C
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Println("Received Ctrl+C, shutting down...")
		cancel()
	}()

	// Example: Get location
	response, err := client.GetLocation(ctx, "203.0.113.42")
	if err != nil {
		log.Printf("Error getting location: %v", err)
	} else {
		log.Printf("Get location result:")
		log.Printf("  Latitude: %s", response.Latitude)
		log.Printf("  Longitude: %s", response.Longitude)
		log.Printf("  SP Address: %s", response.SPAddress)
		log.Printf("  Request ID: %s", response.RequestId)
	}

	// Example: Send Tping data
	/*
		result, err = client.SendTpingData(ctx, "37.5665,126.9780", 150, "TpingAddress1123456789")
		if err != nil {
			log.Printf("Error sending Tping data: %v", err)
		} else {
			log.Printf("Send Tping data result: %s", result)
		}
	*/

	// Wait for Ctrl+C
	<-ctx.Done()
}
