package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

// TpingResultRequest represents the POST request body for the /result endpoint
type TpingResultRequest struct {
	WalletAddress string  `json:"wallet_address"`
	ResponseTime  float64 `json:"response_time"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	IP            string  `json:"ip"`
}

// PollingResponse represents the response from the /polling endpoint
type PollingResponse struct {
	IP string `json:"ip"`
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

// GpingHTTPClient is an HTTP client for Gping's REST API
type GpingHTTPClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewGpingWebSocketClient creates a new Gping WebSocket client
func NewGpingWebSocketClient(url string) *GpingWebSocketClient {
	return &GpingWebSocketClient{
		URL:            url,
		pendingRequest: make(map[string]*ResponseHandler),
	}
}

// NewGpingHTTPClient creates a new Gping HTTP client
func NewGpingHTTPClient(baseURL string) *GpingHTTPClient {
	return &GpingHTTPClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
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

		// 응답에 RequestId가 없으면 우리가 보낸 ID로 설정
		if response.RequestId == "" {
			response.RequestId = requestId
		}

		return &response, nil
	case rpcErr := <-handler.Error:
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcErr.Message, rpcErr.Code)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetLocationWithPolling combines WebSocket and HTTP functionality to get the location
func (c *GpingWebSocketClient) GetLocationWithPolling(ctx context.Context, httpClient *GpingHTTPClient, ip string, walletAddress string) (*LocationResponse, error) {
	// Use channels for async communication
	resultChan := make(chan *LocationResponse, 1)
	errorChan := make(chan error, 1)
	stopPollingChan := make(chan struct{}, 1)

	// Start WebSocket request
	go func() {
		id := c.GenerateMessageID()
		handler := c.RegisterRequest(id)
		defer c.UnregisterRequest(id)

		// Create the request
		req := LocationRequest{
			IP:        ip,
			RequestId: id,
		}
		params, err := json.Marshal(req)
		if err != nil {
			errorChan <- fmt.Errorf("error marshaling params: %w", err)
			return
		}

		message := RPCMessage{
			ID:     id,
			Method: "get_location",
			Params: params,
		}

		// Send the request via WebSocket
		messageBytes, err := json.Marshal(message)
		if err != nil {
			errorChan <- fmt.Errorf("error marshaling message: %w", err)
			return
		}

		log.Printf("Sending location request via WebSocket: %s", messageBytes)
		if err := c.Conn.WriteMessage(websocket.TextMessage, messageBytes); err != nil {
			errorChan <- fmt.Errorf("error sending message: %w", err)
			return
		}

		// Wait for the response from WebSocket
		select {
		case result := <-handler.Resp:
			var response LocationResponse
			if err := json.Unmarshal(result, &response); err != nil {
				errorChan <- fmt.Errorf("error unmarshaling response: %w", err)
				return
			}

			// We got a response through WebSocket, no need to poll anymore
			log.Printf("✅ Received response via WebSocket, stopping HTTP polling")
			close(stopPollingChan)
			resultChan <- &response

		case rpcErr := <-handler.Error:
			errorChan <- fmt.Errorf("RPC error: %s (code: %d)", rpcErr.Message, rpcErr.Code)

		case <-ctx.Done():
			errorChan <- ctx.Err()
		}
	}()

	// Start polling REST API for IP
	go func() {
		// Wait a short time before starting to poll to allow the WebSocket to initialize
		time.Sleep(500 * time.Millisecond)

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		// Only send result once flag
		resultSent := false

		for {
			select {
			case <-stopPollingChan:
				// Stop polling if we got a direct response from WebSocket or were signaled to stop
				log.Printf("👋 Stopping HTTP polling as requested")
				return

			case <-ticker.C:
				// Check if we already sent a result
				if resultSent {
					break
				}

				// Poll the /polling endpoint to get the latest requested IP
				pollingResp, err := httpClient.Poll(ctx)
				if err != nil {
					log.Printf("❌ Error polling: %v", err)
					continue
				} else {
					log.Printf("📊 Polling response: IP=%s", pollingResp.IP)
				}

				// Check if the IP matches our requested IP
				if pollingResp.IP == ip {
					log.Printf("🎯 Found matching IP in polling: %s", ip)

					// Send the result to the REST API
					err = httpClient.SendResult(ctx, TpingResultRequest{
						WalletAddress: walletAddress,
						ResponseTime:  float64(100 + time.Now().UnixNano()%900), // Random response time between 100-1000ms
						Latitude:      37.5326,                                  // Sample latitude (Seoul)
						Longitude:     127.0246,                                 // Sample longitude (Seoul)
						IP:            ip,
					})

					if err != nil {
						log.Printf("❌ Error sending result: %v", err)
					} else {
						log.Printf("✅ Successfully sent result for IP: %s", ip)
						resultSent = true

						// No need to exit the polling loop - continue polling but don't send results
						// The server might still be processing and we want to detect when it has an answer
					}
				} else if pollingResp.IP == "" {
					log.Printf("⏳ No IP in polling response, waiting...")
				} else {
					log.Printf("⚠️ IP mismatch in polling: got '%s', expecting '%s'", pollingResp.IP, ip)
				}

			case <-ctx.Done():
				log.Printf("👋 Context cancelled, stopping HTTP polling")
				return
			}
		}
	}()

	// Wait for result or error
	select {
	case response := <-resultChan:
		log.Printf("✅ Returning final location result for IP: %s", ip)
		return response, nil
	case err := <-errorChan:
		log.Printf("❌ Returning error for IP %s: %v", ip, err)
		return nil, err
	case <-ctx.Done():
		log.Printf("👋 Context done, returning error")
		return nil, ctx.Err()
	}
}

// Poll polls the /polling endpoint to get the latest requested IP
func (c *GpingHTTPClient) Poll(ctx context.Context) (*PollingResponse, error) {
	url := fmt.Sprintf("%s/polling", c.BaseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}

	var response PollingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &response, nil
}

// SendResult sends the result to the /result endpoint
func (c *GpingHTTPClient) SendResult(ctx context.Context, result TpingResultRequest) error {
	url := fmt.Sprintf("%s/result", c.BaseURL)

	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("error marshaling result: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}

	return nil
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

// GetLocationWithPolling is a helper function to call the method on GpingWebSocketClient
func GetLocationWithPolling(wsClient *GpingWebSocketClient, httpClient *GpingHTTPClient, ctx context.Context, ip string, walletAddress string) (*LocationResponse, error) {
	return wsClient.GetLocationWithPolling(ctx, httpClient, ip, walletAddress)
}

func main() {
	// Hardcoded configuration based on config_1.toml
	websocketURL := "ws://localhost:1111/ws"
	restAPIBaseURL := "http://localhost:1112"
	ipAddress := "8.8.8.8"
	walletAddress := "H3kBWmfufNFRtievRDFyGc2Udq5eoDmsk816TpVwgRNU"

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create WebSocket client
	wsClient := NewGpingWebSocketClient(websocketURL)
	if err := wsClient.Connect(); err != nil {
		log.Fatalf("Error connecting to WebSocket: %v", err)
	}
	defer wsClient.Close()

	// Create HTTP client for REST API
	httpClient := NewGpingHTTPClient(restAPIBaseURL)

	// Set up signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		log.Println("Received interrupt signal. Shutting down...")
		cancel()
	}()

	log.Printf("Starting location request for IP: %s, wallet: %s", ipAddress, walletAddress)
	response, err := GetLocationWithPolling(wsClient, httpClient, ctx, ipAddress, walletAddress)
	if err != nil {
		log.Fatalf("Error getting location: %v", err)
	}

	log.Printf("Location for IP %s: Latitude=%s, Longitude=%s, SP=%s, RequestId=%s",
		ipAddress, response.Latitude, response.Longitude, response.SPAddress, response.RequestId)
}
