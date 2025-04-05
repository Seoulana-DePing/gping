package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"

	"github.com/Seoulana-DePing/gping/internal/config"
	"github.com/Seoulana-DePing/gping/internal/models"
	"github.com/Seoulana-DePing/gping/internal/p2p"
	"github.com/gorilla/websocket"
)

// Handler handles RPC requests
type Handler struct {
	config       *config.Config
	p2pNetwork   *p2p.P2PNetwork
	vaultAddress string
	tpingData    map[string][]models.TpingData // map[ip][]TpingData
	tpingDataMu  sync.RWMutex
}

// NewHandler creates a new handler
func NewHandler(cfg *config.Config, p2pNetwork *p2p.P2PNetwork) *Handler {
	return &Handler{
		config:       cfg,
		p2pNetwork:   p2pNetwork,
		vaultAddress: cfg.Vault.Address,
		tpingData:    make(map[string][]models.TpingData),
	}
}

// SetupRoutes sets up HTTP routes
func (h *Handler) SetupRoutes(mux *http.ServeMux) {
	// WebSocket endpoint for P2P communication
	mux.HandleFunc("/ws", h.handleWebSocket)
	mux.HandleFunc("/ws/p2p", h.handleWebSocket)

	// RPC endpoints (kept for backward compatibility)
	mux.HandleFunc("/get_location", h.handleGetLocation)
	mux.HandleFunc("/guess_location", h.handleGuessLocation)
}

// HandleWebSocket handles WebSocket connections for P2P communication
func (h *Handler) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Log detailed debug information
	log.Printf("🔍 DEBUG: WebSocket connection request received")
	log.Printf("🔍 DEBUG: URL path: %s", r.URL.Path)
	log.Printf("🔍 DEBUG: Remote address: %s", r.RemoteAddr)
	log.Printf("🔍 DEBUG: Headers: %v", r.Header)

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all connections for simplicity
		},
	}

	// If this is a P2P connection from another Gping, handle it differently
	// For simplicity, we'll use the URL path to determine the connection type
	if r.URL.Path == "/ws/p2p" {
		log.Printf("🔍 DEBUG: Upgrading as P2P connection")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Error upgrading connection to WebSocket for P2P: %v", err)
			return
		}
		// For P2P connections, don't use defer conn.Close() since the P2P handler will manage the connection

		origin := r.Header.Get("Origin")
		remoteIP := r.RemoteAddr
		log.Printf("🔍 DEBUG: P2P connection established from: %s (IP: %s)", origin, remoteIP)

		// Pass to P2P connection handler
		h.handleP2PConnection(conn)
		return
	}

	// Otherwise, this is a client connection for RPC
	log.Printf("🔍 DEBUG: Upgrading as RPC connection")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection to WebSocket for RPC: %v", err)
		return
	}
	// For RPC connections, use defer conn.Close() since this method will return
	defer conn.Close()

	origin := r.Header.Get("Origin")
	log.Printf("🔍 DEBUG: RPC client connection established from: %s", origin)
	h.handleRPCConnection(conn)
}

// HandleP2PConnection handles a WebSocket connection for P2P communication
func (h *Handler) handleP2PConnection(conn *websocket.Conn) {
	// Extract the remote address for logging
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("🔌 P2P connection handling started for: %s", remoteAddr)

	// Initialize a temporary address for this connection
	// We'll use the address from the first received message later
	tempAddr := fmt.Sprintf("temp-%s", remoteAddr)

	log.Printf("🔌 Adding temporary connection with ID: %s", tempAddr)

	// Add this connection to the P2P network with a temporary address
	h.p2pNetwork.AddTempConnection(conn, tempAddr)

	// DO NOT close the connection here - the P2P network will handle it

	// Let the P2P network handle the rest of the message receiving
	log.Printf("✅ P2P connection handed over to P2P network handler for: %s", tempAddr)

	// Wait here instead of returning immediately to prevent the connection from being closed
	// This is necessary because the defer conn.Close() in handleWebSocket would execute otherwise
	select {} // Block forever
}

// HandleRPCConnection handles a WebSocket connection for RPC communication
func (h *Handler) handleRPCConnection(conn *websocket.Conn) {
	for {
		// Read the RPC message
		var message models.RPCMessage
		if err := conn.ReadJSON(&message); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		// Process the RPC method
		go h.processRPCMessage(conn, message)
	}
}

// ProcessRPCMessage processes an RPC message and sends the response
func (h *Handler) processRPCMessage(conn *websocket.Conn, message models.RPCMessage) {
	response := models.RPCMessage{
		ID: message.ID,
	}

	switch message.Method {
	case "get_location":
		h.handleGetLocationRPC(conn, message, &response)
	case "guess_location":
		h.handleGuessLocationRPC(conn, message, &response)
	default:
		response.Error = &models.RPCError{
			Code:    -32601,
			Message: "Method not found",
		}
		conn.WriteJSON(response)
	}
}

// HandleGetLocation handles the get_location RPC method over HTTP (kept for backward compatibility)
func (h *Handler) handleGetLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.LocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Check if we already have an answer for this IP
	if location, exists := models.GetAnswer(req.IP); exists {
		h.sendLocationResponse(w, location)
		return
	}

	// Start a new goroutine to process the location request
	if !models.MarkIPAsProcessing(req.IP) {
		// This IP is already being processed
		http.Error(w, "Request for this IP is already being processed", http.StatusTooManyRequests)
		return
	}

	go h.processLocationRequest(req.IP)

	// Respond immediately to the client
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "processing",
		"message": "Location request is being processed",
	})
}

// HandleGuessLocation handles the guess_location RPC method over HTTP (kept for backward compatibility)
func (h *Handler) handleGuessLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data models.TpingData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate the Tping address
	if !h.isTpingAuthorized(data.Address) {
		http.Error(w, "Unauthorized Tping address", http.StatusUnauthorized)
		return
	}

	// Store the data
	h.tpingDataMu.Lock()
	if _, exists := h.tpingData[data.GPS]; !exists {
		h.tpingData[data.GPS] = make([]models.TpingData, 0)
	}
	h.tpingData[data.GPS] = append(h.tpingData[data.GPS], data)
	h.tpingDataMu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Data received",
	})
}

// HandleGetLocationRPC handles the get_location RPC method
func (h *Handler) handleGetLocationRPC(conn *websocket.Conn, message models.RPCMessage, response *models.RPCMessage) {
	// Parse the parameters
	var req models.LocationRequest
	if err := json.Unmarshal(message.Params, &req); err != nil {
		response.Error = &models.RPCError{
			Code:    -32700,
			Message: "Invalid request format",
		}
		conn.WriteJSON(response)
		return
	}

	// Check if we already have an answer for this IP
	if location, exists := models.GetAnswer(req.IP); exists {
		// Create the result
		result := models.LocationResponse{
			Location: location,
			Vault:    h.vaultAddress,
		}
		resultBytes, _ := json.Marshal(result)
		response.Result = resultBytes
		conn.WriteJSON(response)
		return
	}

	// Start a new goroutine to process the location request
	if !models.MarkIPAsProcessing(req.IP) {
		// This IP is already being processed
		response.Error = &models.RPCError{
			Code:    -32000,
			Message: "Request for this IP is already being processed",
		}
		conn.WriteJSON(response)
		return
	}

	// Send immediate response that processing has started
	processingResult := map[string]string{
		"status":  "processing",
		"message": "Location request is being processed",
	}
	resultBytes, _ := json.Marshal(processingResult)
	response.Result = resultBytes
	conn.WriteJSON(response)

	// Process in the background
	go func() {
		h.processLocationRequest(req.IP)

		// After processing is complete, we could notify the client if we had a mechanism
		// For now, the client would need to poll using another get_location call
	}()
}

// HandleGuessLocationRPC handles the guess_location RPC method
func (h *Handler) handleGuessLocationRPC(conn *websocket.Conn, message models.RPCMessage, response *models.RPCMessage) {
	// Parse the parameters
	var data models.TpingData
	if err := json.Unmarshal(message.Params, &data); err != nil {
		response.Error = &models.RPCError{
			Code:    -32700,
			Message: "Invalid request format",
		}
		conn.WriteJSON(response)
		return
	}

	// Validate the Tping address
	if !h.isTpingAuthorized(data.Address) {
		response.Error = &models.RPCError{
			Code:    -32000,
			Message: "Unauthorized Tping address",
		}
		conn.WriteJSON(response)
		return
	}

	// Store the data
	h.tpingDataMu.Lock()
	if _, exists := h.tpingData[data.GPS]; !exists {
		h.tpingData[data.GPS] = make([]models.TpingData, 0)
	}
	h.tpingData[data.GPS] = append(h.tpingData[data.GPS], data)
	h.tpingDataMu.Unlock()

	// Send success response
	successResult := map[string]string{
		"status":  "success",
		"message": "Data received",
	}
	resultBytes, _ := json.Marshal(successResult)
	response.Result = resultBytes
	conn.WriteJSON(response)
}

// ProcessLocationRequest processes a location request in a separate goroutine
func (h *Handler) processLocationRequest(ip string) {
	defer models.UnmarkIPAsProcessing(ip)

	// This would involve waiting for Tping data, then processing it
	// For now, we'll implement a simple version that just polls the data periodically

	// Wait for enough Tping data
	// In a real implementation, this would involve some kind of waiting mechanism
	// For simplicity, we'll just check if we have any data

	// After receiving enough data, determine the best location
	location, tpingAddresses := h.determineBestLocation(ip)

	if location == "" {
		log.Printf("Failed to determine location for IP: %s", ip)
		return
	}

	// Store the location
	models.StoreAnswer(ip, location)

	// Store Tping addresses for withdrawal
	models.StoreTpingData(ip, tpingAddresses)

	// Broadcast the answer to the P2P network only if is_proposal is true
	if h.config.Server.IsProposal {
		success, err := h.p2pNetwork.BroadcastAnswer(ip, location)
		if err != nil {
			log.Printf("Error broadcasting answer: %v", err)
			return
		}

		if !success {
			log.Printf("Failed to reach consensus for IP: %s", ip)
			return
		}

		log.Printf("Successfully determined location for IP: %s = %s (with consensus)", ip, location)
	} else {
		log.Printf("Successfully determined location for IP: %s = %s (no broadcast)", ip, location)
	}
}

// DetermineBestLocation determines the best location based on Tping data
func (h *Handler) determineBestLocation(ip string) (string, []string) {
	h.tpingDataMu.RLock()
	defer h.tpingDataMu.RUnlock()

	data, exists := h.tpingData[ip]
	if !exists || len(data) == 0 {
		return "", nil
	}

	// Sort the data by response time
	sort.Slice(data, func(i, j int) bool {
		return data[i].Time < data[j].Time
	})

	// Take only the top 10% of data (or at least 1)
	count := max(1, len(data)/10)
	if count > len(data) {
		count = len(data)
	}

	// Get the location with the lowest response time
	bestLocation := data[0].GPS

	// Collect Tping addresses
	tpingAddresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		tpingAddresses = append(tpingAddresses, data[i].Address)
	}

	return bestLocation, tpingAddresses
}

// SendLocationResponse sends a location response
func (h *Handler) sendLocationResponse(w http.ResponseWriter, location string) {
	resp := models.LocationResponse{
		Location: location,
		Vault:    h.vaultAddress,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// IsTpingAuthorized checks if a Tping address is authorized
func (h *Handler) isTpingAuthorized(address string) bool {
	for _, addr := range h.config.Tpings.Addresses {
		if addr == address {
			return true
		}
	}
	return false
}

// StartServer starts the HTTP server
func (h *Handler) StartServer(ctx context.Context) error {
	mux := http.NewServeMux()
	h.SetupRoutes(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", h.config.Server.Port),
		Handler: mux,
	}

	// Start the server
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Shutdown the server
	return server.Shutdown(context.Background())
}
