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

	// RPC endpoints
	mux.HandleFunc("/get_location", h.handleGetLocation)
	mux.HandleFunc("/guess_location", h.handleGuessLocation)
}

// HandleWebSocket handles WebSocket connections for P2P communication
func (h *Handler) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all connections for simplicity
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection to WebSocket: %v", err)
		return
	}

	// Handle the WebSocket connection
	// This would be managed by the P2P network
	// For simplicity, we'll just close it after a while
	defer conn.Close()

	// Keep the connection alive until the client disconnects
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading from WebSocket: %v", err)
			break
		}
	}
}

// HandleGetLocation handles the get_location RPC method
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

	// Broadcast the answer to the P2P network
	success, err := h.p2pNetwork.BroadcastAnswer(ip, location)
	if err != nil {
		log.Printf("Error broadcasting answer: %v", err)
		return
	}

	if !success {
		log.Printf("Failed to reach consensus for IP: %s", ip)
		return
	}

	log.Printf("Successfully determined location for IP: %s = %s", ip, location)
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

// HandleGuessLocation handles the guess_location RPC method
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
