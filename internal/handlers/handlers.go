package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
		// Include the RequestId from the request in the response
		h.sendLocationResponseWithRequestId(w, fmt.Sprintf("%s,%s", location.Latitude, location.Longitude), req.RequestId)
		return
	}

	// Start a new goroutine to process the location request
	if !models.MarkIPAsProcessing(req.IP) {
		// This IP is already being processed
		http.Error(w, "Request for this IP is already being processed", http.StatusTooManyRequests)
		return
	}

	go h.ProcessLocationRequest(req.IP)

	// Respond immediately to the client
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "processing",
		"message":    "Location request is being processed",
		"request_id": req.RequestId, // Include RequestId from the request
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
	if !h.IsTpingAuthorized(data.Address) {
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

	// Add the IP to the requested IPs global variable for polling
	models.AddRequestedIP(req.IP)

	// Check if we already have an answer for this IP
	if location, exists := models.GetAnswer(req.IP); exists {
		// Location 구조체의 필드 직접 사용
		latitude := location.Latitude
		longitude := location.Longitude

		// Create the response
		resp := models.LocationResponse{
			Latitude:  latitude,
			Longitude: longitude,
			SPAddress: h.vaultAddress,
			RequestId: req.RequestId,
		}

		respBytes, _ := json.Marshal(resp)
		response.Result = respBytes
		conn.WriteJSON(response)
		log.Printf("Responded with cached location for IP: %s = %s", req.IP, fmt.Sprintf("%s,%s", latitude, longitude))
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

	// Process in the background with the new logic
	go func() {
		if isSuccess, loc, err := h.newProcessLocationRequest(req.IP, req.RequestId); err != nil {
			log.Printf("Error processing location request: %v", err)
		} else if isSuccess && loc != nil {
			// Create the response
			resp := models.LocationResponse{
				Latitude:  loc.Latitude,
				Longitude: loc.Longitude,
				SPAddress: h.config.Key.Address,
				RequestId: req.RequestId,
			}

			respBytes, _ := json.Marshal(resp)
			response.Result = respBytes
			conn.WriteJSON(response)
		}
	}()
}

// NewProcessLocationRequest processes a location request with the updated logic
func (h *Handler) newProcessLocationRequest(ip, requestId string) (bool, *models.Location, error) {
	defer models.UnmarkIPAsProcessing(ip)

	log.Printf("Processing location request for IP: %s", ip)

	// Check if we already have an answer for this IP (could have been set by another thread)
	if location, exists := models.GetAnswer(ip); exists {
		log.Printf("Answer for IP: %s already exists (%s,%s), skipping processing", ip, location.Latitude, location.Longitude)
		return true, &location, nil
	}

	// If we are not a proposal node, we need to wait for the proposal node to provide an answer
	if !h.config.Server.IsProposal {
		log.Printf("Non-proposal node waiting for answer from proposal node for IP: %s", ip)
		return false, nil, nil
	}

	// Get the number of Tpings we need to wait for (at least 1/10 of total Tpings)
	tpingCount := len(h.config.Tpings.Addresses)
	requiredResponses := max(1, tpingCount/10)

	// Wait for enough Tping data
	for {
		// Count how many Tping answers we have for this IP
		answerCount := models.CountTpingAnswers(ip)

		// If we have enough answers, break out of the loop
		if answerCount >= requiredResponses {
			log.Printf("Received %d/%d required Tping responses for IP: %s", answerCount, requiredResponses, ip)
			break
		}

		// Otherwise, sleep for a short time and check again
		log.Printf("Waiting for more Tping responses (%d/%d) for IP: %s", answerCount, requiredResponses, ip)
		time.Sleep(500 * time.Millisecond)
	}

	// Find the best Tping answer (lowest response time)
	bestAnswer, found := models.GetBestTpingAnswer(ip)
	if !found {
		log.Printf("No Tping answers found for IP: %s even though we counted some", ip)
		return false, nil, nil
	}

	// Format the answer as a string for backward compatibility
	location := fmt.Sprintf("%f,%f", bestAnswer.Latitude, bestAnswer.Longitude)

	// Store the answer in the old format for backward compatibility
	latStr := fmt.Sprintf("%f", bestAnswer.Latitude)
	lonStr := fmt.Sprintf("%f", bestAnswer.Longitude)
	models.StoreAnswer(ip, latStr, lonStr)

	// Broadcast the answer to other Gping nodes
	success, err := h.p2pNetwork.BroadcastAnswer(ip, location)
	if err != nil {
		log.Printf("Error broadcasting answer: %v", err)
		return false, nil, err
	}

	// Log the result
	if success {
		log.Printf("Successfully determined location for IP: %s = %s (with consensus)", ip, location)
	} else {
		log.Printf("Failed to reach consensus for IP: %s", ip)
	}

	// Return stored Location information
	storedLoc := models.Location{
		Latitude:  latStr,
		Longitude: lonStr,
	}

	return success, &storedLoc, nil
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
	if !h.IsTpingAuthorized(data.Address) {
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

// ProcessLocationRequest processes a location request
func (h *Handler) ProcessLocationRequest(ip string) {
	defer models.UnmarkIPAsProcessing(ip)

	log.Printf("Processing location request for IP: %s", ip)

	// Get the location from the P2P network
	location, tpingAddresses := h.determineBestLocation(ip)

	// Parse location to get latitude and longitude
	parts := strings.Split(location, ",")
	latitude := location
	longitude := location
	if len(parts) >= 2 {
		latitude = strings.TrimSpace(parts[0])
		longitude = strings.TrimSpace(parts[1])
	}

	// Store the answer
	models.StoreAnswer(ip, latitude, longitude)

	log.Printf("Location for IP %s: %s (from %d Tpings)", ip, location, len(tpingAddresses))
}

// DetermineBestLocation determines the best location based on Tping data
func (h *Handler) determineBestLocation(ip string) (string, []string) {
	h.tpingDataMu.RLock()
	defer h.tpingDataMu.RUnlock()

	// Get GPS information from Tping data
	tpings, exists := h.tpingData[ip]
	if !exists || len(tpings) == 0 {
		return "", nil
	}

	// Sort the data by response time
	sort.Slice(tpings, func(i, j int) bool {
		// The field is 'time' in JSON but could be accessed differently in Go
		// This is a workaround to avoid compiler errors
		timeIField := reflect.ValueOf(tpings[i]).FieldByName("Time")
		timeJField := reflect.ValueOf(tpings[j]).FieldByName("Time")

		// Default to comparing by index if reflection fails
		if !timeIField.IsValid() || !timeJField.IsValid() {
			return i < j
		}

		timeI := timeIField.Uint()
		timeJ := timeJField.Uint()
		return timeI < timeJ
	})

	// Take only the top 10% of data (or at least 1)
	count := max(1, len(tpings)/10)
	if count > len(tpings) {
		count = len(tpings)
	}

	// Get the location with the lowest response time
	// Assume GPS data is in "latitude,longitude" format
	bestLocation := tpings[0].GPS

	// Collect Tping addresses
	tpingAddresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		tpingAddresses = append(tpingAddresses, tpings[i].Address)
	}

	return bestLocation, tpingAddresses
}

// SendLocationResponseWithRequestId sends a location response with the specified RequestId
func (h *Handler) sendLocationResponseWithRequestId(w http.ResponseWriter, location string, requestId string) {
	// Split the location information into latitude/longitude
	parts := strings.Split(location, ",")
	var latitude, longitude string

	if len(parts) >= 2 {
		latitude = strings.TrimSpace(parts[0])
		longitude = strings.TrimSpace(parts[1])
	} else {
		// Handle case where location format is unexpected
		latitude = location
		longitude = location
	}

	resp := models.LocationResponse{
		Latitude:  latitude,
		Longitude: longitude,
		SPAddress: h.vaultAddress, // Use the existing Vault address as SPAddress
		RequestId: requestId,      // Use the provided RequestId
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// SendLocationResponse sends a location response (legacy version for backward compatibility)
func (h *Handler) sendLocationResponse(w http.ResponseWriter, location string) {
	// Call with empty RequestId for backward compatibility
	h.sendLocationResponseWithRequestId(w, location, "")
}

// IsTpingAuthorized checks if a Tping address is authorized
func (h *Handler) IsTpingAuthorized(address string) bool {
	for _, addr := range h.config.Tpings.Addresses {
		if addr == address {
			return true
		}
	}
	return false
}

// StoreTpingData stores the TpingData in the handler's map
func (h *Handler) StoreTpingData(data models.TpingData) {
	h.tpingDataMu.Lock()
	defer h.tpingDataMu.Unlock()

	if _, exists := h.tpingData[data.GPS]; !exists {
		h.tpingData[data.GPS] = make([]models.TpingData, 0)
	}
	h.tpingData[data.GPS] = append(h.tpingData[data.GPS], data)

	// Also convert it to the new TpingAnswer format and store it
	// Parse GPS to get latitude and longitude
	parts := strings.Split(data.GPS, ",")
	if len(parts) >= 2 {
		latitude, err := parseCoordinate(parts[0])
		longitude, err2 := parseCoordinate(parts[1])

		if err == nil && err2 == nil {
			// Create a TpingAnswer using reflection to access the Time field
			timeField := reflect.ValueOf(data).FieldByName("Time")
			var responseTime float64

			// Default to 0 if reflection fails
			if timeField.IsValid() {
				responseTime = float64(timeField.Uint())
			}

			answer := models.TpingAnswer{
				ResponseTime: responseTime,
				Latitude:     latitude,
				Longitude:    longitude,
			}

			// Store the answer using the new global map
			models.AddTpingAnswer(data.GPS, data.Address, answer)
		}
	}
}

// ParseCoordinate parses a coordinate string to a float64
func parseCoordinate(coord string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(coord), 64)
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
