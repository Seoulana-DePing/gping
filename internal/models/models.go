package models

import (
	"encoding/json"
	"sync"
	"time"
)

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
	GPS     string `json:"gps"`     // GPS information
	Time    uint   `json:"time"`    // Response time
	Address string `json:"address"` // Solana address of the Tping
}

// TpingAnswer represents the answer from a Tping node with detailed location info
type TpingAnswer struct {
	ResponseTime float64 `json:"response_time"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
}

// TpingResultRequest represents the POST request body for the /result endpoint
type TpingResultRequest struct {
	WalletAddress string  `json:"wallet_address"`
	ResponseTime  float64 `json:"response_time"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	IP            string  `json:"ip"`
}

// SignedMessage represents a signed message in the P2P network
type SignedMessage struct {
	Message   json.RawMessage `json:"message"`   // The marshaled message
	Signature string          `json:"signature"` // The signature of the message
}

// GpingNode represents a Gping node in the P2P network
type GpingNode struct {
	URL     string // URL of the Gping node
	Address string // Solana address of the Gping node
}

// ProposalInfo represents the proposal information for the P2P network
type ProposalInfo struct {
	Address    string `json:"address"`     // Solana address of the proposal node
	IsProposal bool   `json:"is_proposal"` // Flag to identify this is a proposal message
}

// RPCMessage represents a WebSocket RPC message
type RPCMessage struct {
	ID     string          `json:"id"`               // Message ID for request/response matching
	Method string          `json:"method"`           // RPC method name
	Params json.RawMessage `json:"params,omitempty"` // Parameters for the method
	Result json.RawMessage `json:"result,omitempty"` // Result of the method call
	Error  *RPCError       `json:"error,omitempty"`  // Error information if any
}

// RPCError represents an error in a WebSocket RPC message
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Global variables for storing location data and Tping withdrawal information
var (
	// Answers maps IP addresses to their determined locations
	Answers = struct {
		sync.RWMutex
		Data map[string]string // map[ipAddress]gpsLocation
	}{Data: make(map[string]string)}

	// TpingWithdrawal stores information about Tpings that contributed data
	// map[ipAddress][timestamp][]tpingAddresses
	TpingWithdrawal = struct {
		sync.RWMutex
		Data map[string]map[uint][]string
	}{Data: make(map[string]map[uint][]string)}

	// ProcessingIPs keeps track of IPs currently being processed
	ProcessingIPs = struct {
		sync.RWMutex
		Data map[string]bool
	}{Data: make(map[string]bool)}

	// RequestedIPs keeps track of all requested IPs for polling
	RequestedIPs = struct {
		sync.RWMutex
		Data map[string]bool
	}{Data: make(map[string]bool)}

	// TpingAnswers stores Tping answers in the format map[ip][tping wallet address] = {ResponseTime, Latitude, Longitude}
	TpingAnswers = struct {
		sync.RWMutex
		Data map[string]map[string]TpingAnswer
	}{Data: make(map[string]map[string]TpingAnswer)}
)

// StoreAnswer stores a determined location for an IP address
func StoreAnswer(ip, location string) {
	Answers.Lock()
	defer Answers.Unlock()
	Answers.Data[ip] = location
}

// GetAnswer retrieves a stored location for an IP address
func GetAnswer(ip string) (string, bool) {
	Answers.RLock()
	defer Answers.RUnlock()
	location, exists := Answers.Data[ip]
	return location, exists
}

// StoreTpingData stores information about Tpings that contributed data for a specific IP
func StoreTpingData(ip string, tpingAddresses []string) {
	timestamp := uint(time.Now().Unix())

	TpingWithdrawal.Lock()
	defer TpingWithdrawal.Unlock()

	if _, exists := TpingWithdrawal.Data[ip]; !exists {
		TpingWithdrawal.Data[ip] = make(map[uint][]string)
	}

	TpingWithdrawal.Data[ip][timestamp] = tpingAddresses
}

// MarkIPAsProcessing marks an IP as currently being processed
func MarkIPAsProcessing(ip string) bool {
	ProcessingIPs.Lock()
	defer ProcessingIPs.Unlock()

	if ProcessingIPs.Data[ip] {
		return false // Already processing
	}

	ProcessingIPs.Data[ip] = true
	return true
}

// UnmarkIPAsProcessing removes an IP from the processing state
func UnmarkIPAsProcessing(ip string) {
	ProcessingIPs.Lock()
	defer ProcessingIPs.Unlock()
	delete(ProcessingIPs.Data, ip)
}

// AddRequestedIP adds an IP to the requested IPs map for polling
func AddRequestedIP(ip string) {
	RequestedIPs.Lock()
	defer RequestedIPs.Unlock()
	RequestedIPs.Data[ip] = true
}

// GetRequestedIPs returns all requested IPs for polling
func GetRequestedIPs() []string {
	RequestedIPs.RLock()
	defer RequestedIPs.RUnlock()

	ips := make([]string, 0, len(RequestedIPs.Data))
	for ip := range RequestedIPs.Data {
		ips = append(ips, ip)
	}
	return ips
}

// AddTpingAnswer adds a Tping answer for an IP
func AddTpingAnswer(ip, walletAddress string, answer TpingAnswer) {
	TpingAnswers.Lock()
	defer TpingAnswers.Unlock()

	if _, exists := TpingAnswers.Data[ip]; !exists {
		TpingAnswers.Data[ip] = make(map[string]TpingAnswer)
	}
	TpingAnswers.Data[ip][walletAddress] = answer
}

// GetTpingAnswers returns all Tping answers for an IP
func GetTpingAnswers(ip string) map[string]TpingAnswer {
	TpingAnswers.RLock()
	defer TpingAnswers.RUnlock()

	if answers, exists := TpingAnswers.Data[ip]; exists {
		// Create a copy to avoid concurrent map access issues
		result := make(map[string]TpingAnswer)
		for k, v := range answers {
			result[k] = v
		}
		return result
	}
	return make(map[string]TpingAnswer)
}

// GetBestTpingAnswer returns the best (lowest response time) Tping answer for an IP
func GetBestTpingAnswer(ip string) (TpingAnswer, bool) {
	TpingAnswers.RLock()
	defer TpingAnswers.RUnlock()

	if answers, exists := TpingAnswers.Data[ip]; exists && len(answers) > 0 {
		var bestAnswer TpingAnswer
		bestTime := float64(9999999)
		found := false

		for _, answer := range answers {
			if !found || answer.ResponseTime < bestTime {
				bestAnswer = answer
				bestTime = answer.ResponseTime
				found = true
			}
		}

		return bestAnswer, found
	}
	return TpingAnswer{}, false
}

// CountTpingAnswers returns the count of Tping answers for an IP
func CountTpingAnswers(ip string) int {
	TpingAnswers.RLock()
	defer TpingAnswers.RUnlock()

	if answers, exists := TpingAnswers.Data[ip]; exists {
		return len(answers)
	}
	return 0
}
