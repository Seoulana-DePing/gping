package models

import (
	"encoding/json"
	"sync"
	"time"
)

// LocationRequest is the request structure for the get_location RPC method
type LocationRequest struct {
	IP string `json:"ip"`
}

// LocationResponse is the response structure for the get_location RPC method
type LocationResponse struct {
	Location string `json:"location"`
	Vault    string `json:"vault"` // Vault contract address
}

// TpingData represents the data sent by Tpings
type TpingData struct {
	GPS     string `json:"gps"`     // GPS information
	Time    uint   `json:"time"`    // Response time
	Address string `json:"address"` // Solana address of the Tping
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
	URL     string `json:"url"`
	Address string `json:"address"`
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
