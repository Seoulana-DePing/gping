package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/Seoulana-DePing/gping/internal/config"
	"github.com/Seoulana-DePing/gping/internal/models"
	"github.com/gin-gonic/gin"
)

// RESTHandler handles REST API requests
type RESTHandler struct {
	config   *config.Config
	handler  *Handler
	restPort int
}

// NewRESTHandler creates a new REST API handler
func NewRESTHandler(cfg *config.Config, handler *Handler) *RESTHandler {
	// REST API port is WebSocket port + 1
	restPort := cfg.Server.Port + 1
	return &RESTHandler{
		config:   cfg,
		handler:  handler,
		restPort: restPort,
	}
}

// SetupRoutes sets up HTTP routes for the REST API
func (r *RESTHandler) SetupRoutes(router *gin.Engine) {
	// Health check endpoint
	router.GET("/health", r.handleHealth)

	// Health check endpoint with just status 200 and no response body
	router.GET("/health-check", r.handleHealthCheck)

	// Polling endpoint to get the latest requested IP
	router.GET("/polling", r.handlePolling)

	// Result endpoint to submit Tping data
	router.POST("/result", r.handleResult)

	// API group with v1 prefix
	v1 := router.Group("/api/v1")
	{
		// Location-related endpoints
		v1.POST("/location", r.handleGetLocation)
		v1.POST("/guess", r.handleGuessLocation)

		// Node information endpoint
		v1.GET("/node/info", r.handleNodeInfo)
	}
}

// HandleHealth handles the health check endpoint
func (r *RESTHandler) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "gping-rest-api",
	})
}

// HandleHealthCheck handles the health-check endpoint with just status 200
func (r *RESTHandler) handleHealthCheck(c *gin.Context) {
	c.Status(http.StatusOK)
}

// HandleGetLocation handles the get_location API endpoint
func (r *RESTHandler) handleGetLocation(c *gin.Context) {
	var req models.LocationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request format",
		})
		return
	}

	// Add the IP to the requested IPs global variable for polling
	models.AddRequestedIP(req.IP)

	// Check if we already have an answer for this IP
	if location, exists := models.GetAnswer(req.IP); exists {

		c.JSON(http.StatusOK, gin.H{
			"ip":         req.IP,
			"location":   location,
			"latitude":   location.Latitude,
			"longitude":  location.Longitude,
			"status":     "success",
			"cached":     true,
			"request_id": req.RequestId,
		})
		log.Printf("Responded with cached location for IP: %s = %s", req.IP, location)
		return
	}

	// Start a new goroutine to process the location request if not already processing
	if !models.MarkIPAsProcessing(req.IP) {
		// This IP is already being processed
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":      "Request for this IP is already being processed",
			"request_id": req.RequestId,
		})
		return
	}

	// Use the new location request handler for consistency
	go r.handler.newProcessLocationRequest(req.IP, req.RequestId)

	// Respond immediately to the client
	c.JSON(http.StatusAccepted, gin.H{
		"status":     "processing",
		"message":    "Location request is being processed",
		"request_id": req.RequestId,
		"ip":         req.IP,
	})
}

// HandleGuessLocation handles the guess_location API endpoint
func (r *RESTHandler) handleGuessLocation(c *gin.Context) {
	var data models.TpingData
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request format",
		})
		return
	}

	// Validate the Tping address
	if !r.handler.IsTpingAuthorized(data.Address) {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Unauthorized Tping address",
		})
		return
	}

	// Store the data (need to make this method accessible)
	r.handler.StoreTpingData(data)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Data received",
	})
}

// HandleNodeInfo returns information about the node
func (r *RESTHandler) handleNodeInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"address":     r.config.Key.Address,
		"is_proposal": r.config.Server.IsProposal,
		"ws_port":     r.config.Server.Port,
		"rest_port":   r.restPort,
		"version":     "1.0.0", // Replace with actual version
	})
}

// HandlePolling handles the polling endpoint
func (r *RESTHandler) handlePolling(c *gin.Context) {
	// Get the latest requested IPs
	ips := models.GetRequestedIPs()

	// Return the IPs as JSON
	if len(ips) > 0 {
		c.JSON(http.StatusOK, gin.H{
			"ip": ips[0], // Return the first IP for simplicity
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"ip": "",
		})
	}
}

// HandleResult handles the result endpoint
func (r *RESTHandler) handleResult(c *gin.Context) {
	var req models.TpingResultRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request format",
		})
		return
	}

	// Create a TpingAnswer from the request
	answer := models.TpingAnswer{
		ResponseTime: req.ResponseTime,
		Latitude:     req.Latitude,
		Longitude:    req.Longitude,
	}

	// Store the Tping answer
	models.AddTpingAnswer(req.IP, req.WalletAddress, answer)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Result received",
	})
}

// StartServer starts the REST API server
func (r *RESTHandler) StartServer(ctx context.Context) error {
	// Set Gin to release mode in production
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	// Use logger and recovery middleware
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	// Setup routes
	r.SetupRoutes(router)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", r.restPort),
		Handler: router,
	}

	// Start the server
	go func() {
		log.Printf("REST API server started on port %d", r.restPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting REST API server: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Shutdown the server
	return server.Shutdown(context.Background())
}
