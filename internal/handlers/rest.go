package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

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
	// Health check endpoint with just status 200 and no response body
	router.GET("/health-check", r.handleHealthCheck)

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

	// Check if we already have an answer for this IP
	if location, exists := models.GetAnswer(req.IP); exists {
		parts := strings.Split(location, ",")
		var latitude, longitude string
		if len(parts) >= 2 {
			latitude = strings.TrimSpace(parts[0])
			longitude = strings.TrimSpace(parts[1])
		}

		c.JSON(http.StatusOK, gin.H{
			"ip":         req.IP,
			"location":   location,
			"latitude":   latitude,
			"longitude":  longitude,
			"status":     "success",
			"cached":     true,
			"request_id": req.RequestId,
		})
		return
	}

	// Start a new goroutine to process the location request
	if !models.MarkIPAsProcessing(req.IP) {
		// This IP is already being processed
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":      "Request for this IP is already being processed",
			"request_id": req.RequestId,
		})
		return
	}

	go r.handler.ProcessLocationRequest(req.IP)

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
