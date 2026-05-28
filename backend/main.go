package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/danwa/kafka-playground/backend/kafka"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow any origin for local development playground
		return true
	},
}

type CreateTopicRequest struct {
	Name              string            `json:"name" binding:"required"`
	Partitions        int               `json:"partitions" binding:"required"`
	ReplicationFactor int               `json:"replicationFactor" binding:"required"`
	Configs           map[string]string `json:"configs"`
}

type ProduceMessageRequest struct {
	Topic   string            `json:"topic" binding:"required"`
	Key     string            `json:"key"`
	Value   string            `json:"value" binding:"required"`
	Headers map[string]string `json:"headers"`
}

type BatchProduceRequest struct {
	Topic       string `json:"topic" binding:"required"`
	Count       int    `json:"count" binding:"required"`
	BatchSize   int    `json:"batchSize"`
	Compression string `json:"compression"` // "none", "gzip", "snappy", "lz4"
	Acks        string `json:"acks"`        // "0", "1", "all"
}

func main() {
	// Initialize the synchronous Kafka producer client
	kafka.InitProducer()
	defer func() {
		if err := kafka.CloseProducer(); err != nil {
			log.Printf("Error closing Kafka producer: %v", err)
		}
	}()

	// Ensure a default topic exists so the playground is usable out of the box
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := kafka.CreateTopic(ctx, "playground-events", 3, 3); err != nil {
		log.Printf("Note: could not create default topic (may already exist): %v", err)
	}

	r := gin.Default()

	// Simple CORS middleware
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	api := r.Group("/api")
	{
		// Metadata: inspect active brokers, topics, and partitions
		api.GET("/metadata", handleMetadata)
		// Topics: create or delete Kafka topics
		api.POST("/topics", handleCreateTopic)
		api.DELETE("/topics/:name", handleDeleteTopic)
		api.POST("/topics/:name/config", handleSetTopicConfig)
		api.GET("/topics/:name/config", handleGetTopicConfig)
		// Produce: publish messages
		api.POST("/produce", handleProduce)
		api.POST("/produce/batch", handleBatchProduce)
		// Consume WS: stream messages in real-time
		api.GET("/consume/ws", handleConsumeWS)
		// Consumer groups: rebalancing visualization
		api.GET("/consumers", handleListConsumers)
		api.GET("/consumers/groups", handleListConsumerGroups)
		api.GET("/consumers/groups/:groupId", handleDescribeConsumerGroup)
		// Offset management: rewind / reset offsets
		api.POST("/consumers/groups/:groupId/reset", handleResetOffsets)
		// Stress test: produce messages rapidly to observe lag
		api.POST("/stress/start", handleStartStress)
		api.POST("/stress/stop", handleStopStress)
		api.GET("/stress", handleGetStressStatus)
	}

	log.Println("Starting Go Kafka backend on :8081...")
	if err := r.Run(":8081"); err != nil {
		log.Fatalf("Failed to run HTTP server: %v", err)
	}
}

func handleMetadata(c *gin.Context) {
	meta, err := kafka.GetClusterMetadata(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, meta)
}

func handleCreateTopic(c *gin.Context) {
	var req CreateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := kafka.CreateTopic(c.Request.Context(), req.Name, req.Partitions, req.ReplicationFactor)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Apply custom configs (retention, compaction, etc.) if specified
	if len(req.Configs) > 0 {
		if err := kafka.SetTopicConfig(c.Request.Context(), req.Name, req.Configs); err != nil {
			log.Printf("Warning: failed to set topic config for %s: %v", req.Name, err)
		}
	}

	c.JSON(http.StatusCreated, gin.H{"status": "Topic created successfully"})
}

func handleSetTopicConfig(c *gin.Context) {
	name := c.Param("name")
	var req map[string]string
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := kafka.SetTopicConfig(c.Request.Context(), name, req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "Config updated"})
}

func handleGetTopicConfig(c *gin.Context) {
	name := c.Param("name")
	configs, err := kafka.DescribeTopicConfig(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"configs": configs})
}

func handleDeleteTopic(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic name is required"})
		return
	}

	err := kafka.DeleteTopic(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "Topic deleted successfully"})
}

func handleProduce(c *gin.Context) {
	var req ProduceMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	partition, offset, err := kafka.PublishMessage(
		c.Request.Context(),
		req.Topic,
		req.Key,
		req.Value,
		req.Headers,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "Message published",
		"partition": partition,
		"offset":    offset,
	})
}

func handleBatchProduce(c *gin.Context) {
	var req BatchProduceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.BatchSize <= 0 {
		req.BatchSize = 1
	}
	if req.Compression == "" {
		req.Compression = "none"
	}
	if req.Acks == "" {
		req.Acks = "1"
	}

	result, err := kafka.BatchProduce(c.Request.Context(), req.Topic, req.Count, req.BatchSize, req.Compression, req.Acks)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func handleConsumeWS(c *gin.Context) {
	topic := c.Query("topic")
	groupID := c.Query("groupId")
	offset := c.Query("offset")

	if topic == "" || groupID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic and groupId are required query params"})
		return
	}

	consumerID := fmt.Sprintf("consumer-%s-%d", groupID[:min(8, len(groupID))], rand.Intn(9999))
	fromBeginning := offset == "earliest"

	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer ws.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register consumer for rebalancing visualization
	kafka.RegisterConsumer(consumerID, groupID, topic)
	defer kafka.UnregisterConsumer(consumerID)

	// Send consumer ID to the client
	ws.WriteJSON(map[string]string{"type": "connected", "consumerId": consumerID})

	// Connection monitor: if client closes connection, trigger context cancellation.
	go func() {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	eventChan := make(chan kafka.ConsumerEvent, 100)

	// Stream messages from Kafka into our channel
	go func() {
		err := kafka.StreamMessages(ctx, topic, groupID, fromBeginning, eventChan)
		if err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
		close(eventChan)
	}()

	// Read from channel and push to client WebSocket
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-eventChan:
			if !ok {
				return
			}
			err := ws.WriteJSON(event)
			if err != nil {
				log.Printf("WebSocket send failed: %v", err)
				return
			}
		}
	}
}

func handleListConsumers(c *gin.Context) {
	members := kafka.GetRegisteredConsumers()
	c.JSON(http.StatusOK, gin.H{"consumers": members})
}

func handleListConsumerGroups(c *gin.Context) {
	groups, err := kafka.ListConsumerGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"groups": groups})
}

func handleDescribeConsumerGroup(c *gin.Context) {
	groupID := c.Param("groupId")
	view, err := kafka.DescribeConsumerGroup(groupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, view)
}

type ResetOffsetsRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Target    string `json:"target"` // "earliest" or "latest"
}

func handleResetOffsets(c *gin.Context) {
	groupID := c.Param("groupId")

	var req ResetOffsetsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var err error
	if req.Partition >= 0 && req.Offset >= 0 {
		err = kafka.ResetConsumerGroupOffsetToSpecific(groupID, req.Topic, req.Partition, req.Offset)
	} else {
		target := kafka.ResetToEarliest
		if req.Target == "latest" {
			target = kafka.ResetToLatest
		}
		err = kafka.ResetConsumerGroupOffset(groupID, req.Topic, target)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "Offsets reset successfully"})
}

type StartStressRequest struct {
	Topic     string `json:"topic" binding:"required"`
	RatePerSec int   `json:"ratePerSec"`
}

func handleStartStress(c *gin.Context) {
	var req StartStressRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.RatePerSec <= 0 {
		req.RatePerSec = 50
	}

	if err := kafka.StartStressTest(req.Topic, req.RatePerSec); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "Stress test started"})
}

func handleStopStress(c *gin.Context) {
	kafka.StopStressTest()
	c.JSON(http.StatusOK, gin.H{"status": "Stress test stopped"})
}

func handleGetStressStatus(c *gin.Context) {
	status := kafka.GetStressTestStatus()
	c.JSON(http.StatusOK, status)
}
