package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"

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
	Name              string `json:"name" binding:"required"`
	Partitions        int    `json:"partitions" binding:"required"`
	ReplicationFactor int    `json:"replicationFactor" binding:"required"`
}

type ProduceMessageRequest struct {
	Topic   string            `json:"topic" binding:"required"`
	Key     string            `json:"key"`
	Value   string            `json:"value" binding:"required"`
	Headers map[string]string `json:"headers"`
}

func main() {
	// Initialize the synchronous Kafka producer client
	kafka.InitProducer()
	defer func() {
		if err := kafka.CloseProducer(); err != nil {
			log.Printf("Error closing Kafka producer: %v", err)
		}
	}()

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
		// Produce: publish messages
		api.POST("/produce", handleProduce)
		// Consume WS: stream messages in real-time
		api.GET("/consume/ws", handleConsumeWS)
		// Consumer groups: rebalancing visualization
		api.GET("/consumers", handleListConsumers)
		api.GET("/consumers/groups", handleListConsumerGroups)
		api.GET("/consumers/groups/:groupId", handleDescribeConsumerGroup)
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

	c.JSON(http.StatusCreated, gin.H{"status": "Topic created successfully"})
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
