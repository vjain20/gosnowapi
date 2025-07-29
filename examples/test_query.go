package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/vjain20/gosnowapi/snowapi"
)

func main() {
	// Load PEM keys from files (or use env vars / other secrets mechanism)
	privKeyPEM, err := os.ReadFile("testdata/rsa_key.p8")
	if err != nil {
		log.Fatalf("Failed to read private key: %v", err)
	}

	pubKeyPEM, err := os.ReadFile("testdata/rsa_key.pub")
	if err != nil {
		log.Fatalf("Failed to read public key: %v", err)
	}

	// Create client config
	cfg := snowapi.Config{
		Account:     "CXEEZLW-JQB53549",
		User:        "VJAIN27",
		PrivateKey:  privKeyPEM,
		PublicKey:   pubKeyPEM,
		ExpireAfter: 2 * time.Minute,
		HTTPTimeout: 10 * time.Second,
	}

	// Create client
	client, err := snowapi.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Run a simple query
	result, err := client.Query("SELECT CURRENT_TIMESTAMP()")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Println("Query Result:")
	for _, row := range result {
		fmt.Printf(" - %v\n", row)
	}
}
