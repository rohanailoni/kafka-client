package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {

	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Println("Error connecting to Kafka broker:", err)
		return
	}
	defer conn.Close()

	request := buildMetadataRequest()
	sendRequest(conn, request)

	response := readResponse(conn)
	partitions := parseMetadataResponse(response)

	for topic, partitionList := range partitions {
		fmt.Printf("Topic: %s, Partitions: %v\n", topic, partitionList)
	}
}

func buildMetadataRequest() []byte {

	request := []byte{
		0x00, 0x00, 0x00, 0x2d, // Request size
		0x00, 0x03, 0x00, 0x00, // API key: MetadataRequest
		0x00, 0x00, 0x00, 0x00, // API version
		0x00, 0x00, 0x00, 0x00, // Correlation ID
		0x00, 0x00, 0x00, 0x01, // Client ID length
		0x00,                   // Empty client ID
		0x00, 0x00, 0x00, 0x00, // No topics
	}

	return request
}

func sendRequest(conn net.Conn, request []byte) {

	_, err := conn.Write(request)
	if err != nil {
		fmt.Println("Error sending request:", err)
	}
}

func readResponse(conn net.Conn) []byte {

	reader := bufio.NewReader(conn)
	response, err := reader.ReadBytes(0)
	if err != nil {
		fmt.Println("Error reading response:", err)
	}
	return response[:len(response)-1]
}

func parseMetadataResponse(response []byte) map[string][]int32 {

	partitions := make(map[string][]int32)

	return partitions
}
