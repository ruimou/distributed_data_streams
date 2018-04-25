package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"./lib/consumer"
)

func main() {
	fmt.Println("This program connects to the data service as well as to an internal port for")
	fmt.Println("sending data to a webserver.")
	fmt.Println("Usage: go run <file> <serv-ip>:<serv-port> <internal-ip:internal-port>")

	internalConn, err := net.Dial("tcp", os.Args[2])
	if err != nil {
		fmt.Println("Could not connect to internal:", err)
	}

	topicName := "ubc"
	rSess, err := consumer.GetTopic(topicName, os.Args[1], fmt.Sprintf("Read Id: %d", 1))
	if err != nil {
		fmt.Printf("Could not get ReadSession for Topic: [%s]\n", topicName)
		return
	}

	for {
		points, err := rSess.Read()

		if err != nil {
			fmt.Printf("Could not read")
			log.Println(err)
			time.Sleep(10 * time.Second)
			continue
		}

		for _, point := range points {
			internalConn.Write([]byte(point))
		}
		break
	}

	// Terminal, when user presses read, read it again
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Type: 'read' if you want to read the updated data")
		text, _ := reader.ReadString('\n')

		if text != "read\n" {
			fmt.Printf("Could not understand input: [%s]. \nPlease text 'read' if you wish you read the latest data\n\n", text)
			continue
		}

		points, err := rSess.Read()

		if err != nil {
			log.Println(err)
			continue
		}

		for _, point := range points {
			internalConn.Write([]byte(point))
		}
	}
}
