package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type Message struct {
	Text      string
	Leader    bool // true if message from client, false otherwise
	Timestamp int64
	ServerResponse bool // server responds to leader server
}

var connection net.Conn
var currentTime int64

// func inOutMessages(sendMessage chan Message, receiveMessage chan Message) {
// 	go outgoingMessages(sendMessage)
// 	go incomingMessages(receiveMessage)
// }

func incomingMessages(receiveMessage chan Message) {
	var response Message
	decoder := gob.NewDecoder(connection)
	err := decoder.Decode(&response)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error decoding response")
		return
	}

	receiveMessage <- response
	// fmt.Println("Received message")
	fmt.Println(response.Text)

	if strings.Contains(response.Text, "COMMIT") || strings.Contains(response.Text, "ABORTED") {
		connection.Close()
		os.Exit(0)
	}
}

func outgoingMessages(sendMessage chan Message) {
	encoder := gob.NewEncoder(connection)
	message := <- sendMessage
	err := encoder.Encode(message)
	if err != nil {
		// fmt.Println("Error encoding message HERE", err)
		return
	}

	// fmt.Println("Message sent to server")
}

func main() {
	args := os.Args
	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "Right format: ./client id config.txt")
		return
	}

	config_file := args[2]

	outsideTransaction := false
	sendMessage := make(chan Message, 200)
	receiveMessage := make(chan Message, 200)
	// currentTime := time.Now().UnixNano()

	// need to randomly select a server to be the coordinator
	file, err := os.ReadFile(config_file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't open config file")
	}
	fileString := string(file)
	eachLine := strings.Split(fileString, "\n")
	
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		
		command := scanner.Text()
		//reader := bufio.NewReader(os.Stdin)
		//bytes_read, err := reader.ReadString('\n')
		//fmt.Println(bytes_read)
		command = strings.TrimRight(command, "\n")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Reading from Stdin error: %v\n", err)
		}

		if command == "BEGIN" {
			currentTime = time.Now().UnixNano()
			outsideTransaction = true
			// pick a random server
			serverIndex := rand.Intn(len(eachLine))
			randServer := strings.Fields(eachLine[serverIndex])
			address := randServer[1]
			port := randServer[2]
					
			msg := Message{}
			msg.Text = command
			msg.Leader = true
			msg.Timestamp = currentTime
			msg.ServerResponse = false
			sendMessage <- msg
			connection, err = net.Dial("tcp", address + ":" + port)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Couldn't connect to server")
				return
			}
			defer connection.Close()

			// go inOutMessages(sendMessage, receiveMessage)
			outgoingMessages(sendMessage)
			incomingMessages(receiveMessage)

			command = ""
			
		} else if outsideTransaction == false {
			continue	// skip command since transaction hasn't started yet
		} else { // else if strings.Contains(command, "DEPOSIT") || strings.Contains(command, "WITHDRAW")
			msg := Message{}
			msg.Text = command
			msg.Leader = true
			msg.Timestamp = currentTime
			msg.ServerResponse = false
			sendMessage <- msg
			
			// go inOutMessages(sendMessage, receiveMessage)
			outgoingMessages(sendMessage)
			incomingMessages(receiveMessage)
			
			command = ""
		} // else if strings.Contains(command, "BALANCE")
	}
}