// Set all ServerResponse to false?

// TODO: Handle response from server to send to client

package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	// "time"
)

type Message struct {
	Text           string
	Leader         bool // true if message from client, false otherwise
	Timestamp      int64
	ServerResponse bool // server responds to leader server
}

type Server struct {
	Id             string
	Address        string
	Port           string
	ReceiveMessage chan Message
	SendMessage    chan Message
	Leader         bool
	Accounts       map[string]SafeAccount
	NumResponses   map[int64]int
}

// For example, if this was Server A and we tried to add an account named "foo", "foo" would have this struct
type Account struct {
	CommittedValue     int
	CommittedTimestamp int64
	RTS                []int64
	// TW                 []TentativeWrite
	TW map[int64]int // timestamp -> value
}

type SafeAccount struct {
	acc *Account
	m  *sync.Mutex
	c  *sync.Cond
}

var node Server
var connections map[string]net.Conn = make(map[string]net.Conn) // sending connections
var clientConnections map[int64]net.Conn = make(map[int64]net.Conn)
var duck sync.Mutex = sync.Mutex{}

func initiateConnections(fileString string) {
	eachLine := strings.Split(fileString, "\n")

	// making all connections
	for i := 0; i < 5; i++ {
		server := strings.Fields(eachLine[i])
		address := server[1]
		port := server[2]
		var err error = nil
		duck.Lock()
		connections[server[0]], err = net.Dial("tcp", address+":"+port)
		duck.Unlock()
		for err != nil {
			duck.Lock()
			connections[server[0]], err = net.Dial("tcp", address+":"+port)
			duck.Unlock()
		}
	}
}

func receiveConnections() {
	listen, err := net.Listen("tcp", ":"+node.Port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen - Listen error")
		return
	}

	defer listen.Close()

	for {
		connection, err := listen.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept failure")
			return
		}

		go handleMessages(connection)
	}
}

// client ensures only one command from a transaction at a time
func handleMessages(connection net.Conn) {
	for {
		msg := Message{}
		decoder := gob.NewDecoder(connection)
		err := decoder.Decode(&msg)
		if err != nil {
			// fmt.Fprintf(os.Stderr, "Error decoding message HANDLE")
			return
		}

		// fmt.Println("Received message: ", msg.Text)
		// node.Leader = msg.Leader

		if msg.Text == "BEGIN" {
			// start a transaction - need to do that
			//sendMessage := make(chan Message, 200)
			clientConnections[msg.Timestamp] = connection
			encoder := gob.NewEncoder(connection)
			errs := encoder.Encode(Message{"OK", false, msg.Timestamp, false})
			if errs != nil {
				// fmt.Fprintf(os.Stderr, "Error encoding response")
				return
			}
			// fmt.Println("Response sent to client")
			//node.ReceiveMessage <- msg
			//node.SendMessage <- Message{"OK", false}

		} else if strings.Contains(msg.Text, "DEPOSIT") || strings.Contains(msg.Text, "WITHDRAW") {
			if msg.Leader == true {
				// send to non-leader node that the transaction is meant for
				clientConnections[msg.Timestamp] = connection
				node.SendMessage <- msg
				go sendToServer(msg.Text)
			} else {
				node.ReceiveMessage <- msg
				go read_write(connection)
				// node.ReceiveMessage <- msg
				// write(connection) // write rule
			}
		} else if strings.Contains(msg.Text, "BALANCE") {
			if msg.Leader == true {
				clientConnections[msg.Timestamp] = connection
				node.SendMessage <- msg
				go sendToServer(msg.Text)
			} else {
				node.ReceiveMessage <- msg
				go read(connection)
			}
		} else if strings.Contains(msg.Text, "COMMIT") {
			node.ReceiveMessage <- msg
			twoPC()
		} else if strings.Contains(msg.Text, "PREPARE") {
			node.ReceiveMessage <- msg
			checkCommitVal(connection)
		} else if strings.Contains(msg.Text, "GOOD") {
			node.ReceiveMessage <- msg
			finishCommit()
		} else if strings.Contains(msg.Text, "ABORT") {
			node.ReceiveMessage <- msg
			encoder := gob.NewEncoder(connection)
			errs := encoder.Encode(Message{"ABORTED", false, msg.Timestamp, false})
			if errs != nil {
				// fmt.Fprintf(os.Stderr, "Error encoding response")
				return
			}
			// fmt.Println("Response sent to client")
			aborting()
		} else if strings.Contains(msg.Text, "DELETE") {
			for key := range node.Accounts {
				editedAccount := false

				node.Accounts[key].m.Lock()
				_, ok := node.Accounts[key].acc.TW[msg.Timestamp]
				if ok {
					delete(node.Accounts[key].acc.TW, msg.Timestamp)
					editedAccount = true
				} 
				
				indexToDelete := -1
				for i, v := range node.Accounts[key].acc.RTS {
					if v == msg.Timestamp {
						indexToDelete = i
						break
					}
				}

				if indexToDelete != -1 {
					node.Accounts[key].acc.RTS = append(node.Accounts[key].acc.RTS[:indexToDelete], node.Accounts[key].acc.RTS[indexToDelete+1:]...)
					editedAccount = true
				}

				node.Accounts[key].m.Unlock()

				// node.Accounts[key].acc = accountDetails
				// fmt.Println(node.Accounts[key].acc)
				if editedAccount {
					node.Accounts[key].c.L.Lock()
					node.Accounts[key].c.Broadcast()
					node.Accounts[key].c.L.Unlock()
				}
			}
		}
	}
}

func aborting() {
	messages := <-node.ReceiveMessage

	for key := range connections {
		duck.Lock()
		temp := connections[key]
		duck.Unlock()
		sendAbort(temp, messages)
	}
}

func sendAbort(connection net.Conn, messages Message) {
	messages.Text = "DELETE"
	messages.Leader = false
	encoder := gob.NewEncoder(connection)
	err := encoder.Encode(messages)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error encoding message")
		return
	}
}

func finishCommit() {
	message := <-node.ReceiveMessage
	for key := range node.Accounts {
		// accountDetails := Account{}
		// accountDetails := node.Accounts[key].acc
		editedAccount := false

		// fmt.Println("Gets here")
		node.Accounts[key].m.Lock()
		if value, ok := node.Accounts[key].acc.TW[message.Timestamp]; ok {
			node.Accounts[key].acc.CommittedValue = value
			node.Accounts[key].acc.CommittedTimestamp = message.Timestamp
			delete(node.Accounts[key].acc.TW, message.Timestamp)
			editedAccount = true
		}

		indexToDelete := -1
		for i, v := range node.Accounts[key].acc.RTS {
			if v == message.Timestamp {
				indexToDelete = i
				break
			}
		}

		if indexToDelete != -1 {
			node.Accounts[key].acc.RTS = append(node.Accounts[key].acc.RTS[:indexToDelete], node.Accounts[key].acc.RTS[indexToDelete+1:]...)
			editedAccount = true
		}

		node.Accounts[key].m.Unlock()

		// node.Accounts[key].acc = accountDetails
		// fmt.Println(node.Accounts[key])
		if editedAccount {
			node.Accounts[key].c.L.Lock()
			node.Accounts[key].c.Broadcast()
			node.Accounts[key].c.L.Unlock()
		}
	}

	for key := range node.Accounts {
		node.Accounts[key].m.Lock()
		if node.Accounts[key].acc.CommittedTimestamp != 0 && node.Accounts[key].acc.CommittedValue != 0 {
			fmt.Println(key, ":", node.Accounts[key].acc.CommittedValue)
		}
		node.Accounts[key].m.Unlock()
	}
}

func checkCommitVal(connection net.Conn) {
	var check int
	check = 0
	messages := <-node.ReceiveMessage
	for key := range node.Accounts { // go through all the accounts in the server
		node.Accounts[key].m.Lock()
		if value, ok := node.Accounts[key].acc.TW[messages.Timestamp]; ok { // if the timestamp exists in TW, meaning we tried to write it
			if value >= 0 { // check if the value would be negative
				// check if any smaller uncommitted transactions
				// ks := make([]int64, 0)
				// for k := range node.Accounts {
				// 	append(ks, k)
				// }
				// sort.Slice(ks, func(i, j int) bool { // sorting it in increasing order since rtsCheck needs RTS list to be ordered
				// 	return ks[i] < ks[j]
				// })
				for {
					isFirst := true
					for k := range node.Accounts[key].acc.TW {
						if k < messages.Timestamp {
							isFirst = false

							node.Accounts[key].m.Unlock()
	
							node.Accounts[key].c.L.Lock()
							node.Accounts[key].c.Wait()
							node.Accounts[key].c.L.Unlock()

							node.Accounts[key].m.Lock()
							break
						}
					}
					if isFirst {
						break
					}
				}
				check = 1
			} else {
				check = 2
				node.Accounts[key].m.Unlock()
				break
			}
		}
		node.Accounts[key].m.Unlock()
	}

	if check == 0 || check == 1 {
		messages.Text = "YES"
		messages.Leader = true
	} else {
		messages.Text = "NO"
		messages.Leader = true
	}

	encoder := gob.NewEncoder(connection)
	errs := encoder.Encode(messages)
	if errs != nil {
		// fmt.Fprintf(os.Stderr, "Error encoding message")
		return
	}
}

func twoPC() {
	messages := <-node.ReceiveMessage

	for key := range connections {
		if messages.Text == "COMMIT" {
			duck.Lock()
			temp := connections[key]
			duck.Unlock()
			srMessage(temp, messages)
		} else {
			duck.Lock()
			temp := connections[key]
			duck.Unlock()
			committing(temp, messages)
		}
	}
}

func committing(connection net.Conn, messages Message) {
	encoder := gob.NewEncoder(connection)
	err := encoder.Encode(messages)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error encoding")
		return
	}
}

func srMessage(connection net.Conn, messages Message) {
	prepare(connection, messages)
	receiveInitialResponse(connection)
}

func receiveInitialResponse(connection net.Conn) {
	var response Message
	decoder := gob.NewDecoder(connection)
	err := decoder.Decode(&response)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error decoding message")
		return
	}

	// fmt.Println("Received message", response.Text)
	node.NumResponses[response.Timestamp] += 1

	if response.Text == "NO" {
		// ABORT
		// fmt.Println("NEED TO ABORT")
		node.NumResponses[response.Timestamp] -= 1

		var newResponse Message
		newResponse.Text = "ABORTED"
		newResponse.Leader = false
		newResponse.Timestamp = response.Timestamp
		newResponse.ServerResponse = true
		encoder := gob.NewEncoder(clientConnections[response.Timestamp])
		errs := encoder.Encode(newResponse)
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding message")
			return
		}
		// fmt.Println("Response sent to client")
		aborting()
	} else if node.NumResponses[response.Timestamp] == 5 { // we can commit - tell the client
		response.Text = "GOOD"
		node.ReceiveMessage <- response
		twoPC()

		var newResponse Message
		newResponse.Text = "COMMIT OK"
		newResponse.Leader = false
		newResponse.Timestamp = response.Timestamp
		newResponse.ServerResponse = true
		encoder := gob.NewEncoder(clientConnections[response.Timestamp])
		errs := encoder.Encode(newResponse)
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding message")
			return
		}
	}
}

func prepare(connection net.Conn, messages Message) {
	messages.Text = "PREPARE"
	messages.Leader = false
	encoder := gob.NewEncoder(connection)
	errs := encoder.Encode(messages)
	if errs != nil {
		// fmt.Fprintf(os.Stderr, "Error encoding response")
		return
	}
	// fmt.Println("Prepare sent to non-leader servers")
}

func sendToServer(transaction string) {
	var findServer string
	if strings.Contains(transaction, "A.") {
		findServer = "A"
	} else if strings.Contains(transaction, "B.") {
		findServer = "B"
	} else if strings.Contains(transaction, "C.") {
		findServer = "C"
	} else if strings.Contains(transaction, "D.") {
		findServer = "D"
	} else {
		findServer = "E"
	}

	duck.Lock()
	temp := connections[findServer]
	duck.Unlock()
	sendReceiveMessage(temp)

}

func sendReceiveMessage(connection net.Conn) {
	sendMessages(connection)
	receiveMessages(connection)
}

func sendMessages(connection net.Conn) {
	encoder := gob.NewEncoder(connection)
	message := <-node.SendMessage
	message.Leader = false
	err := encoder.Encode(message)
	if err != nil {
		// fmt.Println("Error encoding message: ", err)
		return
	}

	// fmt.Println("Message sent to non-leader server")
}

func receiveMessages(connection net.Conn) {
	var response Message
	decoder := gob.NewDecoder(connection)
	err := decoder.Decode(&response)
	if err != nil {
		// fmt.Fprintf(os.Stderr, "Error decoding message RECEIVE")
		return
	}

	if response.Text == "NOT FOUND, ABORTED" || response.Text == "ABORTED" {
		node.ReceiveMessage <- response
		aborting()
	}

	node.SendMessage <- response
	// fmt.Println("Received message")

	sendToClient()
}

func sendToClient() {
	message := <-node.SendMessage
	encoder := gob.NewEncoder(clientConnections[message.Timestamp])
	err := encoder.Encode(message)
	if err != nil {
		// fmt.Println("Error encoding message: ", err)
		return
	}

	// fmt.Println("Message sent to client")
}

func read(connection net.Conn) {
	messages := <-node.ReceiveMessage
	eachWord := strings.Split(messages.Text, " ")
	nodeId := node.Id
	dot := '.'
	concatenatedVersion := nodeId + string(dot)
	account := strings.TrimPrefix(eachWord[1], concatenatedVersion) // we have the account name now

	_, ok := node.Accounts[account] // check whether the account exists

	if !ok {
		// ABORT
		encoder := gob.NewEncoder(connection)
		errs := encoder.Encode(Message{"NOT FOUND, ABORTED", false, messages.Timestamp, true})
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding response")
			return
		}

		return
	}

	node.Accounts[account].m.Lock()
	if messages.Timestamp > node.Accounts[account].acc.CommittedTimestamp {
		objectD := node.Accounts[account].acc
		var rightTime int64
		rightTime = objectD.CommittedTimestamp
		for key := range objectD.TW {
			if (key <= messages.Timestamp) && (key > rightTime) { // (key > objectD.CommittedTimestamp) &&
				rightTime = key
			}
		}

		// if rightTime == 0 { // no tentative writes
		// 	rightTime = objectD.CommittedTimestamp
		// }

		if rightTime == objectD.CommittedTimestamp { // Ds is committed since we got the committed timestamp as the max write timestamp

			commitVal := strconv.Itoa(objectD.CommittedValue)
			equals := " = "
			sendingMessage := Message{}
			sendingMessage.Text = eachWord[1] + equals + commitVal
			sendingMessage.Leader = false
			sendingMessage.ServerResponse = true
			sendingMessage.Timestamp = messages.Timestamp
			encoder := gob.NewEncoder(connection)
			errs := encoder.Encode(sendingMessage)
			if errs != nil {
				// fmt.Fprintf(os.Stderr, "Error encoding response")
				node.Accounts[account].m.Unlock()
				return
			}

			if len(objectD.RTS) > 0 {
				isPresent := false
				for i := 0; i < len(objectD.RTS); i++ {
					if objectD.RTS[i] == messages.Timestamp {
						isPresent = true
						break
					}
				}
				if !isPresent {
					objectD.RTS = append(objectD.RTS, messages.Timestamp)
					sort.Slice(objectD.RTS, func(i, j int) bool { // sorting it in increasing order since rtsCheck needs RTS list to be ordered
						return objectD.RTS[i] < objectD.RTS[j]
					})
					// node.Accounts[account].acc = objectD
				}
			} else {
				objectD.RTS = append(objectD.RTS, messages.Timestamp)
				sort.Slice(objectD.RTS, func(i, j int) bool { // sorting it in increasing order since rtsCheck needs RTS list to be ordered
					return objectD.RTS[i] < objectD.RTS[j]
				})
				// node.Accounts[account].acc = objectD

			}
		} else { // Ds isn't committed
			if rightTime == messages.Timestamp { // if Ds was written by Tc - this case is working
				val := node.Accounts[account].acc.TW[rightTime]
				commitVal := strconv.Itoa(val)
				equals := " = "
				sendingMessage := Message{}
				sendingMessage.Text = eachWord[1] + equals + commitVal
				sendingMessage.Leader = false
				sendingMessage.ServerResponse = true
				sendingMessage.Timestamp = messages.Timestamp
				encoder := gob.NewEncoder(connection)
				errs := encoder.Encode(sendingMessage)
				if errs != nil {
					// fmt.Fprintf(os.Stderr, "Error encoding response")
					node.Accounts[account].m.Unlock()
					return
				}
			} else { // need to wait
				// fmt.Println("WAITING")
				node.Accounts[account].m.Unlock()

				node.Accounts[account].c.L.Lock()
				// for objectD.CommittedTimestamp < rightTime {
				// 	objectD.c.Wait()
				// }
				node.Accounts[account].c.Wait()
				// fmt.Println("Wakes up")
				node.Accounts[account].c.L.Unlock()
				// node.ReceiveMessage <- messages

				// fmt.Println("Gets here")

				node.ReceiveMessage <- messages
				read(connection)

				// node.Accounts[account].m.Lock()
				return
			}
		}
	} else {
		// ABORT
		encoder := gob.NewEncoder(connection)
		errs := encoder.Encode(Message{"ABORTED", false, messages.Timestamp, true})
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding response")
			node.Accounts[account].m.Unlock()
			return
		}
		//fmt.Println("ABORTED")
	}
	node.Accounts[account].m.Unlock()
}

func read_write(connection net.Conn) {
	messages := <-node.ReceiveMessage
	eachWord := strings.Split(messages.Text, " ")
	nodeId := node.Id
	dot := '.'
	concatenatedVersion := nodeId + string(dot)
	account := strings.TrimPrefix(eachWord[1], concatenatedVersion) // we have the account name now
	value, _ := strconv.Atoi(eachWord[2])

	_, ok := node.Accounts[account]

	// key doesn't exist, meaning the account doesn't exist yet
	if !ok {
		if eachWord[0] == "WITHDRAW" {
			// ABORT
			encoder := gob.NewEncoder(connection)
			errs := encoder.Encode(Message{"NOT FOUND, ABORTED", false, messages.Timestamp, true})
			if errs != nil {
				// fmt.Fprintf(os.Stderr, "Error encoding response")
				return
			}

			return
		} else {
			accountDetails := Account{}
			accountDetails.CommittedValue = 0
			accountDetails.CommittedTimestamp = 0 // flag for rolling back account creation
			accountDetails.TW = make(map[int64]int)

			safeaccountDetails := SafeAccount{}
			safeaccountDetails.acc = &accountDetails
			safeaccountDetails.m = &(sync.Mutex{})
			safeaccountDetails.c = sync.NewCond(safeaccountDetails.m)
			duck.Lock()
			node.Accounts[account] = safeaccountDetails
			duck.Unlock()
		}
	}

	var read_value int = 0

	node.Accounts[account].m.Lock()
	if messages.Timestamp > node.Accounts[account].acc.CommittedTimestamp {
		objectD := node.Accounts[account].acc
		var rightTime int64
		rightTime = objectD.CommittedTimestamp
		for key := range objectD.TW {
			if (key <= messages.Timestamp) && (key > rightTime) { // (key > objectD.CommittedTimestamp) &&
				rightTime = key
			}
		}

		if rightTime == objectD.CommittedTimestamp { // Ds is committed since we got the committed timestamp as the max write timestamp
			read_value = objectD.CommittedValue

			if len(objectD.RTS) > 0 {
				isPresent := false
				for i := 0; i < len(objectD.RTS); i++ {
					if objectD.RTS[i] == messages.Timestamp {
						isPresent = true
						break
					}
				}
				if !isPresent {
					objectD.RTS = append(objectD.RTS, messages.Timestamp)
					sort.Slice(objectD.RTS, func(i, j int) bool { // sorting it in increasing order since rtsCheck needs RTS list to be ordered
						return objectD.RTS[i] < objectD.RTS[j]
					})
					// node.Accounts[account].acc = objectD
				}
			} else {
				objectD.RTS = append(objectD.RTS, messages.Timestamp)
				sort.Slice(objectD.RTS, func(i, j int) bool { // sorting it in increasing order since rtsCheck needs RTS list to be ordered
					return objectD.RTS[i] < objectD.RTS[j]
				})
				// node.Accounts[account].acc = objectD

			}
		} else { // Ds isn't committed
			if rightTime == messages.Timestamp { // if Ds was written by Tc - this case is working
				read_value = node.Accounts[account].acc.TW[rightTime]
			} else { // need to wait
				// fmt.Println("WAITING")
				node.Accounts[account].m.Unlock()

				node.Accounts[account].c.L.Lock()
				// for objectD.CommittedTimestamp < rightTime {
				// 	objectD.c.Wait()
				// }
				node.Accounts[account].c.Wait()
				node.Accounts[account].c.L.Unlock()

				node.ReceiveMessage <- messages
				read_write(connection)

				// node.Accounts[account].m.Lock()
				return
			}
		}
	} else {
		// ABORT
		// fmt.Println("ABORTED")
		encoder := gob.NewEncoder(connection)
		errs := encoder.Encode(Message{"ABORTED", false, messages.Timestamp, true})
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding response")
			node.Accounts[account].m.Unlock()
			return
		}
	}

	if rtsCheck(messages, account) == true && messages.Timestamp > node.Accounts[account].acc.CommittedTimestamp {
		if eachWord[0] == "WITHDRAW" {
			node.Accounts[account].acc.TW[messages.Timestamp] = read_value - value
		} else {
			node.Accounts[account].acc.TW[messages.Timestamp] = read_value + value
		}
		// fmt.Println(node.Accounts[account].TW[messages.Timestamp])
		// fmt.Println(messages.Timestamp)
		encoder := gob.NewEncoder(connection)
		errs := encoder.Encode(Message{"OK", false, messages.Timestamp, true})
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding response")
			node.Accounts[account].m.Unlock()
			return
		}
		// fmt.Println("Sent back to leader server")
	} else {
		// ABORT
		encoder := gob.NewEncoder(connection)
		errs := encoder.Encode(Message{"ABORTED", false, messages.Timestamp, true})
		if errs != nil {
			// fmt.Fprintf(os.Stderr, "Error encoding response")
			node.Accounts[account].m.Unlock()
			return
		}
		//fmt.Println("ABORTED")
	}

	node.Accounts[account].m.Unlock()
}

func rtsCheck(messages Message, account string) bool {
	lastIndex := len(node.Accounts[account].acc.RTS) - 1
	if lastIndex < 0 {
		return true // there's nothing in the RTS
	}

	if messages.Timestamp >= node.Accounts[account].acc.RTS[lastIndex] {
		return true
	} else {
		return false
	}
}

func main() {
	args := os.Args
	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "Right format: ./server branch config.txt")
		return
	}

	config_file := args[2]
	file, err := os.ReadFile(config_file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't open config file")
	}

	node = Server{}
	node.ReceiveMessage = make(chan Message, 200)
	node.SendMessage = make(chan Message, 200)

	fileString := string(file)
	eachLine := strings.Split(fileString, "\n")

	// populating current server's struct
	for i := 0; i < 5; i++ {
		server := strings.Fields(eachLine[i])
		if server[0] == args[1] {
			// this is the current server
			node.Id = server[0]
			node.Address = server[1]
			node.Port = server[2]
			node.Accounts = make(map[string]SafeAccount)
			node.NumResponses = make(map[int64]int)

			break
		} else {
			continue
		}
	}

	// go initiateConnections(fileString)
	// go receiveConnections()

	var waiting sync.WaitGroup

	waiting.Add(2)

	go func() {
		defer waiting.Done()
		initiateConnections(fileString)
	}()

	go func() {
		defer waiting.Done()
		receiveConnections()
	}()

	waiting.Wait()
}