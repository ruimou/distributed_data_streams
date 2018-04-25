/*
File masterTestApp.go is the master test app. It starts all of the other test
apps and receives the co-ordinates that they generate directly. It uses this to
generate a heatmap.

A different app will be used to receive data from the distributed data queues.
*/
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"./lib/producer"
	"./movement"
)

var collectedPoints []movement.Point
var collectedPointsLock sync.Mutex

func main() {
	fmt.Println("This program connects to the data service as well as to an internal port for")
	fmt.Println("sending data to a webserver.")
	fmt.Println("Usage: go run <file> <serv-ip>:<serv-port> <internal-ip:internal-port>")
	var files []string

	root := "./testGraphs"
	appMap := make(map[string][]string) // Id -> list of filepaths
	currId := ""
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			appMap[currId] = files
			currId = info.Name()
			files = nil
		} else {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

	internalConn, err := net.Dial("tcp", os.Args[2])
	if err != nil {
		fmt.Println("Could not connect to internal:", err)
	}

	topicName := "ubc"
	for producerNodeId, files := range appMap {
		for _, file := range files {
			fmt.Println("Starting client node with graph file", file)

			m, err, firstPoint := movement.CreateLocationGraph(file)
			if err != nil {
				fmt.Println(err)
				continue
			}

			myId := producerNodeId

			// IDs 0,1,2 write to left side of WestMall
			// IDs 3,4 write to right side of WestMall

			// TODO UNCOMMENT IF WANT MULTIPLE TOPICS FOR DEMO
			// if i, _ := strconv.ParseInt(filepath.Base(file), 10, 64); i < int64(2) {
			// 	topicName = "westmall_left"
			// } else {
			// 	topicName = "westmall_right"
			// }

			wSess, err := producer.OpenTopic(topicName, os.Args[1], fmt.Sprintf("Writer %d", myId))
			if err != nil {
				continue
			}

			fn := func(p movement.Point) {
				parseF := func(float1 float64) string {
					return strconv.FormatFloat(float1, 'f', -1, 64)
				}

				// fmt.Printf("ID%d: %s %s\n", myId, parseF(p.X), parseF(p.Y))
				appendPoint(p)

				datum := fmt.Sprintf("%s %s\n", parseF(p.X), parseF(p.Y))

				internalConn.Write([]byte(datum))
				wSess.Write(datum)
			}

			// Hardcoding speed for demo
			f := float64(0.0002)

			// parse the speed from the filename
			// f, err := strconv.ParseFloat(file[strings.Index(file, "/")+1:], 64)
			// if err != nil {
			// 	continue
			// }

			// fmt.Println("PRINTING POINTS .... LEN : %d", len(m))
			// fmt.Println("%+v\n\n", m)
			go movement.Travel(firstPoint, m, f, fn)
		}
	}

	// New Write session for terminal inputs
	// Listening on any terminal inputs so we can add writes on demand for the demo

	var wSess *producer.WriteSession

	for {
		wSess, err = producer.OpenTopic(topicName, os.Args[1], fmt.Sprintf("Writer %d", 10))
		if err != nil {
			fmt.Println("Couldn't Open Write Session")
			time.Sleep(10 * time.Second)
			continue
		} else {
			break
		}
	}

	// Wait until the movement graphs have completed, and then poll stdin
	time.Sleep(50 * time.Second)
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter a point in the format: X,Y")
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		tokens := strings.Split(text, ",")

		if len(tokens) == 2 {
			x, y := tokens[0], tokens[1]

			datum := fmt.Sprintf("%s %s\n", x, y)

			// Write it 15 times because we can't see anything on the map otherwise
			for i := 0; i < 10; i++ {
				err := wSess.Write(datum)
				if err != nil {
					fmt.Println("ERROR IN WRITE.")
				}
				internalConn.Write([]byte(datum))
			}
		} else {
			fmt.Println("Not a valid point. Must be format: X,Y\n\n")
		}
	}
}

// Use this to atomically append a point to the global point slice
func appendPoint(point movement.Point) {
	collectedPointsLock.Lock()
	defer collectedPointsLock.Unlock()

	collectedPoints = append(collectedPoints, point)
}

// Self explanatory
func printAllPoints() {
	collectedPointsLock.Lock()
	defer collectedPointsLock.Unlock()

	for _, p := range collectedPoints {
		fmt.Printf("%.2f %.2f\n", p.X, p.Y)
	}
}
