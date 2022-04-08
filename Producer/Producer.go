package main

import (
	"bufio"
	"fmt"
	"github.com/nats-io/stan.go"
	"os"
)

func main() {
	// Connect to nats-streaming-server
	sc, err := stan.Connect("prod", "pub")
	// Handle error of connection
	if err != nil {
		panic("Can't connect to nats-streaming server")
	}
	// Close connection after end of main function
	defer sc.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("-------NEW PORTION OF DATA-------")
		fmt.Printf("PATH: ")
		scanner.Scan()
		name := scanner.Text()
		data, err := os.ReadFile(name)
		// Handle error of reading file
		if err != nil {
			fmt.Printf("no such file\n")
			continue
		}
		fmt.Printf("read successfully\n")
		sc.Publish("json_data", data)

	}
}
