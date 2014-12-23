package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	pathFilePtr := flag.String("pathFile", "", "path of the geonames' file.")
	flag.Parse()

	if *pathFilePtr == "" {
		flag.PrintDefaults()
		os.Exit(2)
	}

	channel := make(chan []string)

	for i := 0; i < 100; i++ {
		go printCsvLine(channel)
	}

	go readCsv(pathFilePtr, channel)

	time.Sleep(time.Second * 10000)
}

func readCsv(pathFile *string, channel chan []string) {
	csvFile, err := os.Open(*pathFile)

	defer csvFile.Close()

	if err != nil {
		panic(err)
	}

	reader := csv.NewReader(csvFile)
	reader.Comma = '\t'

	counter := 0

	for {
		counter = counter + 1
		record, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		channel <- record
	}

	fmt.Println("==================================")
	fmt.Println("Total processed lines: ", counter-1)
	os.Exit(1)
}

func printCsvLine(channel chan []string) {
	for {
		fmt.Println(<-channel)
	}
}
