package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	elastigo "github.com/mattbaird/elastigo/lib"
)

type GeoElement struct {
	Id string
}

var (
	host *string = flag.String("host", "localhost", "Elasticsearch Host")
)

func main() {
	pathFilePtr := flag.String("pathFile", "", "path of the geonames' file.")
	flag.Parse()

	if *pathFilePtr == "" {
		flag.PrintDefaults()
		os.Exit(2)
	}

	channel := make(chan []string)

	for i := 0; i < 10; i++ {
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

		reflect.TypeOf(record)

		channel <- record

		if counter%1000 == 0 {
			if counter%10000 == 0 {
				fmt.Print(". " + strconv.Itoa(counter/1000) + "K")
				fmt.Println("")
			} else {
				fmt.Print(".")
			}
		}
	}
	fmt.Println("")
	fmt.Println("==================================")
	fmt.Println("Total processed lines: ", counter-1)
	os.Exit(1)
}

func printCsvLine(channel chan []string) {
	c := elastigo.NewConn()

	for {
		record := <-channel

		element := GeoElement{record[1]}

		value, _ := json.Marshal(element)

		_, err := c.Index("geolocations", "geoname", record[0], nil, string(value))

		if err != nil {
			panic(err)
		}
	}
}
