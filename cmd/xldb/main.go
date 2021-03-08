package main

import (
	"flag"
	"log"

	"github.com/MishaNiki/xldb"
)

var pathToInFile string

func init() {
	flag.StringVar(&pathToInFile, "file", "", "path to input file")
}

func main() {
	flag.Parse()

	if pathToInFile == "" {
		log.Fatal("empty path to input file")
		return
	}

	data, err := xldb.ParseInFile(pathToInFile)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(data); i++ {
		if err != data[i].Insert() {
			log.Fatal(err)
		}
	}
}
