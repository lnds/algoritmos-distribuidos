package main

import (
	"flag"
	"log"
	mr "mapreduce/internal/mapreduce"
	"regexp"
	"strings"
)

func main() {
	var master string
	flag.StringVar(&master, "master", "localhost:8000", "master address, default localhost:8000")
	flag.Parse()
	mapper, err := mr.NewMapServer(master, newWordCountMapper())
	if err != nil {
		log.Fatal("can not start mapper", err)
	}
	log.Fatal(mapper.Run())
}

// Implements Word Count
type wordCountMapImpl struct{}

func newWordCountMapper() mr.MapImplementation[string, string, int] {
	return &wordCountMapImpl{}
}

// LineToRecords in this case split a line in words
func (m *wordCountMapImpl) LineToRecords(line string) ([]string, error) {
	re := regexp.MustCompile("[a-z]+")
	return re.FindAllString(strings.ToLower(line), -1), nil
}

// Map  a word to (word,1)
func (m *wordCountMapImpl) Map(word string) (mr.Pair[string, int], error) {
	return mr.Pair[string, int]{Key: word, Value: 1}, nil
}
