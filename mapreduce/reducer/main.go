package main

import (
	"errors"
	"flag"
	"log"
	mr "mapreduce/internal/mapreduce"
	"strconv"
	"strings"
)

func main() {
	var master string

	flag.StringVar(&master, "master", "localhost:8000", "master address, default localhost:8000")
	flag.Parse()
	mapper, err := mr.NewReduceServer(master, newWordCountReducer())
	if err != nil {
		log.Fatal("can not start reducer", err)
	}
	log.Fatal(mapper.Run())
}

// Implements Word Count

type wordCountReduceImpl struct{}

func newWordCountReducer() mr.ReduceImplementation[string, string, int, int] {
	return &wordCountReduceImpl{}
}

func (r *wordCountReduceImpl) LineToPair(line string) (result mr.Pair[string, int], err error) {
	split := strings.Split(line, ",")
	if len(split) != 2 {
		err = errors.New("wrong line format")
		return
	}
	result.Key = split[0]
	result.Value, err = strconv.Atoi(split[1])
	return
}

func (r *wordCountReduceImpl) Reduce(key string, values []int) ([]mr.Pair[string, int], error) {
	sum := 0
	for _, value := range values {
		sum += value
	}
	// we only need to return one value
	return []mr.Pair[string, int]{
		{Key: key, Value: sum},
	}, nil
}
