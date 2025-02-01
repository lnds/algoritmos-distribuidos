package mapreduce

import (
	"fmt"
	"log"
	"strings"
)

type ReduceImplementation[K1, K2 comparable, V1, V2 any] interface {
	LineToPair(line string) (Pair[K1, V1], error)
	Reduce(key K1, values []V1) ([]Pair[K2, V2], error)
}

type reduceProcessor[K1, K2 comparable, V1, V2 any] struct {
	reducer ReduceImplementation[K1, K2, V1, V2]
}

func NewReduceServer[K1, K2 comparable, V1, V2 any](
	masterAddress string,
	r ReduceImplementation[K1, K2, V1, V2],
) (*StageServer, error) {
	rp := &reduceProcessor[K1, K2, V1, V2]{
		reducer: r,
	}
	srv, err := newStageServer("reducer", masterAddress, rp)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (r *reduceProcessor[K1, K2, V1, V2]) Process(s *StageServer, cmd string, args ...string) string {
	log.Printf("Process -> %s (%s)\n", cmd, strings.Join(args, " "))
	switch cmd {
	case "reduce":
		if len(args) != 2 {
			return "invalid reduce command, expected 2 args\n"
		}
		go r.runReduce(s, args[0], args[1])
		return fmt.Sprintf("ok reducing %s -> %s\n", args[0], args[1])
	case "ping":
		return "pong\n"
	}
	return ""
}

func (r *reduceProcessor[K1, K2, V1, V2]) runReduce(s *StageServer, fnIn, fnOut string) {
	log.Printf("running reduce in: %s, out: %s\n", fnIn, fnOut)
	err := ReduceTextFile(fnIn, fnOut, r.reducer)
	if err != nil {
		log.Printf("failed running reduce: %s\n", err)
		err := s.Write("reduce error %s %s\n", s.Id(), err)
		if err != nil {
			log.Printf("failed to notify master: %s\n", err)
		}
		return
	}
	log.Println("reduce done, notify master")
	err = s.Write("reduce done %s %s\n", s.Id(), fnOut)
	if err != nil {
		log.Printf("failed to notify master: %s\n", err)
	}
}
