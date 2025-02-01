package mapreduce

import (
	"fmt"
	"log"
	"strings"
)

type MapImplementation[K1, K2 comparable, V any] interface {
	LineToRecords(line string) ([]K1, error)
	Map(key K1) (Pair[K2, V], error)
}

type mapProcessor[K1, K2 comparable, V any] struct {
	mapper MapImplementation[K1, K2, V]
}

func NewMapServer[K1, K2 comparable, V any](masterAddress string, m MapImplementation[K1, K2, V]) (*StageServer, error) {
	mp := &mapProcessor[K1, K2, V]{
		mapper: m,
	}
	srv, err := newStageServer("mapper", masterAddress, mp)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (m *mapProcessor[K1, K2, V]) Process(s *StageServer, cmd string, args ...string) string {
	log.Printf("Process -> %s (%s)\n", cmd, strings.Join(args, " "))
	switch cmd {
	case "map":
		if len(args) != 2 {
			return "invalid map command, expected 2 args\n"
		}
		go m.runMap(s, args[0], args[1])
		return fmt.Sprintf("ok mapping %s -> %s\n", args[0], args[1])
	case "ping":
		return "pong\n"
	}
	return ""
}

func (m *mapProcessor[K1, K2, V]) runMap(s *StageServer, fnIn, fnOut string) {
	log.Printf("running map in: %s, out: %s\n", fnIn, fnOut)
	err := MapTextFile(fnIn, fnOut, m.mapper)
	if err != nil {
		log.Printf("failed running map: %s\n", err)
		err := s.Write("map error %s %s\n", s.Id(), err)
		if err != nil {
			log.Printf("failed to notify master: %s\n", err)
		}
		return
	}
	log.Println("mapping done, notify master")
	err = s.Write("map done %s %s\n", s.Id(), fnOut)
	if err != nil {
		log.Printf("failed to notify master: %s\n", err)
	}
}
