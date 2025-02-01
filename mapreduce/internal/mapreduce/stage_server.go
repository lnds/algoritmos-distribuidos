package mapreduce

import (
	"errors"
	"fmt"
	"io"
	"log"
	n "mapreduce/internal/network"
	"strings"
	"sync"
)

const (
	msgRegister = "register %s\n"
	msgAccepted = "%s accepted"
)

type StageProcessor interface {
	Process(*StageServer, string, ...string) string
}

type StageServer struct {
	label     string
	id        string
	master    n.Connection
	processor StageProcessor
	mu        sync.Mutex
}

func newStageServer(label, masterAddress string, processor StageProcessor) (*StageServer, error) {
	c, err := n.NewClient(masterAddress)
	if err != nil {
		return nil, err
	}
	return &StageServer{
		label:     label,
		master:    c,
		processor: processor,
	}, nil
}

func (s *StageServer) Run() error {
	err := s.register()
	if err != nil {
		return err
	}
	return s.run()
}

func (s *StageServer) register() error {
	_, err := s.master.Write(msgRegister, s.label)
	if err != nil {
		return err
	}
	response, err := s.master.Read()
	if err != nil {
		return err
	}
	prefix := fmt.Sprintf(msgAccepted, s.label)
	if !strings.HasPrefix(response, prefix) {
		return errors.New("can not register")
	}
	s.id = strings.TrimSpace(strings.TrimPrefix(response, prefix))
	return nil
}

func (s *StageServer) run() error {
	for {
		msg, err := s.master.Read()
		if err != nil {
			log.Println("error reading from master", err)
			if err == io.EOF {
				log.Println("connection closed")
				return err
			}
			continue
		}
		err = s.Write(s.process(msg))
		if err != nil {
			log.Println("error writing response to master", err)
			if err == io.EOF {
				log.Println("connection closed")
				return err
			}
		}
	}
}

func (s *StageServer) process(msg string) string {
	split := strings.Split(msg, " ")
	if len(split) == 0 {
		return "invalid command"
	}
	cmd := strings.ToLower(split[0])
	args := split[1:]
	return s.processor.Process(s, cmd, args...)
}

func (s *StageServer) Id() string {
	return s.id
}

func (s *StageServer) Write(msg string, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.master.Write(msg, args...)
	return err
}
