package network

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

type Handler interface {
	Process(context.Context, string) (string, error)
}

type Server interface {
	Run() error
	Log(string, ...any)
}

type server struct {
	name    string
	address string
	handler Handler
}

func NewServer(name, address string, handler Handler) Server {
	return &server{
		name:    name,
		address: address,
		handler: handler,
	}
}

func (s *server) Run() error {
	listener, err := net.Listen(PROTO, s.address)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("listener: Accept error: %s", err.Error())
			continue
		}
		go s.process(conn)
	}
}

func (s *server) process(conn net.Conn) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := newConnection(conn)
	ctx = context.WithValue(ctx, "connection", c)
	defer c.Close()
	defer cancel()
	for {
		message, err := c.Read()
		if err != nil {
			s.Log("error reading message: %s", err)
			if err == io.EOF {
				s.Log("connection closed from: %s", c.RemoteAddress())
				return
			}
		}
		response, err := s.handler.Process(ctx, message)
		if err != nil {
			s.Log("error processing response: %s", err)
			continue
		}

		if response == "" {
			continue
		}

		if !strings.HasSuffix(response, DELIM_SUFFIX) {
			response += DELIM_SUFFIX
		}
		_, err = c.Write(response)
		if err != nil {
			s.Log("error writing response: %s", err)
			if err == io.EOF {
				s.Log("connection reset by peer %s", c.RemoteAddress())
				return
			}
		}
	}
}

func (s *server) Log(msg string, args ...any) {
	log.Printf("[server: %s] %s\n", s.name, fmt.Sprintf(msg, args...))
}
