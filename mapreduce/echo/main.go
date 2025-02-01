package main

import (
	n "mapreduce/internal/network"
)

type handler struct{}

func (s *handler) Process(cmd string) (string, error) {
	return "ECHO " + cmd, nil
}

func main() {
	srv := n.NewServer("echo", ":8000", &handler{})
	srv.Run()
}
