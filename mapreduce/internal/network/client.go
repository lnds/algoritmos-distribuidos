package network

import "net"

func NewClient(address string) (Connection, error) {
	dial, err := net.Dial(PROTO, address)
	if err != nil {
		return nil, err
	}
	return newConnection(dial), nil
}
