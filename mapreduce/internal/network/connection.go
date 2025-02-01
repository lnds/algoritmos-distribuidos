package network

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

const (
	PROTO        = "tcp"
	DELIM        = '\n'
	DELIM_SUFFIX = "\n"
)

type Connection interface {
	Read() (string, error)
	Write(string, ...any) (int, error)
	Close()
	RemoteAddress() string
}

type connection struct {
	conn net.Conn
	rw   *bufio.ReadWriter
}

func newConnection(conn net.Conn) *connection {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return &connection{
		conn: conn,
		rw:   rw,
	}
}

func (c *connection) Read() (string, error) {
	response, err := c.rw.ReadString(DELIM)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(response), nil
}

func (c *connection) Write(s string, args ...any) (int, error) {
	msg := fmt.Sprintf(s, args...)
	n, err := c.rw.WriteString(msg)
	if err != nil {
		return 0, err
	}
	err = c.rw.Flush()
	return n, err
}

func (c *connection) Close() {
	c.conn.Close()
}

func (c *connection) RemoteAddress() string {
	addr := c.conn.RemoteAddr()
	return fmt.Sprintf("%s:%s", addr.Network(), addr.String())
}
