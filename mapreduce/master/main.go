package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mr "mapreduce/internal/mapreduce"
	n "mapreduce/internal/network"
)

const (
	taskRunning taskStatus = iota
	taskFailed
	taskSucceed
)

type taskStatus int

type task struct {
	processorId string
	in          string
	out         string
	status      taskStatus
}

type master struct {
	address      string
	mappers      []n.Connection
	reducers     []n.Connection
	tasks        map[string]task
	mu           sync.Mutex
	client       n.Connection
	processStart time.Time
}

var server n.Server

func main() {
	var host string
	flag.StringVar(
		&host,
		"host",
		"localhost",
		"host name or ip address of server, default: localhost",
	)
	port := 8000
	flag.IntVar(&port, "port", port, "port to listen, default: 8000")
	flag.Parse()
	address := fmt.Sprintf("%s:%d", host, port)
	server = n.NewServer("master", address, newMaster(address))
	server.Log("server started")
	server.Run()
}

func newMaster(address string) *master {
	return &master{
		address: address,
		tasks:   make(map[string]task),
	}
}

func (m *master) Process(ctx context.Context, msg string) (string, error) {
	conn := ctx.Value("connection").(n.Connection)
	msg = strings.TrimSpace(msg)
	server.Log("processsing [%s]\n", msg)
	split := strings.Split(msg, " ")
	if len(split) == 0 {
		return "", errors.New("empty command not supported")
	}
	cmd := strings.ToLower(split[0])
	args := split[1:]
	switch cmd {
	case "register":
		return m.register(conn, args...)
	case "status":
		return fmt.Sprintf(
			"connected mappers %d\nconnected reducers %d\n",
			len(m.mappers),
			len(m.reducers),
		), nil
	case "process":
		if len(m.mappers) == 0 {
			return "can't process because no mappers connected", nil
		}
		if len(m.reducers) == 0 {
			return "can't process because no reducers connected", nil
		}
		if !m.registerClient(conn) {
			return "server is busy try later", nil
		}
		go m.startMapStage(ctx, args[0])
		return "ok, processing", nil
	case "map":
		if len(args) < 3 {
			return "invalid map command", nil
		}
		server.Log("map result: %s", args[0])
		finished := m.processTaskResult(args[0], args[1], args[2])
		if finished {
			elapsed := time.Since(m.processStart)
			_, err := m.client.Write("map finished, elapsed time %s\n", elapsed)
			if err != nil {
				server.Log("error sending status to client %v\n", err.Error())
			}
			go m.startReduceStage(ctx)
		}
		return "ok", nil
	case "reduce":
		if len(args) < 3 {
			return "invalid reduce command", nil
		}
		server.Log("reduce result: %s", args[0])
		finished := m.processTaskResult(args[0], args[1], args[2])
		if finished {
			elapsed := time.Since(m.processStart)
			_, err := m.client.Write("reduce finished elapsed time %s\n", elapsed)
			if err != nil {
				server.Log("error sending status to client %v\n", err.Error())
			}
			m.removeClient()
		}
		return "ok", nil
	case "ok":
		server.Log("ack received: %s", msg)
		return "", nil
	}
	server.Log("!!unknown command [%s]", msg)
	return "unknown command", nil
}

func (m *master) registerClient(conn n.Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		return false
	}
	m.client = conn
	m.processStart = time.Now()
	return true
}

func (m *master) removeClient() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.client = nil
}

func (m *master) startMapStage(ctx context.Context, fnIn string) {
	parts := len(m.mappers)
	server.Log("running map reduce with %d parts", parts)
	files, err := mr.SplitTextFile(fnIn, "/tmp/m", parts)
	if err != nil {
		server.Log("failed to split file: %s", err)
		return
	}
	for i, mapper := range m.mappers {
		select {
		case <-ctx.Done():
			return
		default:
			t := task{
				processorId: strconv.Itoa(i),
				in:          files[i],
				out:         fmt.Sprintf("/tmp/m-out-%d.txt", i),
				status:      taskRunning,
			}
			server.Log("sending map %s command to mapper %d\n", t.in, i)

			_, err := mapper.Write("map %s %s\n", t.in, t.out)
			if err != nil {
				// TODO: handle better this error, should be added to a reintent queue
				server.Log("failed to send map command: %s to mapper %d", err, i)
			}
			m.tasks[t.processorId] = t
		}
	}
	server.Log("mappers notified")
}

func (m *master) processTaskResult(status string, mapperId string, outputFile string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[mapperId]
	switch status {
	case "done":
		t.status = taskSucceed
		t.out = outputFile
	case "error":
		t.status = taskFailed
		t.out = outputFile
	}
	m.tasks[mapperId] = t
	for _, t := range m.tasks {
		if t.status == taskRunning {
			return false
		}
	}
	return true
}

func (m *master) startReduceStage(ctx context.Context) {
	server.Log("running reduce stage\n")
	files, err := m.shuffleMapReduceResults()
	if err != nil {
		server.Log("failed to shuffle map reduce results: %s", err)
		return
	}
	for i, reducer := range m.reducers {
		select {
		case <-ctx.Done():
			return
		default:
			t := task{
				processorId: strconv.Itoa(i),
				in:          files[i],
				out:         fmt.Sprintf("/tmp/r-out-%d.txt", i),
				status:      taskRunning,
			}
			server.Log("sending reduce %s command to reducer %d\n", t.in, i)

			_, err := reducer.Write("reduce %s %s\n", t.in, t.out)
			if err != nil {
				server.Log("failed to send reduce command: %s to reducer %d", err, i)
			}
			m.tasks[t.processorId] = t
		}
	}
	server.Log("reducers notified\n")
}

func (m *master) shuffleMapReduceResults() ([]string, error) {
	server.Log("shuffling map reduce results\n")
	var fnMapRes []string
	for k, t := range m.tasks {
		fnMapRes = append(fnMapRes, t.out)
		delete(m.tasks, k)
	}
	nreducers := len(m.reducers)
	if nreducers == 0 {
		return nil, errors.New("can only shuffle if there are reducers\n")
	}
	names, err := mr.ShuffleTextFiles(fnMapRes, "/tmp/r", nreducers, func(key string) int {
		if len(key) == 0 {
			return 0
		}
		return int(([]byte(key))[0]-'a') % nreducers
	})
	return names, err
}

func (m *master) register(conn n.Connection, args ...string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kind := strings.ToLower(args[0])
	switch kind {
	case "mapper":
		id := m.addMapper(conn)
		return fmt.Sprintf("mapper accepted %d\n", id), nil
	case "reducer":
		id := m.addReducer(conn)
		return fmt.Sprintf("reducer accepted %d\n", id), nil
	}
	return "invalid register command", nil
}

func (m *master) addMapper(conn n.Connection) int {
	id := len(m.mappers)
	m.mappers = append(m.mappers, conn)
	return id
}

func (m *master) addReducer(conn n.Connection) int {
	id := len(m.reducers)
	m.reducers = append(m.reducers, conn)
	return id
}
