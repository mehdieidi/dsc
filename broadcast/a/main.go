package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type srv struct {
	node          *maelstrom.Node
	logger        *slog.Logger
	cache         cache
	topology      map[string][]string
	topologyMutex sync.RWMutex
}

func (s *srv) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	message := body["message"].(float64)

	s.cache.mu.Lock()
	s.cache.data = append(s.cache.data, int(message))
	s.cache.mu.Unlock()

	response := map[string]any{"type": "broadcast_ok"}
	return s.node.Reply(msg, response)
}

func (s *srv) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	response := map[string]any{"type": "read_ok"}

	s.cache.mu.RLock()
	response["messages"] = s.cache.data
	s.cache.mu.RUnlock()

	return s.node.Reply(msg, response)
}

type topologyReq struct {
	Topology map[string][]string `json:"topology"`
}

func (s *srv) topologyHandler(msg maelstrom.Message) error {
	var body topologyReq
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	s.topologyMutex.Lock()
	s.topology = body.Topology
	s.topologyMutex.Unlock()

	response := map[string]any{"type": "topology_ok"}
	return s.node.Reply(msg, response)
}

func main() {
	logFile, err := os.Create("/home/mehdi/work/dsc/broadcast/logs.log")
	if err != nil {
		log.Fatal(err)
	}
	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(logFile)

	logger := slog.New(slog.NewTextHandler(logFile, nil))

	n := maelstrom.NewNode()

	srv := srv{
		node:     n,
		logger:   logger,
		cache:    cache{data: make([]int, 0)},
		topology: make(map[string][]string),
	}

	n.Handle("broadcast", srv.broadcastHandler)
	n.Handle("read", srv.readHandler)
	n.Handle("topology", srv.topologyHandler)

	err = n.Run()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
