package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"log/slog"
	"os"
)

type srv struct {
	node   *maelstrom.Node
	logger *slog.Logger
	cache  cache
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
	s.cache.data[int(message)] = struct{}{}
	s.cache.mu.Unlock()

	// Broadcast the message to all the nodes in the cluster.
	for _, nodeID := range s.node.NodeIDs() {
		// This node is the sender of the message.
		if nodeID == msg.Src {
			continue
		}
		// This is the current node.
		if nodeID == s.node.ID() {
			continue
		}

		go func(id string) {
			err = s.node.Send(id, body)
			if err != nil {
				s.logger.Error(err.Error())
			}
		}(nodeID)
	}

	response := map[string]any{"type": "broadcast_ok"}
	err = s.node.Reply(msg, response)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	return nil
}

func (s *srv) readHandler(msg maelstrom.Message) error {
	response := map[string]any{"type": "read_ok"}

	var messages []int

	s.cache.mu.RLock()
	for d := range s.cache.data {
		messages = append(messages, d)
	}
	s.cache.mu.RUnlock()

	response["messages"] = messages

	return s.node.Reply(msg, response)
}

func (s *srv) topologyHandler(msg maelstrom.Message) error {
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
		node:   n,
		logger: logger,
		cache:  cache{data: make(map[int]struct{})},
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
