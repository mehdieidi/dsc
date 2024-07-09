package main

import (
	"encoding/json"
	"github.com/oklog/ulid/v2"
	"log"
	"log/slog"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type srv struct {
	node   *maelstrom.Node
	logger *slog.Logger
}

func (s *srv) generateHandler(msg maelstrom.Message) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	response := map[string]any{
		"type": "generate_ok",
		"id":   ulid.Make(),
	}

	return s.node.Reply(msg, response)
}

func main() {
	logFile, err := os.Create("/home/mehdi/work/dsc/globally-unique-id-generation/logs.log")
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
	}

	n.Handle("generate", srv.generateHandler)

	err = n.Run()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
