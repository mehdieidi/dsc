package main

import (
	"encoding/json"
	"github.com/oklog/ulid/v2"
	"log"
	"log/slog"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type handler struct {
	node   *maelstrom.Node
	logger *slog.Logger
}

func (h *handler) msgHandler(msg maelstrom.Message) error {
	var body map[string]any
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		h.logger.Error(err.Error())
		return err
	}

	body["type"] = "generate_ok"

	body["id"] = ulid.Make()

	return h.node.Reply(msg, body)
}

func main() {
	logFile, err := os.Create("./logs.log")
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	logger := slog.New(slog.NewJSONHandler(logFile, nil))

	n := maelstrom.NewNode()

	handler := handler{n, logger}

	n.Handle("generate", handler.msgHandler)

	err = n.Run()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
