package model

import "encoding/json"

type Message struct {
	Value   int    `json:"value"`
	Command string `json:"command"`
}

func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
