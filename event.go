package ciao

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Payload map[string]any
type Event struct {
	Id        string
	Timestamp time.Time
	Type      string
	Subject   string
	payload   string
	Author    string
}

func NewEvent(options ...Option) *Event {
	return NewRawEvent(append(options, GenerateIdIfMissing, SetTimestamp)...)
}
func NewEventWithId(id string, options ...Option) *Event {
	return NewRawEvent(append(options, WithId(id), SetTimestamp)...)
}

func NewRawEvent(options ...Option) *Event {
	var e = &Event{}
	for _, option := range options {
		option(e)
	}
	return e
}

func (e *Event) SetPayload(payload Payload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	e.payload = string(b)
	return nil
}

func (e *Event) SetPayloadFromAny(payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var m = make(Payload)
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	return e.SetPayload(m)
}

func (e *Event) Save() (map[string]bigquery.Value, string, error) {
	var m = map[string]bigquery.Value{
		"id":        e.Id,
		"timestamp": e.Timestamp,
		"type":      e.Type,
		"subject":   nil,
		"payload":   nil,
		"author":    nil,
	}
	if len(e.Subject) > 0 {
		m["subject"] = e.Subject
	}
	if len(e.payload) > 0 {
		m["payload"] = e.payload
	}
	if len(e.Author) > 0 {
		m["author"] = e.Author
	}
	return m, "", nil
}

func (e *Event) String() string {
	var buf = bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(buf, "Event %s, type: %s, at: %s, subject: %s, author: %s\n", e.Id, e.Type, e.Timestamp.Format(time.RFC3339Nano), e.Subject, e.Author)
	_, _ = fmt.Fprintf(buf, "Event %s payload start:\n", e.Id)
	_, _ = fmt.Fprintf(buf, "%+v", e.payload)
	_, _ = fmt.Fprintf(buf, "\n----\nEvent %s payload end", e.Id)
	return buf.String()
}

type Option func(e *Event)

func GenerateId(e *Event) {
	e.Id = uuid.NewString()
}
func GenerateIdIfMissing(e *Event) {
	if len(e.Id) == 0 {
		e.Id = uuid.NewString()
	}
}

func WithId(id string) func(e *Event) {
	return func(e *Event) {
		e.Id = id
	}
}

func SetTimestamp(e *Event) {
	e.Timestamp = time.Now()
}
