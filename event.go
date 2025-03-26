package ciao

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"fmt"
	"github.com/rs/xid"
	"time"
)

type EventBuilder struct {
	Ciao   *Ciao
	Author string
}

func (ciao *Ciao) NewEventBuilder(author string) *EventBuilder {
	return &EventBuilder{Ciao: ciao, Author: author}
}

func (eb *EventBuilder) NewEvent(_type string, timestamp *time.Time, payload any) (*Event, error) {
	evt := NewRawEvent()
	evt.Author = eb.Author
	evt.Type = _type
	if timestamp != nil {
		evt.SetTimestamp(*timestamp)
	} else {
		evt.SetTimestamp(time.Now())
	}
	evt.RegenId()
	if payload != nil {
		switch p := payload.(type) {
		case Payload:
			if err := evt.SetPayload(p); err != nil {
				return nil, err
			}
		default:
			if err := evt.SetPayloadFromAny(p); err != nil {
				return nil, err
			}
		}
	}
	return evt, nil
}

type Payload map[string]any

func (p Payload) Add(k string, v any) Payload {
	p[k] = v
	return p
}

func (p Payload) Merge(p2 Payload) Payload {
	for k, v := range p2 {
		p[k] = v
	}
	return p
}

func (p Payload) MergeAny(p2 any) error {
	b, err := json.Marshal(p2)
	if err != nil {
		return err
	}
	var m = make(Payload)
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	p.Merge(m)
	return nil
}

type Event struct {
	Id        xid.ID
	Timestamp time.Time
	Type      string
	Subject   string
	payload   string
	Author    string
}

func NewEvent(options ...Option) *Event {
	return NewRawEvent(append(options, SetTimestamp, GenerateIdIfMissing)...)
}

func NewRawEvent(options ...Option) *Event {
	var e = &Event{}
	for _, option := range options {
		option(e)
	}
	return e
}

func (e *Event) SetTimestamp(ts time.Time) *Event {
	e.Timestamp = ts
	return e
}
func (e *Event) SetSubject(subject string) *Event {
	e.Subject = subject
	return e
}

func (e *Event) RegenId() *Event {
	GenerateId(e)
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
	if e.Timestamp.IsZero() {
		e.Id = xid.New()
	} else {
		e.Id = xid.NewWithTime(e.Timestamp)
	}
}
func GenerateIdIfMissing(e *Event) {
	if e.Id.IsZero() {
		GenerateId(e)
	}
}

func WithId(id xid.ID) func(e *Event) {
	return func(e *Event) {
		e.Id = id
	}
}

func SetTimestamp(e *Event) {
	e.Timestamp = time.Now()
}
