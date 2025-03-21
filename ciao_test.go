package ciao

import (
	"cloud.google.com/go/bigquery"
	"context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"os"
	"testing"
)

func makeTestClient(t *testing.T, ctx context.Context) *Ciao {
	var jsonAuthRaw, err = os.ReadFile("cred.json")
	cred, err := google.CredentialsFromJSON(ctx, jsonAuthRaw)
	if err != nil {
		t.Fatalf("unable to read json credentials: %s", err)
	}
	bq, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentialsJSON(cred.JSON))
	if err != nil {
		t.Fatalf("unable to open create bq client: %s", err)
	}
	return NewFromClient(bq, "logs_dev", "test_tbl")
}

func TestCiao_Insert(t *testing.T) {
	ctx := context.Background()
	ciao := makeTestClient(t, ctx)
	var event = NewEvent()
	event.Type = "test"
	event.Author = "developer"
	event.Subject = "TestCiao_Insert"
	if err := event.SetPayload(Payload{
		"beautiful": true,
		"nested": Payload{
			"random": 42,
		},
		"string": "ciao",
	}); err != nil {
		t.Fatalf("unable to set payload: %s", err)
	}

	if err := ciao.Insert(ctx, event); err != nil {
		t.Fatalf("unable to insert event: %s", err)
	}

}

func TestCiao_EventBuilder(t *testing.T) {
	ctx := context.Background()
	ciao := makeTestClient(t, ctx)
	ciao.InitDefaultEventBuilder("test_suite")
	event, err := ciao.DefaultEventBuilder.NewEvent("test_event", nil, Payload{
		"beautiful": true,
		"nested": Payload{
			"random": 42,
		},
		"string": "ciao",
	})
	if err != nil {
		t.Fatalf("unable to create event from builder: %s", err)
	}
	event.SetSubject("TestCiao_EventBuilder")
	if err := ciao.Insert(ctx, event); err != nil {
		t.Fatalf("unable to insert event: %s", err)
	}
}
