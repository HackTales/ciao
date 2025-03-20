package ciao

import (
	"cloud.google.com/go/bigquery"
	"context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"os"
	"testing"
)

func TestCiao_Insert(t *testing.T) {
	ctx := context.Background()
	var jsonAuthRaw, err = os.ReadFile("cred.json")
	cred, err := google.CredentialsFromJSON(ctx, jsonAuthRaw)
	if err != nil {
		t.Fatalf("unable to read json credentials: %s", err)
	}
	bq, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentialsJSON(cred.JSON))
	if err != nil {
		t.Fatalf("unable to open create bq client: %s", err)
	}
	var ciao = NewFromClient(bq, "logs_dev", "test_tbl")

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
