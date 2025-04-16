package ciao

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type Ciao struct {
	client              *bigquery.Client
	table               *bigquery.Table
	DefaultEventBuilder *EventBuilder
}

func New(ctx context.Context, file, projectId, ds, table string) (*Ciao, error) {
	client, err := bigquery.NewClient(ctx, projectId, option.WithCredentialsFile(file))
	if err != nil {
		return nil, err
	}
	var tbl = client.Dataset(ds).Table(table)
	return &Ciao{
		client: client,
		table:  tbl,
	}, nil
}

func NewFromClient(client *bigquery.Client, ds, table string) *Ciao {
	var tbl = client.Dataset(ds).Table(table)
	return &Ciao{
		client: client,
		table:  tbl,
	}
}

func (ciao *Ciao) InitDefaultEventBuilder(author string) {
	ciao.DefaultEventBuilder = ciao.NewEventBuilder(author)
}

func (ciao *Ciao) Insert(ctx context.Context, e *Event) error {
	var inserter = ciao.table.Inserter()
	return inserter.Put(ctx, e)
}

type BulkInsert struct {
	ciao      *Ciao
	buf       []*Event
	AutoFlush int
}

func (ciao *Ciao) BulkInsert(autoFlushSize int) *BulkInsert {
	return &BulkInsert{
		ciao:      ciao,
		AutoFlush: autoFlushSize,
	}
}

func (b *BulkInsert) Insert(ctx context.Context, e *Event) error {
	b.buf = append(b.buf, e)
	if b.AutoFlush > 0 && len(b.buf) >= b.AutoFlush {
		return b.Flush(ctx)
	}
	return nil
}

func (b *BulkInsert) Flush(ctx context.Context) error {
	if len(b.buf) == 0 {
		return nil
	}
	var inserter = b.ciao.table.Inserter()
	if err := inserter.Put(ctx, b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	return nil
}

/*

[
    {
        "name": "id",
        "type": "STRING",
        "mode": "REQUIRED",
        "maxLength": "64"
    },
    {
        "name": "timestamp",
        "type": "TIMESTAMP",
        "mode": "REQUIRED"
    },
    {
        "name": "type",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "subject",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "author",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "payload",
        "type": "JSON",
        "mode": "NULLABLE"
    }
]

*/
