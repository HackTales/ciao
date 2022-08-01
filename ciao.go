package ciao

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type Ciao struct {
	client *bigquery.Client
	table  *bigquery.Table
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

func (ciao *Ciao) Insert(ctx context.Context, e *Event) error {
	var inserter = ciao.table.Inserter()
	return inserter.Put(ctx, e)
}
