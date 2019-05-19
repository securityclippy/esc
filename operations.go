package esc

import (
	"context"

	"github.com/olivere/elastic"
)

func (e *ESC) UpsertInterfaceStream(objectStream chan interface{}, indexName string) error {
	err := e.upsertIndex(indexName)
	if err != nil {
		return err
	}
	bulk := e.Bulk().Index(indexName).Type("doc")
	for obj := range objectStream {
		bulk.Add(elastic.NewBulkIndexRequest().Doc(obj))
		if bulk.NumberOfActions() >= e.batchSize {
			_, err := bulk.Do(context.Background())
			if err != nil {
				return err
			}
		}
	}
	if bulk.NumberOfActions() > 0 {
		_, err := bulk.Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}
