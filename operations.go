package esc

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic"
)

func (e *ESC) UpsertInterfaceStream(objectStream chan interface{}, indexName string) error {
	err := e.upsertIndex(indexName)
	if err != nil {
		return err
	}

	bp, err := e.Client.BulkProcessor().
		Name("bulk-worker").
		Workers(1).
		BulkActions(10).
		BulkSize(2 << 20).
		FlushInterval(10 * time.Second).
		Do(context.Background())

	if err != nil {
		return err
	}

	i := 0
	for obj := range objectStream {
		r := elastic.NewBulkIndexRequest().Index(indexName).Type("doc").Doc(obj)
		bp.Add(r)
		i ++
	}

	err = bp.Flush()
	if err != nil {
		return err
	}

	stats := bp.Stats()


	fmt.Printf("Number of times flush has been invoked: %d\n", stats.Flushed)
	fmt.Printf("Number of times workers committed reqs: %d\n", stats.Committed)
	fmt.Printf("Number of requests indexed            : %d\n", stats.Indexed)
	fmt.Printf("Number of requests reported as created: %d\n", stats.Created)
	fmt.Printf("Number of requests reported as updated: %d\n", stats.Updated)
	fmt.Printf("Number of requests reported as success: %d\n", stats.Succeeded)
	fmt.Printf("Number of requests reported as failed : %d\n", stats.Failed)

	for i, w := range stats.Workers {
		fmt.Printf("Worker %d: Number of requests queued: %d\n", i, w.Queued)
		fmt.Printf("           Last response time       : %v\n", w.LastDuration)
	}
	err = bp.Close()
	if err != nil {
		return err
	}
	return nil
}

func (e *ESC) UpsertInterface(obj interface{}, index string) error {

	if err := e.upsertIndex(index); err != nil {
		return err
	}

	_, err := e.Client.Index().Index(index).BodyJson(obj).Type("doc").Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

