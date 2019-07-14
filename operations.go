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

	//bulk := e.Bulk().Index(indexName).Type("doc")
	i := 0
	for obj := range objectStream {
		r := elastic.NewBulkIndexRequest().Index(indexName).Type("doc").Doc(obj)
		bp.Add(r)
		i ++
		//bulk.Add(elastic.NewBulkIndexRequest().Doc(obj))
		//if bulk.NumberOfActions() >= e.batchSize {
			//result, err := bulk.Do(context.Background())
			//if err != nil {
				//return err
			//}
			//if len(result.Failed()) > 0 {
				//for _, r := range result.Items {
					//for k, v := range r {
						//if v.Error != nil {
							//fmt.Printf("K: %+v, V: %+v\n", k, v)
						//}
					//}
				//}
			//}
		//}
	}

	err = bp.Flush()
	if err != nil {
		return err
	}
	//if bulk.NumberOfActions() > 0 {
		//result, err := bulk.Do(context.Background())
		//if err != nil {
			//return err
		//}
		//if len(result.Failed()) > 0 {
			//fmt.Printf("%d failed during upload\n", len(result.Failed()))
		//}
	//}
	//fmt.Printf("Uploaded: %d to elasticsearch\n", i)

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
