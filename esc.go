package esc

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/olivere/elastic"
	v4 "github.com/olivere/elastic/aws/v4"
	log "github.com/sirupsen/logrus"
)

type ESC struct {
	*elastic.Client
	batchSize int
}

// New returns a new ESC client
func New(host, username, password string, useInsecure bool) *ESC {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: useInsecure,
		},
	}
	scheme := "https"
	if !strings.HasPrefix(host, "https") {
		scheme = "http"
	}
	httpClient := &http.Client{Transport: tr}
	var client *elastic.Client
	var err error

	if username != "" {
		client, err = elastic.NewClient(
			elastic.SetURL(host),
			elastic.SetScheme(scheme),
			elastic.SetHttpClient(httpClient),
			elastic.SetSniff(false),
			elastic.SetBasicAuth(username, password),
		)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		client, err = elastic.NewClient(
			elastic.SetURL(host),
			elastic.SetScheme(scheme),
			elastic.SetHttpClient(httpClient),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	esc := &ESC{
		Client:    client,
		batchSize: 250,
	}
	return esc
}

// NewAWS returns a new ESC client using environmental credentials to authenticate to an AWS elasticsearch service
func NewAWS(host string) *ESC {
	creds := credentials.NewEnvCredentials()
	signingClient := v4.NewV4SigningClient(creds, os.Getenv("AWS_REGION"))
	client, err := elastic.NewClient(
		elastic.SetURL(host),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetHttpClient(signingClient),
	)
	if err != nil {
		log.Fatal(err)
	}
	esc := &ESC{
		Client:    client,
		batchSize: 250,
	}

	return esc
}

func (e *ESC) upsertIndex(indexName string) error {
	indices, err := e.IndexNames()
	if err != nil {
		return err
	}
	for _, i := range indices {
		if i == indexName {
			return nil
		}
	}
	_, err = e.CreateIndex(indexName).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (e *ESC) SetBatchSize(size int) {
	e.batchSize = size
}
