package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
)

type AdInfo struct {
	Id     string `json:"id" dynamodbav:"id"`
	Result string `json:"result" dynamodbav:"result"`
}

type Config struct {
	Region    string `default:"us-east-1"`
	TableName string `default:"ads"`
}

type advertiser struct {
	awsSession *session.Session
	c          Config
	dbClient   *dynamodb.DynamoDB
}

type Advertiser advertiser

func main() {
	var conf Config
	err := envconfig.Process("advertiser", &conf)
	if err != nil {
		log.Fatal(err.Error())
	}

	ad := &Advertiser{
		c: conf,
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(conf.Region),
	}))

	ad.awsSession = sess
	ad.dbClient = dynamodb.New(ad.awsSession)

	r := mux.NewRouter()
	r.HandleFunc("/find", ad.Find).Methods("GET")
	fmt.Println("Starting up on 8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func (a *Advertiser) Find(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	id := req.URL.Query().Get("id")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*1000))
	defer cancel()

	input := &dynamodb.GetItemInput{
		TableName: aws.String(a.c.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: aws.String(id)},
		},
	}

	r, err := a.dbClient.GetItemWithContext(ctx, input)
	if err != nil {
		log.Println(err)
		fmt.Fprintln(w, "{}")
		return
	}

	var adInfo AdInfo
	err = dynamodbattribute.UnmarshalMap(r.Item, &adInfo)
	if err != nil {
		fmt.Println(err)
		fmt.Fprintln(w, "{}")
		return
	}

	response, err := json.Marshal(adInfo)
	if err != nil {
		fmt.Println(err)
		fmt.Fprintln(w, "{}")
		return
	}

	fmt.Fprintln(w, string(response))
}
