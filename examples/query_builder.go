package main

import (
	_ "embed"
	"fmt"
	"github.com/clarkmcc/gomongo/qb"
	"go.mongodb.org/mongo-driver/bson"
)

//go:embed example_query_1.json
var template string

func main() {
	query, err := qb.Build[bson.M](template, map[string]any{
		"id":   "000000000000000000000000",
		"name": "/john|jane/i",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(query)
}
