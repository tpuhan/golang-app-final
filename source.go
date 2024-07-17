package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery/storage/managedwriter"
)

const (
	project = "bigquerytestdefault"
	dataset = "siddag_summer2024"
	table   = "table5"
)

func main() {
	ctx := context.Background()

	// Read JSON file
	jsonFile, err := os.Open("data.json")
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Failed to read JSON file: %v", err)
	}
	//Unmarshal JSON into JSON map
	var rows []map[string]interface{}
	if err := json.Unmarshal(byteValue, &rows); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Create client
	client, err := managedwriter.NewClient(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	//use getDescriptors to get the message descriptor, and descriptor proto
	md, descriptor := getDescriptors(ctx, client, project, dataset, table)

	// Hard Coded Table reference (will fix)
	tableReference := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	// Create stream using NewManagedStream
	managedStream, err := client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithDestinationTable(tableReference),
		//use the descriptor proto when creating the new managed stream
		managedwriter.WithSchemaDescriptor(descriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		log.Fatal("NewManagedStream: ", err)
	}
	defer managedStream.Close()

	// Serialize rows
	// Checking Results Async (will check at end)
	var offset int64
	for _, row := range rows {
		//transform each row of data into binary using the json_to_binary function and the message descriptor from the getDescriptors function
		buf, err := json_to_binary(md, row)
		if err != nil {
			log.Fatal("converting from json to binary failed: ", err)
		}

		var data [][]byte
		data = append(data, buf)

		stream, err := managedStream.AppendRows(ctx, data, managedwriter.WithOffset(offset))
		if err != nil {
			log.Fatal("AppendRows: ", err)
		}
		recvOffset, err := stream.GetResult(ctx)
		if err != nil {
			log.Fatalf("append returned error: %v", err)
		}
		log.Printf("Successfully appended data at offset %d.\n", recvOffset)
		offset++
	}
	log.Println("Done")
}
