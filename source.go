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
	table   = "raahi_summer2024table2"
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
		managedwriter.WithType(managedwriter.DefaultStream),
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
	var data [][]byte
	for _, row := range rows {
		//transform each row of data into binary using the json_to_binary function and the message descriptor from the getDescriptors function
		buf, err := json_to_binary(md, row)
		if err != nil {
			log.Fatal("converting from json to binary failed: ", err)
		}
		data = append(data, buf)
	}

	// Checking Results Async (will check at end)
	var results []*managedwriter.AppendResult

	// Appending Rows
	stream, err := managedStream.AppendRows(ctx, data)
	if err != nil {
		log.Fatal("AppendRows: ", err)
	}
	results = append(results, stream)

	// Checks if all results were successful
	for k, v := range results {
		// GetResult blocks until we receive a response from the API.
		recvOffset, err := v.GetResult(ctx)
		if err != nil {
			log.Fatalf("append %d returned error: %v", k, err)
		}
		log.Printf("Successfully appended data at offset %d.\n", recvOffset)
	}

	log.Println("Done")
}
