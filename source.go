// https://github.com/alexflint/bigquery-storage-api-example/blob/main/bigquery-storage-api.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	storage "cloud.google.com/go/bigquery/storage/apiv1beta2"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
	"google.golang.org/protobuf/proto"
)

const (
	project = "bigquerytestdefault"
	dataset = "siddag_summer2024"
	table   = "tanishqa_summer2024_table"
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

	var rows []*Row
	if err := json.Unmarshal(byteValue, &rows); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Create client
	client, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Get protobuf descriptor
	var row Row
	descriptor, err := adapt.NormalizeDescriptor((&row).ProtoReflect().Descriptor())
	if err != nil {
		log.Fatal("NormalizeDescriptor: ", err)
	}

	// Create write stream
	// Don't need to create a committed stream
	resp, err := client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table),
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_COMMITTED,
		},
	})
	if err != nil {
		log.Fatal("CreateWriteStream: ", err)
	}

	// Get stream
	stream, err := client.AppendRows(ctx)
	if err != nil {
		log.Fatal("AppendRows: ", err)
	}

	// Serialize rows
	var data [][]byte
	for _, row := range rows {
		buf, err := proto.Marshal(row)
		if err != nil {
			log.Fatal("proto.Marshal: ", err)
		}
		data = append(data, buf)
	}

	// Send to BigQuery
	err = stream.Send(&storagepb.AppendRowsRequest{
		WriteStream: resp.Name,
		Rows: &storagepb.AppendRowsRequest_ProtoRows{
			ProtoRows: &storagepb.AppendRowsRequest_ProtoData{
				// Protocol buffer schema
				WriterSchema: &storagepb.ProtoSchema{
					ProtoDescriptor: descriptor,
				},
				// Protocol buffer data
				Rows: &storagepb.ProtoRows{
					SerializedRows: data, // Serialized protocol buffer data
				},
			},
		},
	})
	if err != nil {
		log.Fatal("AppendRows.Send: ", err)
	}

	// Finalize
	r, err := stream.Recv()
	if err != nil {
		log.Fatal("AppendRows.Recv: ", err)
	}

	if rErr := r.GetError(); rErr != nil {
		log.Printf("result was error: %v", rErr)
	} else if rResult := r.GetAppendResult(); rResult != nil {
		log.Printf("now stream offset is %d", rResult.Offset.Value)
	}

	log.Println("Done")
}
