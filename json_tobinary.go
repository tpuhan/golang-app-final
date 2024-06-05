package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// This function handles getting data on the schema of the table data is being written to. It uses GetWriteStream as well as adapt functions to get the relevant descriptors. The inputs for this function are the context, managed writer client,
// projectID, datasetID, and tableID. getDescriptors returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto(which sends the table schema to the stream when created with
// NewManagedStream as shown in line 54 and 63 of source.go).

// Used in line 48 of source.go
func getDescriptors(ctx context.Context, managed_writer_client *managedwriter.Client, project string, dataset string, table string) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto) {
	//create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	//create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	//call getwritestream to get data on the table
	table_data, err2 := managed_writer_client.GetWriteStream(ctx, &req)
	if err2 != nil {
		log.Fatalf("getWriteStream command failed: %v", err2)
	}
	//get the schema from table data
	table_schema := table_data.GetTableSchema()
	//storage schema ->proto descriptor
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		log.Fatalf("adapt.StorageSchemaToDescriptor: %v", err)
	}
	//proto descriptor -> messageDescriptor
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		log.Fatalf("adapted descriptor is not a message descriptor")
	}

	//messageDescriptor -> descriptor proto
	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		log.Fatalf("NormalizeDescriptor: %v", err)
	}

	return messageDescriptor, dp
}

// This function handles the data transformation from JSON to binary for a single json row. In practice, this function would be utilized within a loop to transform all of the json data. The inputs are the message descriptor
//(which was returned in getDescriptors) as well as the relevant jsonRow (of type map[string]interface{} - which is the output of unmarshalling the json data). The outputs are the corresponding binary data as well as any error that occurs.
//Various json, protojson, and proto marshalling/unmarshalling functions are utilized to transform the data. The message descriptor is used when creating an empty new proto message (which the data is placed into).

// Used in line 69 of source.go
func json_to_binary(message_descriptor protoreflect.MessageDescriptor, jsonRow map[string]interface{}) ([]byte, error) {
	//JSON map -> JSON byte
	row, err := json.Marshal(jsonRow)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json map: %w", err)
	}
	//create empty message
	message := dynamicpb.NewMessage(message_descriptor)

	// First, json->proto message
	err = protojson.Unmarshal(row, message)
	if err != nil {
		return nil, fmt.Errorf("failed to Unmarshal json message: %w", err)
	}

	// Then, proto message -> bytes.
	b, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto bytes: %w ", err)
	}

	return b, nil
}
