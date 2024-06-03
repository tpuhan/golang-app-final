# Simple GoLang Connector
A simple GoLang Connector that takes in information from a JSON and inputs it into a supplied BigQuery Table.\
First, create a directory in your file system that houses these files in this repository.\
Ensure you have Go by running `go version`. If not follow steps to ensure Go is installed correctly.\
Ensure you have the files in this repository downloaded in a directory of your choice. These files should have the necessary configurations to run the code. If there's an error refer to the troubleshooting part of the document.\
To run, simply run the following commands in order:\
`go clean -modcache`\
`go mod tidy`\
`go build`\
`./golang-app1`

## Troubleshooting
Check `proto-gen-go` installation by using the following command:\
  `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`\
Ensure the Binary Directory is in your Path by running `export PATH=$PATH:$(go env GOPATH)/bin` and then `source ~/.bashrc`. Verify proto-gen-go is installed by running `protoc-gen-go --version` in your terminal.\
Finally, run `protoc --go_out=. row.proto` to refresh the file and rerun the program.

Next Steps:
- Examples in the Go Client and managedwriter
- Don't have to unmarshal it
- GetWS (gets schema from destination), should happen internally
- Figure out how to use default stream (Tanishqa)
- Understand more ab asynch sends per row (some examples) (Tanishqa)
	- Don't want to send and wait
- Enable retry logic
- Halt if in cases of invalid data etc (pt2, capture in dead letter queue)
- JSON Handling without .go/.proto step
