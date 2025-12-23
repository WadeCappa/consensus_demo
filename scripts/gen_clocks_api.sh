export PATH="$PATH:$(go env GOPATH)/bin"
protoc --proto_path=api/proto --go_out=gen/go --go_opt=paths=source_relative --go-grpc_out=gen/go --go-grpc_opt=paths=source_relative api/proto/clocks/v1/clocks.proto
