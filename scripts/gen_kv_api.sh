export PATH="$PATH:$(go env GOPATH)/bin"
protoc --proto_path=api/proto --go_out=pkg/go --go_opt=paths=source_relative --go-grpc_out=pkg/go --go-grpc_opt=paths=source_relative api/proto/kvstore/v1/kvstore.proto
