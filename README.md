## Blobs

This repository contains all the modules needed for an application. From creation of a blob, write it to a particular storage sink to read that blob back anytime you want. One can use a particular module of this library to directly save a blob through a store or send it [Haystack-Agent](https://github.com/ExpediaDotCom/haystack-agent) which then dispatches a blob via a dispatcher.

You can look at our sample projects for how to save a blob through a simple web application:
* [Blobs Example](https://github.com/mchandramouli/blobs-example)
* [Span Blob Context Example](https://github.com/vaibhavsawhney/span-blob-example)

## Table of content

- [Setup](#setup)
- [Blobs Core](#blobs-core)
- [Haystack Blobs](#haystack-blobs)
	* [Client](#client)
	* [Agent Provider/Server](#agent-provider-or-server)
	* [Dispatchers](#dispatchers)
	* [Models](#models)
	* [Span-Blob Context](#span-blob-context)
	* [Reverse Proxy](#reverse-proxy)
- [Stores](#stores)
	* [File Store](#file-store)
	* [S3 Store](#s3-store)

## Setup

##### Clone

Use the following command to clone the repository including the submodules present in it:

`git clone --recursive git@github.com:mchandramouli/blobs.git`

##### Build

Use the following command to build the repository:

`mvn clean package`

## Blobs Core

This module contains all the core classes needed to instrument the creation of the blobs and then start the process of writing it to a [store](#stores). Only the Blob Model is created by the `blob.proto` present in [blobs-grpc-models](#models) module inside the package `com.expedia.www.blobs.model`.


## Haystack Blobs

This module contains all the sub modules needed by an application to interact with [Haystack-Agent](https://github.com/ExpediaDotCom/haystack-agent) for dispatching a blob though a dispatcher present in it. The blob is sent to [Haystack-Agent](https://github.com/ExpediaDotCom/haystack-agent) over [GRPC](https://grpc.io/) via a client and then received by the agent via a server already running there. The blob received is then dispatched through a [dispatcher](#dispatchers) integrated in the agent.

Below is a sample configuration for blobs in [Haystack-Agent](https://github.com/ExpediaDotCom/haystack-agent):
```
agents {
  blobs {
    enabled = true
    port = 34001
    maxBlobSizeInKB = 1536
    dispatchers {
      s3 {
        keepAlive = true
        maxOutstandingRequests = 50
        shouldWaitForUpload = false
        maxConnections = 50
        retryCount = 1
        bucketName = "haystack-blobs"
        region = "us-east-1"
        awsAccessKey = "accessKey"
        awsSecretKey = "secretKey"
      }
    }
  }
}

```

## Client

This is a GRPC client that is used to send the blob to wherever the server resides.

The client is initiated using a builder which can either take a [ManagedChannel](https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannel.html) object or `address` and `port` of the server running.

The server in the haystack-agent is running at a default port 34001.

## Agent Provider or Server
We have one agent provider today that is loaded depending upon the configuration as above.

#### Blob Proto Agent
A `blob proto agent` is present to receive the blob from a GRPC client and then dispatch it to the dispatchers. This agent listens as a GRPC server on a configurable port and accepts the protobuf blob from the clients. The blob agent is already implemented in the open source repo and it supports S3 dispatcher.

## Dispatchers

#### S3 Dispatcher

S3 dispatcher uses aws s3 sdk and its TransferManager apis and we require following mandatory configuration properties for it to work.

1. region - aws region for e.g. us-west-2
2. bucketName - aws s3 bucket name
3. awsAccessKey and awsSecretKey - Optional, use them if want to use static AWS credentials.
4. maxOutstandingRequests - maximum parallel uploads to s3, else RateLimitException is thrown and sent to the client
5. keepAlive - Optional, TCP keep alive for aws client. Default: `false`
6. shouldWaitForUpload - Optional, define is it should wait for complete upload of blob to S3. Default: `false`		
7. maxConnections - Optional,maximum connections for aws client
8. retryCount - Optional, maximum error retry for upload


## Models

The blob-grpc-models is used to compile `protos` into Java files. The protos are present in a git submodule i.e. [haystack-idl](https://github.com/ExpediaDotCom/haystack-idl).

## Span-Blob Context

This module contains the `SpanBlobContext` that will be used by `BlobFactory`(inside [blobs core](blobs-core) module) to get a `BlobWriter`(inside [blobs core](blobs-core) module) for writing a blob to haystack-agent.
Whenever a blob is sent to Haystack-Agent, the key produced for that blob is saved as a tag inside the span to be used later for reading from haystack-ui.

The name of the tag can be according to the `BlobType`(inside [blobs core](blobs-core) module). There can be 2 types of tags to store a blob key inside span:
1. request-blob
2. response-blob

## Reverse Proxy

To run the HTTP proxy to GRPC service locally please follow the below steps as we have not automated everything for now. We have used [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway) to generate the stub and proxy files.

1. Inside `reverse-proxy`, build using `go build`
2. To run the server use `./main -http-port=:34002 -grpc-server-endpoint=localhost:34001`. 

	The command line arguments are optional with default value of `http-port` as `:34002` and `grpc-server-endpoint` as `localhost:34001`

##### To generate your own stub and proxy files use the following steps:

1. Copy `blob.proto` and `blobAgent.proto` from [haystack-idl](https://github.com/ExpediaDotCom/haystack-idl) to `reverse-proxy` folder.

2. Inside `blobAgent.proto` replace
```
rpc readBlobAsString(BlobSearch) returns (FormattedBlobReadResponse);
```
with
```
rpc readBlobAsString(BlobSearch) returns (FormattedBlobReadResponse) {
       option (google.api.http) = {
            get: "/getBlob/{key}"
            response_body: "data"
        };
   }
```

Note: You need not commit this change.

2. Run the following two commands. Make sure you have go installed on you local. If not, use `brew install go`, `export PATH="$PATH:$GOPATH/bin"`(specify `$GOPATH` if not already present)
```
go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway v1.9.4
go get -u github.com/golang/protobuf/protoc-gen-go v1.9.4
```

3. Create a folder named `blob` inside the `reverse-proxy` module, if not already present.

4. Inside `reverse-proxy`, generate gRPC stub using
```
protoc -I/usr/local/include -I. \
  -I$GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.9.4/third_party/googleapis \
  --plugin=protoc-gen-go=$GOPATH/bin/protoc-gen-go \
  --go_out=plugins=grpc:blob/. \
  ./blobAgent.proto
```

5. Inside `reverse-proxy`, generate reverse-proxy using `protoc-gen-grpc-gateway`
```
protoc -I/usr/local/include -I. \
  -I$GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.9.4/third_party/googleapis \
  --plugin=protoc-gen-grpc-gateway=$GOPATH/bin/protoc-gen-grpc-gateway  \
  --grpc-gateway_out=logtostderr=true:blob/. \
  ./blobAgent.proto
```

6. Inside `reverse-proxy`, generate `Blob` model
```
protoc -I/usr/local/include -I. \
  -I$GOPATH/pkg/mod/github.com/grpc-ecosystem/grpc-gateway\@v1.9.4/third_party/googleapis \
  --plugin=protoc-gen-go=$GOPATH/bin/protoc-gen-go \
  --go_out=plugins=grpc:blob/. \
  ./blob.proto
```

7. Inside `reverse-proxy` call `go build`

8. To run the server use `./main -http-port=:34002 -grpc-server-endpoint=localhost:34001`. 

	The command line arguments are optional with default value of `http-port` as `:34002` and `grpc-server-endpoint` as `localhost:34001`


## Stores

The stores are the sinks that can directly be integrated with a micro-service to dump a blob to a defined location.

#### File Store

This store is used to dump a blob to a local directory of a system where the micro-service is running.

#### S3 Store

This store will dump a blob to a given S3 bucket directly without using Haystack-Agent's dispatcher.