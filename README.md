Go client library for Cherami
=============================

Cherami is a distributed, scalable, durable, and highly available message queue system we developed at Uber Engineering to transport asynchronous tasks.

cherami-client-go is the go client library for Cherami.

Clone this repo
---------------
Make sure you clone this repo into the correct location.

`git clone git@github.com:uber/cherami-client-go.git $GOPATH/src/github.com/uber/cherami-client-go`
`pushd $GOPATH/src/github.com/uber/cherami-client-go`


Development
-----------
The cherami-client-go repo specifically holds the client library for Cherami, whose thrift APIs are defined in cherami-thrift repo. This repo can be used to talk to Cherami server once the cherami server is up and running.

The repo also holds an `example` which can be executed against the cherami server running locally.

In order to use the example in this repo, the following dependencies needs to be addressed:
1. Make certain that `thrift` (OSX: `brew install thrift`) and `glide` are in your path (above).
2. Make sure that cherami server is up and running by cloning the `cherami-server` repo and following the instructions on that repo.

Once we have the aforementioned steps, one can build the `example` by running:
`make bins`

In order to use `cherami-client-go` as a library in an application which wants to talk to Cherami, in the consuming repo, take in the updated go-client (`github.com/uber/cherami-client-go`) as a package in `glide.yaml`.

Documentation
--------------

Interested in learning more about Cherami? Read the blog post:
[eng.uber.com.cherami](https://eng.uber.com/cherami/)
