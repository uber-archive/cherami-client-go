Go client library for Cherami [![Build Status](https://travis-ci.org/uber/cherami-client-go.svg?branch=master)](https://travis-ci.org/uber/cherami-client-go) [![Coverage Status](https://coveralls.io/repos/uber/cherami-client-go/badge.svg?branch=master&service=github)](https://coveralls.io/github/uber/cherami-client-go?branch=master)
=============================

[Cherami](https://eng.uber.com/cherami/) is a distributed, scalable, durable, and highly available message queue system we developed at Uber Engineering to transport asynchronous tasks.

(This project is deprecated and not maintained.)

`cherami-client-go` is the Go client library for Cherami.

How to Use
----------
Make sure you clone this repo into the correct location.

`git clone git@github.com:uber/cherami-client-go.git $GOPATH/src/github.com/uber/cherami-client-go`

Development
-----------
The cherami-client-go repo specifically holds the client library for Cherami. This repo can be used to talk to Cherami server once the cherami server is up and running.

The repo also holds an `example.go`. It demonstrates some basic operations and runs against a locally running Cherami server.

In order to use the example in this repo, the following dependencies needs to be addressed:
1. You need `glide` in your path.
2. Make sure that Cherami server is up and running by cloning the `cherami-server` repo and following the instructions on that repo.

Once we have the aforementioned steps, one can build the `example` by running:
`make bins`

In order to use `cherami-client-go` as a library in an application, you can just take in the client (`github.com/uber/cherami-client-go`) as a package in `glide.yaml`.

Contributing
------------
We'd love your help in making Cherami great. If you find a bug or need a new feature on the cherami go client, please open an issue and we will respond as fast as we can. If you want to implement new feature(s) and/or fix bug(s) yourself, open a pull request with the appropriate unit tests and we will merge it after review.

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes.

Documentation
--------------
Interested in learning more about Cherami? Read the blog post:
[eng.uber.com/cherami](https://eng.uber.com/cherami/)

License
-------
MIT License, please see [LICENSE](https://github.com/uber/cherami-client-go/blob/master/LICENSE) for details.
