# qg

[![Build Status](https://travis-ci.org/achiku/qg.svg?branch=master)](https://travis-ci.org/achiku/qg)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/achiku/qg/master/LICENSE)

Ruby [Que](https://github.com/chanks/que) implementation in Go. This library is almost a fork of [que-go](https://github.com/bgentry/que-go), the great work of [bgentry](https://github.com/bgentry).


## Why created

First of all, [Que](https://github.com/chanks/que), and it's Go port [que-go](https://github.com/bgentry/que-go) are really great libraries, which can simplify small to mid scale application with some sort of asynchronous tasks/jobs by avoiding to add another moving part if you are using PostgreSQL for main RDBMS. However, as I use `que-go` to develop my application written in Go, there are some functionalities that `que-go` doesn't provide. The following is an list of functionalities I'm going to add to `qg`.

- `database/sql` compatible version of enqueue functions so that many other database libraries can work with it.
- Transaction can be injected to a `Job` to make `WorkFunc` tests much easier.
- Customizable `Job.Delete()`, `Job.Error()` to give more flexibility.
- Synchronous execution option in `Client.Enqueue` and `Client.EnqueueInTx` for easy development.
- Better logger interface to be able to switch whatever loggers developers want. 

This library is still under heavy development, and might significantly change APIs.


## Great Resources

- https://github.com/chanks/que/tree/master/docs
- https://brandur.org/postgres-queues
