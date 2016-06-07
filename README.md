# qg

[![Build Status](https://travis-ci.org/achiku/qg.svg?branch=master)](https://travis-ci.org/achiku/qg)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/achiku/qg/master/LICENSE)

Ruby [Que](https://github.com/chanks/que) implementation in Go. This library is almost a fork of [que-go](https://github.com/bgentry/que-go), the great work of [bgentry](https://github.com/bgentry).


## Why created

- I wanted `database/sql` aware version of `que-go` so that our original app can cooperate well with it.
- I wanted transaction that can be injected to a Job since it make WorkFunc tests much easier.
- I wanted to customize `Job.Delete()` func to move worked job record to different history table.
- I wanted better logging functionality
