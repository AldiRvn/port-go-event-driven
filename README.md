# port-go-event-driven

## My Setup

- Linux
- go1.25.5
- Docker

## Overview

Golang Test File for Publish 1000 data JSON to Kafka in under 2 seconds.

## Test Steps

- `docker compose up`
- Add `0.0.0.0 broker` to `/etc/hosts`
- `go test -v -run ^Test_publisher$ `
- Check latest data in Kafka UI:

http://localhost:8080/ui/clusters/local/all-topics/users/messages?limit=10&page=0&seekDirection=BACKWARD&seekType=LATEST