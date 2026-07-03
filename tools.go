//go:build tools

// Package tools pins the project's external dependencies so they are retained
// by `go mod tidy` before the implementation packages import them directly.
//
// This file is excluded from normal builds via the `tools` build constraint.
// Once the pipeline packages (pkg/..., cmd/pgshadow) import these libraries
// directly, this file may be removed.
package tools

import (
	_ "github.com/google/gopacket"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/leanovate/gopter"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/segmentio/kafka-go"
	_ "golang.org/x/time/rate"
	_ "gopkg.in/yaml.v3"
)
