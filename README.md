This package implements in-memory gocql. Can be useful for unit testing. Uses eval package for agg calculations.

In your code, instead of gocql objects Iter, Query and Session, use shim interfaces:
- gocqlshims.Iter
- gocqlshims.Query
- gocqlshims.Session

So the caller should not now either original gocql implementation is called, or gocqlmem implementation.

Sample code creating a gocqlshims.Session depending on the configuration (test/prod):


```
package main

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/capillariesio/gocqlmem"
	"github.com/capillariesio/gocqlmem/gocqlshims"
)

func NewSession(cfg *SomeConfig) (gocqlshims.Session, error) {
	if cfg.IsTest {
        return gocqlmem.NewGocqlmemSession(), nil
	}

	dataCluster := gocql.NewCluster(cfg.Hosts...)
	dataCluster.Port = cfg.Port
    ...
    cassandraSession, err := dataCluster.CreateSession()
    gocqlshimsSession := gocqlshims.NewGocqlSession(cassandraSession)
    ...
    return gocqlshimsSession, nil
}
```