package gocqlmem

import (
	"fmt"
	"sync"
)

type Keyspace struct {
	TableMap        map[string]*Table
	WithReplication []*KeyValuePair
	Lock            sync.RWMutex
}

func newKeyspace() *Keyspace {
	return &Keyspace{
		TableMap:        map[string]*Table{},
		WithReplication: make([]*KeyValuePair, 0),
	}
}

func (ks *Keyspace) createTable(cmd *CommandCreateTable) error {
	ks.Lock.Lock()
	defer ks.Lock.Unlock()

	_, alreadyExists := ks.TableMap[cmd.TableName]
	if alreadyExists && cmd.IfNotExists {
		return nil
	}
	if alreadyExists && !cmd.IfNotExists {
		return fmt.Errorf("cannot create table %s, it already exists and no IF NOT EXISTS were specified", cmd.TableName)
	}
	newTable, err := newTable(cmd)
	if err != nil {
		return fmt.Errorf("cannot create table %s: %s", cmd.TableName, err.Error())
	}
	ks.TableMap[cmd.TableName] = newTable
	return nil
}

func (ks *Keyspace) dropTable(cmd *CommandDropTable) error {
	ks.Lock.Lock()
	defer ks.Lock.Unlock()

	t, alreadyExists := ks.TableMap[cmd.TableName]
	if !alreadyExists && cmd.IfExists {
		return nil
	}
	if !alreadyExists && !cmd.IfExists {
		return fmt.Errorf("cannot drop table %s, it was not found and no IF EXISTS were specified", cmd.TableName)
	}

	t.Lock.Lock()
	delete(ks.TableMap, cmd.TableName)
	t.Lock.Unlock()

	return nil
}
