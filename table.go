package gocqlmem

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sync"
)

type PrimaryKeyType int

const (
	PrimaryKeyPartition PrimaryKeyType = iota
	PrimaryKeyClustering
	PrimaryKeyNone
)

type DataTypeType int

const (
	DataTypeBigint DataTypeType = iota
	DataTypeText
	DataTypeDecimal
	DataTypeFloat
	DataTypeBool
	DataTypeTimestamp
	DataTypeUnknown
)

func stringToDataType(s string) (DataTypeType, error) {
	switch s {
	case "BIGINT":
		return DataTypeBigint, nil
	case "TEXT":
		return DataTypeText, nil
	case "DECIMAL":
		return DataTypeDecimal, nil
	case "FLOAT":
		return DataTypeFloat, nil
	case "BOOLEAN":
		return DataTypeBool, nil
	case "TIMESTAMP":
		return DataTypeTimestamp, nil
	default:
		return DataTypeUnknown, fmt.Errorf("unsupported data type %s", s)
	}
}

type ColumnDef struct {
	Name            string
	PrimaryKey      PrimaryKeyType
	DataType        DataTypeType
	ClusteringOrder ClusteringOrderType
}

type Table struct {
	ColumnDefs   []*ColumnDef // Partition,clustering,other
	ColumnValues [][]any      // Partition,clustering,other
	ColumnTokens [][]int64    // Partition keys only
	ColumnDefMap map[string]int
	Lock         sync.RWMutex
}

func createColDef(name string, mapColType map[string]DataTypeType, mapColClusteringOrder map[string]ClusteringOrderType) (*ColumnDef, error) {
	dataType, ok := mapColType[name]
	if !ok {
		return nil, fmt.Errorf("cannot find definition for column %s", name)
	}
	clusteringOrder, ok := mapColClusteringOrder[name]
	if !ok {
		clusteringOrder = ClusteringOrderNone
	}
	if !ok {
		return nil, fmt.Errorf("cannot find definition for column %s", name)
	}
	return &ColumnDef{
		Name:            name,
		PrimaryKey:      PrimaryKeyPartition,
		DataType:        dataType,
		ClusteringOrder: clusteringOrder,
	}, nil
}

func newTable(cmd *CommandCreateTable) (*Table, error) {
	t := Table{
		ColumnDefs:   make([]*ColumnDef, len(cmd.ColumnDefs)),
		ColumnValues: make([][]any, len(cmd.ColumnDefs)),
		ColumnTokens: make([][]int64, len(cmd.PartitionKeyColumns)),
		ColumnDefMap: map[string]int{},
	}

	mapColType := map[string]DataTypeType{}
	var err error
	for _, createTableColDef := range cmd.ColumnDefs {
		if mapColType[createTableColDef.Name], err = stringToDataType(createTableColDef.Type); err != nil {
			return nil, err
		}
	}

	mapColClusteringOrder := map[string]ClusteringOrderType{}
	for _, orderByField := range cmd.ClusteringOrderBy {
		// Check definition is present
		if _, ok := mapColType[orderByField.FieldName]; !ok {
			return nil, fmt.Errorf("cannot find definition for clustering order column %s", orderByField.FieldName)
		}
		// Check it's a clustering column
		var isClustering bool
		for _, name := range cmd.ClusteringKeyColumns {
			if orderByField.FieldName == name {
				isClustering = true
				break
			}
		}
		if !isClustering {
			return nil, fmt.Errorf("clustering order column %s is not in the list of clustering keys %v", orderByField.FieldName, cmd.ClusteringKeyColumns)
		}
		// Save ASC/DESC to temp map
		mapColClusteringOrder[orderByField.FieldName] = orderByField.ClusteringOrder
	}

	colDefIdx := 0
	t.ColumnDefMap = map[string]int{}
	// Partition columns first
	for _, name := range cmd.PartitionKeyColumns {
		if t.ColumnDefs[colDefIdx], err = createColDef(name, mapColType, mapColClusteringOrder); err != nil {
			return nil, err
		}
		t.ColumnDefMap[name] = colDefIdx
		colDefIdx++
	}
	// Clustering columns next
	for _, name := range cmd.ClusteringKeyColumns {
		if t.ColumnDefs[colDefIdx], err = createColDef(name, mapColType, mapColClusteringOrder); err != nil {
			return nil, err
		}
		t.ColumnDefMap[name] = colDefIdx
		colDefIdx++
	}
	// All other columns next, in the order of appearance in the CREATE TABLE cmd
	for _, createTableColDef := range cmd.ColumnDefs {
		if _, ok := t.ColumnDefMap[createTableColDef.Name]; !ok {
			if t.ColumnDefs[colDefIdx], err = createColDef(createTableColDef.Name, mapColType, mapColClusteringOrder); err != nil {
				return nil, err
			}
			t.ColumnDefMap[createTableColDef.Name] = colDefIdx
			colDefIdx++
		}
	}

	return &t, nil
}

func (t *Table) getClusteringKeyOrderByName(name string) ClusteringOrderType {
	for _, colDef := range t.ColumnDefs {
		if name == colDef.Name {
			return colDef.ClusteringOrder
		}
	}
	return ClusteringOrderNone
}

type ClusteringKeyEntry struct {
	Idx int
	Key string
}

func (t *Table) getRowSequenceFromColumnDefAndSelectOrderBy(orderByFieldsFromSelect []*OrderByField) ([]int, error) {
	totalRows := len(t.ColumnValues[0])
	tempClusteringKey := make([]ClusteringKeyEntry, totalRows)
	for _, orderByFieldFromSelect := range orderByFieldsFromSelect {
		// Each field from SELECT ORDER by must be a clustering key
		tableClusteringOrder := t.getClusteringKeyOrderByName(orderByFieldFromSelect.FieldName)
		if tableClusteringOrder == ClusteringOrderNone {
			return nil, fmt.Errorf("cannot process ORDER BY %s, this field is not a clustering key", orderByFieldFromSelect.FieldName)
		}
		colIdx := t.ColumnDefMap[orderByFieldFromSelect.FieldName]
		var lastVal any
		var lastTempClusteringKeySegment int
		for i := range totalRows {
			if lastVal == nil {
				lastVal = t.ColumnValues[colIdx][i]
				if tableClusteringOrder == orderByFieldFromSelect.ClusteringOrder {
					lastTempClusteringKeySegment = 0
				} else {
					lastTempClusteringKeySegment = math.MaxInt32
				}
			} else {
				if t.ColumnValues[colIdx][i] != lastVal {
					if tableClusteringOrder == orderByFieldFromSelect.ClusteringOrder {
						lastTempClusteringKeySegment++
					} else {
						lastTempClusteringKeySegment--
					}
				}
			}
			if colIdx == 0 {
				tempClusteringKey[i] = ClusteringKeyEntry{Idx: i, Key: fmt.Sprintf("0x%08X", lastTempClusteringKeySegment)}
			} else {
				tempClusteringKey[i].Key = fmt.Sprintf("%s0x%08X", tempClusteringKey[i].Key, lastTempClusteringKeySegment)
			}
		}
	}

	slices.SortFunc(tempClusteringKey, func(e1, e2 ClusteringKeyEntry) int {
		return cmp.Compare(e1.Key, e2.Key)
	})

	result := make([]int, len(tempClusteringKey))
	for i := range len(tempClusteringKey) {
		result[i] = tempClusteringKey[i].Idx
	}
	return result, nil
}

// func (t *Table) select(cmd *CommandSelect) error {

// }
