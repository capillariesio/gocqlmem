package gocqlmem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableSelectOrderBy(t *testing.T) {
	table := Table{
		ColumnDefs: []*ColumnDef{
			{"col1", PrimaryKeyClustering, DataTypeText, ClusteringOrderAsc},
			{"col2", PrimaryKeyClustering, DataTypeBigint, ClusteringOrderDesc},
			{"col3", PrimaryKeyNone, DataTypeBool, ClusteringOrderNone},
		},
		ColumnValues: [][]any{
			{"a", "a", "b", "c"},
			{3, 2, 1, 1},
		},
		ColumnDefMap: map[string]int{"col1": 0, "col2": 1},
	}

	seq, err := table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderAsc},
		{"col2", ClusteringOrderDesc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{0, 1, 2, 3}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderDesc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 1, 0}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderAsc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{1, 0, 2, 3}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderDesc},
		{"col2", ClusteringOrderDesc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 0, 1}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col3", ClusteringOrderAsc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Contains(t, err.Error(), "cannot process ORDER BY col3, this field is not a clustering key")
}
