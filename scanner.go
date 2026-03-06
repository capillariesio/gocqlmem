package gocqlmem

import "fmt"

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}
type iterScanner struct {
	iter  *Iter
	cols  []interface{}
	valid bool
}

func (is *iterScanner) Next() bool {
	if is.iter.pos >= len(is.iter.RetrievedValues) {
		return false
	}
	for i := range len(is.iter.RetrievedNames) {
		is.cols[i] = is.iter.RetrievedValues[is.iter.pos][i]
	}
	is.iter.pos++
	return true
}

func (is *iterScanner) Scan(dest ...interface{}) error {
	if len(dest) != len(is.cols) {
		return fmt.Errorf("cannot scan %d columns to dest of length %d", len(is.cols), len(dest))
	}

	// TODO: implement more conversions

	for i := range len(is.cols) {
		switch typedDestPtr := dest[i].(type) {
		case *int64:
			switch typedColVal := is.cols[i].(type) {
			case int64:
				*typedDestPtr = typedColVal
			}
		}
	}
	return nil
}
func (is *iterScanner) Err() error {
	iter := is.iter
	is.iter = nil
	is.cols = nil
	is.valid = false
	return iter.Close()
}
