package reclib

import (
	fmt "fmt"

	rec "github.com/Ferroman/recutils/rec"
)

// RecDB represents a rec database
type RecDB struct {
	handle *rec.Database
}

// NewRecDB creates a new RecDB
func NewRecDB() *RecDB {
	db, err := rec.NewDatabase()
	if err != nil {
		return nil
	}
	return &RecDB{handle: db}
}

func (r *RecDB) LoadFile(filename string) error {
	return r.handle.LoadFile(filename)
}

// Size returns the number of record sets in the database
func (r *RecDB) Size() int {
	return int(r.handle.Size())
}

func (r *RecDB) GetRecordSet(index int) (*rec.RecordSet, error) {
	rs, err := r.handle.GetRecordSet(index)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// PrintRecordSet prints all records in the record set
func (r *RecDB) PrintRecordSet(rs *rec.RecordSet) {
	fmt.Printf("Record Set Type: %s\n", rs.GetType())
	fmt.Printf("Number of Records: %d\n", rs.NumRecords())
}

// Query performs a query on the database
func (r *RecDB) Query(params rec.QueryParams) (*rec.RecordSet, error) {
	return r.handle.Query(params)
}
