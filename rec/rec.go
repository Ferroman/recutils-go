package rec

/*
#cgo CFLAGS: -I/usr/include
#cgo LDFLAGS: -lrec
#include <rec.h>
#include <stdbool.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// QueryParams contains parameters for database queries
type QueryParams struct {
	Type          string
	Join          string
	Index         *C.size_t
	SelectionExpr SelectionExpr
	Random        int
	FieldExpr     FieldExpr
	Flags         int
}

// Database represents a rec_db_t database structure
type Database struct {
	db C.rec_db_t
}

// RecordSet represents a rec_rset_t record set structure
type RecordSet struct {
	rst C.rec_rset_t
}

// Record represents a rec_record_t record structure
type Record struct {
	rcd C.rec_record_t
}

// Field represents a rec_field_t field structure
type Field struct {
	fld C.rec_field_t
}

// SelectionExpr represents a rec_sex_t selection expression structure
type SelectionExpr struct {
	sx C.rec_sex_t
}

// FieldExpr represents a rec_fex_t field expression structure
type FieldExpr struct {
	fx C.rec_fex_t
}

// FieldExprElem represents a rec_fex_elem_t field expression element
type FieldExprElem struct {
	fxel C.rec_fex_elem_t
}

// Comment represents a rec_comment_t comment structure
type Comment struct {
	cmnt C.rec_comment_t
}

// Buffer represents a rec_buf_t buffer structure
type Buffer struct {
	buf C.rec_buf_t
}

// Constants for query flags
const (
	REC_Q_DESCRIPTOR = 1 << iota
	REC_Q_ICASE
)

// Constants for field actions
const (
	REC_SET_ACT_RENAME = iota
	REC_SET_ACT_SET
	REC_SET_ACT_ADD
	REC_SET_ACT_SETADD
	REC_SET_ACT_DELETE
	REC_SET_ACT_COMMENT
)

// NewDatabase creates a new empty database
func NewDatabase() (*Database, error) {
	db := C.rec_db_new()
	if db == nil {
		return nil, fmt.Errorf("failed to create new database")
	}
	return &Database{db: db}, nil
}

// Size returns the number of record sets in the database
func (d *Database) Size() int {
	return int(C.rec_db_size(d.db))
}

// LoadFile loads records from a file into the database
func (d *Database) LoadFile(filename string) error {
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	file := C.fopen(cFilename, C.CString("r"))
	if file == nil {
		return fmt.Errorf("failed to open file: %s", filename)
	}
	defer C.fclose(file)

	parser := C.rec_parser_new(file, cFilename)
	if parser == nil {
		return fmt.Errorf("failed to create parser")
	}
	defer C.rec_parser_destroy(parser)

	// Clear existing database
	C.rec_db_destroy(d.db)

	success := C.rec_parse_db(parser, &d.db)
	if !success {
		return fmt.Errorf("failed to parse database")
	}

	return nil
}

// GetRecordSet returns the record set at the given position
func (d *Database) GetRecordSet(position int) (*RecordSet, error) {
	rst := C.rec_db_get_rset(d.db, C.size_t(position))
	if rst == nil {
		return nil, fmt.Errorf("no record set at position %d", position)
	}
	return &RecordSet{rst: rst}, nil
}

// InsertRecordSet inserts a record set at the given position
func (d *Database) InsertRecordSet(rs *RecordSet, position int) error {
	success := C.rec_db_insert_rset(d.db, rs.rst, C.size_t(position))
	if !success {
		return fmt.Errorf("failed to insert record set")
	}
	return nil
}

// RemoveRecordSet removes the record set at the given position
func (d *Database) RemoveRecordSet(position int) error {
	success := C.rec_db_remove_rset(d.db, C.size_t(position))
	if !success {
		return fmt.Errorf("failed to remove record set")
	}
	return nil
}

// HasType checks if a record set of the given type exists
func (d *Database) HasType(typeName string) bool {
	cType := C.CString(typeName)
	defer C.free(unsafe.Pointer(cType))
	return bool(C.rec_db_type_p(d.db, cType))
}

// GetRecordSetByType returns the record set with the given type
func (d *Database) GetRecordSetByType(typeName string) (*RecordSet, error) {
	cType := C.CString(typeName)
	defer C.free(unsafe.Pointer(cType))

	rst := C.rec_db_get_rset_by_type(d.db, cType)
	if rst == nil {
		return nil, fmt.Errorf("no record set with type %s", typeName)
	}
	return &RecordSet{rst: rst}, nil
}

// Query performs a database query with the given parameters
func (d *Database) Query(params QueryParams) (*RecordSet, error) {
	cType := C.CString(params.Type)
	defer C.free(unsafe.Pointer(cType))

	cJoin := C.CString(params.Join)
	defer C.free(unsafe.Pointer(cJoin))

	rst := C.rec_db_query(d.db,
		cType,
		cJoin,
		params.Index,
		params.SelectionExpr.sx,
		nil, // fast_string
		C.size_t(params.Random),
		params.FieldExpr.fx,
		nil, // password
		nil, // group_by
		nil, // sort_by
		C.int(params.Flags))

	if rst == nil {
		return nil, fmt.Errorf("query failed")
	}

	return &RecordSet{rst: rst}, nil
}

// Record Set methods

// NumRecords returns the number of records in the record set
func (rs *RecordSet) NumRecords() int {
	return int(C.rec_rset_num_records(rs.rst))
}

// GetDescriptor returns the record descriptor of the record set
func (rs *RecordSet) GetDescriptor() (*Record, error) {
	rcd := C.rec_rset_descriptor(rs.rst)
	if rcd == nil {
		return nil, fmt.Errorf("record set has no descriptor")
	}
	return &Record{rcd: rcd}, nil
}

// GetType returns the type name of the record set
func (rs *RecordSet) GetType() string {
	return C.GoString(C.rec_rset_type(rs.rst))
}

// Record methods

// ContainsValue checks if the record contains a field with the given value
func (r *Record) ContainsValue(value string, caseInsensitive bool) bool {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	return bool(C.rec_record_contains_value(r.rcd, cValue, C.bool(caseInsensitive)))
}

// ContainsField checks if the record contains a field with the given name and value
func (r *Record) ContainsField(fieldName, fieldValue string) bool {
	cName := C.CString(fieldName)
	defer C.free(unsafe.Pointer(cName))

	cValue := C.CString(fieldValue)
	defer C.free(unsafe.Pointer(cValue))

	return bool(C.rec_record_contains_field(r.rcd, cName, cValue))
}

// Enhanced Query performs a more detailed database query with additional parameters
func (d *Database) EnhancedQuery(
	typeName string,
	join string,
	index *C.size_t,
	selExpr SelectionExpr,
	fastString string,
	random int,
	fieldExpr FieldExpr,
	password string,
	groupBy FieldExpr,
	sortBy FieldExpr,
	flags int,
) (*RecordSet, error) {
	var cType, cJoin, cFastString, cPassword *C.char

	if typeName != "" {
		cType = C.CString(typeName)
		defer C.free(unsafe.Pointer(cType))
	}
	if join != "" {
		cJoin = C.CString(join)
		defer C.free(unsafe.Pointer(cJoin))
	}
	if fastString != "" {
		cFastString = C.CString(fastString)
		defer C.free(unsafe.Pointer(cFastString))
	}
	if password != "" {
		cPassword = C.CString(password)
		defer C.free(unsafe.Pointer(cPassword))
	}

	rst := C.rec_db_query(
		d.db,
		cType,
		cJoin,
		index,
		selExpr.sx,
		cFastString,
		C.size_t(random),
		fieldExpr.fx,
		cPassword,
		groupBy.fx,
		sortBy.fx,
		C.int(flags),
	)

	if rst == nil {
		return nil, fmt.Errorf("query failed")
	}

	return &RecordSet{rst: rst}, nil
}

// Insert adds or replaces records in the database
func (d *Database) Insert(
	typeName string,
	index *C.size_t,
	selExpr SelectionExpr,
	fastString string,
	random int,
	password string,
	record *Record,
	flags int,
) error {
	var cType, cFastString, cPassword *C.char

	if typeName != "" {
		cType = C.CString(typeName)
		defer C.free(unsafe.Pointer(cType))
	}
	if fastString != "" {
		cFastString = C.CString(fastString)
		defer C.free(unsafe.Pointer(cFastString))
	}
	if password != "" {
		cPassword = C.CString(password)
		defer C.free(unsafe.Pointer(cPassword))
	}

	success := C.rec_db_insert(
		d.db,
		cType,
		index,
		selExpr.sx,
		cFastString,
		C.size_t(random),
		cPassword,
		record.rcd,
		C.int(flags),
	)

	if !success {
		return fmt.Errorf("insert operation failed")
	}
	return nil
}

// Delete removes or comments out records from the database
func (d *Database) Delete(
	typeName string,
	index *C.size_t,
	selExpr SelectionExpr,
	fastString string,
	random int,
	flags int,
) error {
	var cType, cFastString *C.char

	if typeName != "" {
		cType = C.CString(typeName)
		defer C.free(unsafe.Pointer(cType))
	}
	if fastString != "" {
		cFastString = C.CString(fastString)
		defer C.free(unsafe.Pointer(cFastString))
	}

	success := C.rec_db_delete(
		d.db,
		cType,
		index,
		selExpr.sx,
		cFastString,
		C.size_t(random),
		C.int(flags),
	)

	if !success {
		return fmt.Errorf("delete operation failed")
	}
	return nil
}

// SetFields manipulates fields in selected records
func (d *Database) SetFields(
	typeName string,
	index *C.size_t,
	selExpr SelectionExpr,
	fastString string,
	random int,
	fieldExpr FieldExpr,
	action int,
	actionArg string,
	flags int,
) error {
	var cType, cFastString, cActionArg *C.char

	if typeName != "" {
		cType = C.CString(typeName)
		defer C.free(unsafe.Pointer(cType))
	}
	if fastString != "" {
		cFastString = C.CString(fastString)
		defer C.free(unsafe.Pointer(cFastString))
	}
	if actionArg != "" {
		cActionArg = C.CString(actionArg)
		defer C.free(unsafe.Pointer(cActionArg))
	}

	success := C.rec_db_set(
		d.db,
		cType,
		index,
		selExpr.sx,
		cFastString,
		C.size_t(random),
		fieldExpr.fx,
		C.int(action),
		cActionArg,
		C.int(flags),
	)

	if !success {
		return fmt.Errorf("set fields operation failed")
	}
	return nil
}

// CheckIntegrity verifies the integrity of all record sets in the database
func (d *Database) CheckIntegrity(checkDescriptors bool, remoteDescriptors bool, errors *Buffer) int {
	return int(C.rec_int_check_db(
		d.db,
		C.bool(checkDescriptors),
		C.bool(remoteDescriptors),
		errors.buf,
	))
}
