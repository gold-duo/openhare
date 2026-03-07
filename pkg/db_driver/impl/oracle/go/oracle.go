package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct oracle_query_value_t {
  int32_t kind;
  int64_t int_value;
  uint64_t uint_value;
  float float_value;
  double double_value;
  int64_t datetime_value;
  uint8_t* bytes_value;
  int32_t bytes_len;
} oracle_query_value_t;

typedef struct oracle_query_row_t {
  oracle_query_value_t* values;
  int32_t value_count;
} oracle_query_row_t;

typedef struct oracle_query_column_t {
  char* name;
  char* column_type;
} oracle_query_column_t;

typedef struct oracle_query_header_t {
  oracle_query_column_t* columns;
  int32_t column_count;
  int64_t affected_rows;
} oracle_query_header_t;

typedef struct oracle_query_batch_t {
  oracle_query_row_t* rows;
  int32_t row_count;
  int32_t done;
} oracle_query_batch_t;
*/
import "C"

import (
	"context"
	"fmt"
	"runtime/cgo"
	"time"
	"unsafe"

	go_ora "github.com/sijms/go-ora/v2"
)

type queryColumn struct {
	Name       string `json:"name"`
	ColumnType string `json:"column_type,omitempty"`
}

type queryValue struct {
	Kind          int32
	IntValue      int64
	UintValue     uint64
	FloatValue    float32
	DoubleValue   float64
	DateTimeValue int64
	BytesValue    []byte
}

const (
	valueKindNull     int32 = 0
	valueKindBytes    int32 = 1
	valueKindInt      int32 = 2
	valueKindUint     int32 = 3
	valueKindFloat    int32 = 4
	valueKindDouble   int32 = 5
	valueKindDateTime int32 = 6
	valueKindString   int32 = 7
)

type queryHeader struct {
	Columns      []queryColumn `json:"columns"`
	AffectedRows int64         `json:"affected_rows"`
}

type queryRow struct {
	Values []queryValue `json:"values"`
}

type oracleConn struct {
	conn *go_ora.Connection
}

type queryCursor struct {
	stmt         *go_ora.Stmt
	rows         *go_ora.DataSet
	columns      []queryColumn
	currentRow   []queryValue
	affectedRows int64
}

func (c *oracleConn) close() {
	_ = c.conn.Close()
}

func (c *oracleConn) openQuery(query string) (*queryCursor, error) {
	stmt := go_ora.NewStmt(query, c.conn)
	rows, err := stmt.Query_(nil)
	if err != nil {
		stmt.Close()
		return nil, err
	}

	colNames := rows.Columns()
	cols := make([]queryColumn, 0, len(colNames))
	for i, name := range colNames {
		tName := ""
		if i < len(colNames) {
			tName = rows.ColumnTypeDatabaseTypeName(i)
		}
		cols = append(cols, queryColumn{Name: name, ColumnType: tName})
	}

	return &queryCursor{
		stmt:         stmt,
		rows:         rows,
		columns:      cols,
		currentRow:   nil,
		affectedRows: 0, // TODO: get affected rows, 暂时拿不到，需要fork改造
	}, nil
}

func (q *queryCursor) close() {
	if q.rows != nil {
		_ = q.rows.Close()
	}
	if q.stmt != nil {
		_ = q.stmt.Close()
	}
}

func (q *queryCursor) nextRow() (bool, error) {
	if !q.rows.Next_() {
		q.currentRow = nil
		if err := q.rows.Err(); err != nil {
			return false, err
		}
		return false, nil
	}

	vals := make([]any, len(q.columns))
	ptrs := make([]any, len(q.columns))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := q.rows.Scan(ptrs...); err != nil {
		return false, err
	}
	row := make([]queryValue, 0, len(vals))
	for _, v := range vals {
		row = append(row, encodeCell(v))
	}
	q.currentRow = row
	return true, nil
}

func (q *queryCursor) header() queryHeader {
	return queryHeader{
		Columns:      q.columns,
		AffectedRows: q.affectedRows,
	}
}

func (q *queryCursor) nextBatch(batchSize int) ([]queryRow, bool, error) {
	if batchSize <= 0 {
		batchSize = 256
	}
	rows := make([]queryRow, 0, batchSize)
	for len(rows) < batchSize {
		hasNext, err := q.nextRow()
		if err != nil {
			return nil, false, err
		}
		if !hasNext {
			break
		}
		rowCopy := make([]queryValue, len(q.currentRow))
		copy(rowCopy, q.currentRow)
		rows = append(rows, queryRow{Values: rowCopy})
	}
	if len(rows) == 0 {
		return nil, true, nil
	}
	return rows, false, nil
}

func encodeCell(v any) queryValue {
	if v == nil {
		return queryValue{Kind: valueKindNull}
	}
	switch vv := v.(type) {
	case []byte:
		return queryValue{Kind: valueKindBytes, BytesValue: append([]byte(nil), vv...)}
	case string:
		return queryValue{Kind: valueKindString, BytesValue: []byte(vv)}
	case time.Time:
		return queryValue{Kind: valueKindDateTime, DateTimeValue: vv.UnixMilli()}
	case int64:
		return queryValue{Kind: valueKindInt, IntValue: vv}
	case int32:
		return queryValue{Kind: valueKindInt, IntValue: int64(vv)}
	case int:
		return queryValue{Kind: valueKindInt, IntValue: int64(vv)}
	case uint64:
		return queryValue{Kind: valueKindUint, UintValue: vv}
	case uint32:
		return queryValue{Kind: valueKindUint, UintValue: uint64(vv)}
	case float64:
		return queryValue{Kind: valueKindDouble, DoubleValue: vv}
	case float32:
		return queryValue{Kind: valueKindFloat, FloatValue: vv}
	case bool:
		if vv {
			return queryValue{Kind: valueKindInt, IntValue: 1}
		}
		return queryValue{Kind: valueKindInt, IntValue: 0}
	default:
		return queryValue{Kind: valueKindString, BytesValue: []byte(fmt.Sprint(v))}
	}
}

func cStringOrNil(s string) *C.char {
	if s == "" {
		return nil
	}
	return C.CString(s)
}

func allocColumns(cols []queryColumn) *C.oracle_query_column_t {
	if len(cols) == 0 {
		return nil
	}
	ptr := C.malloc(C.size_t(len(cols)) * C.size_t(C.sizeof_oracle_query_column_t))
	arr := (*[1 << 30]C.oracle_query_column_t)(ptr)[:len(cols):len(cols)]
	for i, c := range cols {
		arr[i].name = cStringOrNil(c.Name)
		arr[i].column_type = cStringOrNil(c.ColumnType)
	}
	return (*C.oracle_query_column_t)(ptr)
}

func allocValues(values []queryValue) *C.oracle_query_value_t {
	if len(values) == 0 {
		return nil
	}
	ptr := C.malloc(C.size_t(len(values)) * C.size_t(C.sizeof_oracle_query_value_t))
	arr := (*[1 << 30]C.oracle_query_value_t)(ptr)[:len(values):len(values)]
	for i, v := range values {
		arr[i].kind = C.int32_t(v.Kind)
		arr[i].int_value = C.int64_t(v.IntValue)
		arr[i].uint_value = C.uint64_t(v.UintValue)
		arr[i].float_value = C.float(v.FloatValue)
		arr[i].double_value = C.double(v.DoubleValue)
		arr[i].datetime_value = C.int64_t(v.DateTimeValue)
		arr[i].bytes_value = nil
		arr[i].bytes_len = 0
		if len(v.BytesValue) > 0 {
			arr[i].bytes_value = (*C.uint8_t)(C.CBytes(v.BytesValue))
			arr[i].bytes_len = C.int32_t(len(v.BytesValue))
		}
	}
	return (*C.oracle_query_value_t)(ptr)
}

func allocRows(rows []queryRow) *C.oracle_query_row_t {
	if len(rows) == 0 {
		return nil
	}
	ptr := C.malloc(C.size_t(len(rows)) * C.size_t(C.sizeof_oracle_query_row_t))
	arr := (*[1 << 30]C.oracle_query_row_t)(ptr)[:len(rows):len(rows)]
	for i, r := range rows {
		arr[i].values = allocValues(r.Values)
		arr[i].value_count = C.int32_t(len(r.Values))
	}
	return (*C.oracle_query_row_t)(ptr)
}

func setErr(errOut **C.char, err error) {
	if errOut == nil {
		return
	}
	if err == nil {
		*errOut = nil
		return
	}
	*errOut = C.CString(err.Error())
}

func connFromHandle(handle int64) (*oracleConn, bool) {
	c, ok := cgo.Handle(handle).Value().(*oracleConn)
	return c, ok
}

func queryFromHandle(handle int64) (*queryCursor, bool) {
	q, ok := cgo.Handle(handle).Value().(*queryCursor)
	return q, ok
}

//export oracle_open
func oracle_open(dsn *C.char, errOut **C.char) C.int64_t {
	conn, err := go_ora.NewConnection(C.GoString(dsn), nil)
	if err != nil {
		setErr(errOut, err)
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = conn.OpenWithContext(ctx)
	if err != nil {
		setErr(errOut, err)
		return 0
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		setErr(errOut, err)
		return 0
	}

	h := cgo.NewHandle(&oracleConn{conn: conn})
	setErr(errOut, nil)
	return C.int64_t(h)
}

//export oracle_close
func oracle_close(handle C.int64_t) {
	h := cgo.Handle(handle)
	if c, ok := h.Value().(*oracleConn); ok {
		c.close()
	}
	h.Delete()
}

//export oracle_query_open
func oracle_query_open(handle C.int64_t, sqlText *C.char, errOut **C.char) C.int64_t {
	c, ok := connFromHandle(int64(handle))
	if !ok {
		setErr(errOut, fmt.Errorf("invalid connection handle: %d", handle))
		return 0
	}
	q, err := c.openQuery(C.GoString(sqlText))
	if err != nil {
		setErr(errOut, err)
		return 0
	}
	h := cgo.NewHandle(q)
	setErr(errOut, nil)
	return C.int64_t(h)
}

//export oracle_query_close
func oracle_query_close(queryHandle C.int64_t) {
	h := cgo.Handle(queryHandle)
	if q, ok := h.Value().(*queryCursor); ok {
		q.close()
	}
	h.Delete()
}

//export oracle_query_header
func oracle_query_header(queryHandle C.int64_t, errOut **C.char) *C.oracle_query_header_t {
	q, ok := queryFromHandle(int64(queryHandle))
	if !ok {
		setErr(errOut, fmt.Errorf("invalid query handle: %d", queryHandle))
		return nil
	}
	h := q.header()
	out := (*C.oracle_query_header_t)(C.malloc(C.size_t(C.sizeof_oracle_query_header_t)))
	out.columns = allocColumns(h.Columns)
	out.column_count = C.int32_t(len(h.Columns))
	out.affected_rows = C.int64_t(h.AffectedRows)
	setErr(errOut, nil)
	return out
}

//export oracle_query_next_batch
func oracle_query_next_batch(queryHandle C.int64_t, batchSize C.int32_t, errOut **C.char) *C.oracle_query_batch_t {
	q, ok := queryFromHandle(int64(queryHandle))
	if !ok {
		setErr(errOut, fmt.Errorf("invalid query handle: %d", queryHandle))
		return nil
	}
	rows, done, err := q.nextBatch(int(batchSize))
	if err != nil {
		setErr(errOut, err)
		return nil
	}
	out := (*C.oracle_query_batch_t)(C.malloc(C.size_t(C.sizeof_oracle_query_batch_t)))
	out.done = C.int32_t(0)
	out.rows = nil
	out.row_count = 0
	if done {
		out.done = C.int32_t(1)
		setErr(errOut, nil)
		return out
	}
	out.rows = allocRows(rows)
	out.row_count = C.int32_t(len(rows))
	setErr(errOut, nil)
	return out
}

//export oracle_query_free_header
func oracle_query_free_header(header *C.oracle_query_header_t) {
	if header == nil {
		return
	}
	if header.columns != nil && header.column_count > 0 {
		cols := (*[1 << 30]C.oracle_query_column_t)(unsafe.Pointer(header.columns))[:int(header.column_count):int(header.column_count)]
		for i := range cols {
			if cols[i].name != nil {
				C.free(unsafe.Pointer(cols[i].name))
			}
			if cols[i].column_type != nil {
				C.free(unsafe.Pointer(cols[i].column_type))
			}
		}
		C.free(unsafe.Pointer(header.columns))
	}
	C.free(unsafe.Pointer(header))
}

//export oracle_query_free_batch
func oracle_query_free_batch(batch *C.oracle_query_batch_t) {
	if batch == nil {
		return
	}
	if batch.rows != nil && batch.row_count > 0 {
		rows := (*[1 << 30]C.oracle_query_row_t)(unsafe.Pointer(batch.rows))[:int(batch.row_count):int(batch.row_count)]
		for i := range rows {
			if rows[i].values != nil && rows[i].value_count > 0 {
				values := (*[1 << 30]C.oracle_query_value_t)(unsafe.Pointer(rows[i].values))[:int(rows[i].value_count):int(rows[i].value_count)]
				for j := range values {
					if values[j].bytes_value != nil {
						C.free(unsafe.Pointer(values[j].bytes_value))
					}
				}
				C.free(unsafe.Pointer(rows[i].values))
			}
		}
		C.free(unsafe.Pointer(batch.rows))
	}
	C.free(unsafe.Pointer(batch))
}

//export oracle_free_string
func oracle_free_string(s *C.char) {
	if s == nil {
		return
	}
	C.free(unsafe.Pointer(s))
}

func main() {}
