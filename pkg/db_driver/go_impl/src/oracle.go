package main

import (
	"context"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

type oraConn struct {
	conn *go_ora.Connection
}

func (c *oraConn) Close() error { return c.conn.Close() }

func oracleDataType(typeName string) int32 {
	t := strings.ToUpper(typeName)

	// Oracle number types
	if strings.Contains(t, "NUMBER") || strings.Contains(t, "NUMERIC") ||
		strings.Contains(t, "INTEGER") || strings.Contains(t, "INT") ||
		strings.Contains(t, "FLOAT") || strings.Contains(t, "DOUBLE") ||
		strings.Contains(t, "PRECISION") {
		return dataTypeNumber
	}

	// Oracle character types
	if strings.Contains(t, "CHAR") || strings.Contains(t, "VARCHAR") ||
		strings.Contains(t, "CLOB") || strings.Contains(t, "NCLOB") ||
		strings.Contains(t, "LONG") || strings.Contains(t, "XMLTYPE") {
		return dataTypeChar
	}

	// Oracle date/time types
	if strings.Contains(t, "DATE") || strings.Contains(t, "TIMESTAMP") ||
		strings.Contains(t, "INTERVAL") {
		return dataTypeTime
	}

	// Oracle binary types
	if strings.Contains(t, "BLOB") || strings.Contains(t, "BFILE") ||
		strings.Contains(t, "RAW") || strings.Contains(t, "LONG RAW") {
		return dataTypeBlob
	}

	// Oracle JSON
	if strings.Contains(t, "JSON") {
		return dataTypeJson
	}

	return dataTypeChar
}

func (c *oraConn) OpenQuery(sql string) (rowCursor, error) {
	stmt := go_ora.NewStmt(sql, c.conn)
	rows, err := stmt.Query_(nil)
	if err != nil {
		stmt.Close()
		return nil, err
	}
	names := rows.Columns()
	columns := make([]dbQueryColumn, 0, len(names))
	for i, name := range names {
		typeName := rows.ColumnTypeDatabaseTypeName(i)
		columns = append(columns, dbQueryColumn{
			name:     name,
			dataType: oracleDataType(typeName),
		})
	}
	return &oraCur{
		stmt: stmt, rows: rows, columns: columns,
	}, nil
}

type oraCur struct {
	stmt    *go_ora.Stmt
	rows    *go_ora.DataSet
	columns []dbQueryColumn
}

func (q *oraCur) Close() error {
	var err error
	if q.rows != nil {
		err = q.rows.Close()
	}
	if q.stmt != nil {
		if e := q.stmt.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (q *oraCur) Header() *dbQueryHeader {
	return &dbQueryHeader{columns: q.columns}
}

func (q *oraCur) NextRow() (*dbQueryRow, bool, error) {
	if !q.rows.Next_() {
		if err := q.rows.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	raw := make([]any, len(q.columns))
	scanArgs := make([]any, len(q.columns))
	for i := range raw {
		scanArgs[i] = &raw[i]
	}
	if err := q.rows.Scan(scanArgs...); err != nil {
		return nil, false, err
	}

	values := make([]dbQueryValue, 0, len(raw))
	for _, value := range raw {
		values = append(values, buildQueryValue(value))
	}
	return &dbQueryRow{values: values}, true, nil
}

func openOracleConn(dsn string) (driverConn, error) {
	conn, err := go_ora.NewConnection(dsn, nil)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.OpenWithContext(ctx); err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &oraConn{conn: conn}, nil
}
