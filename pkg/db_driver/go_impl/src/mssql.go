package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

type mssqlConn struct {
	db *sql.DB
}

func (c *mssqlConn) Close() error {
	return c.db.Close()
}

func mssqlDataType(typeName string) int32 {
	t := strings.ToUpper(typeName)

	// MSSQL: check datetime first (contains "INT" in DATETIME)
	if strings.Contains(t, "DATE") || strings.Contains(t, "TIME") {
		return dataTypeTime
	}

	// MSSQL number types
	if strings.Contains(t, "INT") && !strings.Contains(t, "POINT") ||
		strings.Contains(t, "DECIMAL") || strings.Contains(t, "NUMERIC") ||
		strings.Contains(t, "MONEY") || strings.Contains(t, "SMALLMONEY") ||
		strings.Contains(t, "FLOAT") || strings.Contains(t, "REAL") {
		return dataTypeNumber
	}

	// MSSQL binary types
	if strings.Contains(t, "BINARY") || strings.Contains(t, "VARBINARY") ||
		strings.Contains(t, "IMAGE") {
		return dataTypeBlob
	}

	// MSSQL special types
	if t == "BIT" {
		return dataTypeDataSet
	}
	if strings.Contains(t, "XML") {
		return dataTypeJson
	}

	// MSSQL character types
	if strings.Contains(t, "CHAR") || strings.Contains(t, "VARCHAR") ||
		strings.Contains(t, "TEXT") || strings.Contains(t, "NTEXT") ||
		strings.Contains(t, "UNIQUEIDENTIFIER") {
		return dataTypeChar
	}

	return dataTypeChar
}

func (c *mssqlConn) OpenQuery(sqlText string) (rowCursor, error) {
	rows, err := c.db.QueryContext(context.Background(), sqlText)
	if err != nil {
		return nil, err
	}

	names, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return nil, err
	}

	columns := make([]dbQueryColumn, 0, len(names))
	for i, name := range names {
		dbType := ""
		if i < len(colTypes) && colTypes[i] != nil {
			dbType = colTypes[i].DatabaseTypeName()
		}
		columns = append(columns, dbQueryColumn{
			name:     name,
			dataType: mssqlDataType(dbType),
		})
	}

	return &mssqlCur{rows: rows, columns: columns}, nil
}

type mssqlCur struct {
	rows    *sql.Rows
	columns []dbQueryColumn
}

func (q *mssqlCur) Close() error {
	return q.rows.Close()
}

func (q *mssqlCur) Header() *dbQueryHeader {
	return &dbQueryHeader{columns: q.columns}
}

func (q *mssqlCur) NextRow() (*dbQueryRow, bool, error) {
	if !q.rows.Next() {
		if err := q.rows.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	n := len(q.columns)
	raw := make([]any, n)
	scanArgs := make([]any, n)
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

func openMssqlConn(dsn string) (driverConn, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &mssqlConn{db: db}, nil
}
