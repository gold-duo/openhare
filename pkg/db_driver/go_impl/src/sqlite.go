package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	sqliteInteger  = "INTEGER"
	sqliteInt      = "INT"
	sqliteBigInt   = "BIGINT"
	sqliteSmallInt = "SMALLINT"
	sqliteTinyInt  = "TINYINT"
	sqliteReal     = "REAL"
	sqliteDouble   = "DOUBLE"
	sqliteFloat    = "FLOAT"
	sqliteNumeric  = "NUMERIC"
	sqliteDecimal  = "DECIMAL"

	sqliteText    = "TEXT"
	sqliteChar    = "CHAR"
	sqliteVarchar = "VARCHAR"
	sqliteClob    = "CLOB"

	sqliteBlob = "BLOB"

	sqliteDate      = "DATE"
	sqliteDatetime  = "DATETIME"
	sqliteTime      = "TIME"
	sqliteTimestamp = "TIMESTAMP"

	sqliteJson = "JSON"
)

// sqliteConn 直接使用 go-sqlite3 的 driver.Conn（*sqlite3.SQLiteConn），不经过 database/sql.DB。
type sqliteConn struct {
	conn *sqlite3.SQLiteConn
}

func (c *sqliteConn) Close() error {
	return c.conn.Close()
}

func sqliteDataType(typeName string) int32 {
	t := strings.ToUpper(strings.TrimSpace(typeName))

	if strings.Contains(t, "INT") {
		return dataTypeNumber
	}
	if strings.Contains(t, "REAL") || strings.Contains(t, "FLOA") ||
		strings.Contains(t, "DOUB") || strings.Contains(t, "NUMERIC") ||
		strings.Contains(t, "DECIMAL") {
		return dataTypeNumber
	}
	if strings.Contains(t, "CHAR") || strings.Contains(t, "CLOB") ||
		strings.Contains(t, "TEXT") {
		return dataTypeChar
	}
	if strings.Contains(t, "DATE") || strings.Contains(t, "TIME") {
		return dataTypeTime
	}
	if strings.Contains(t, "BLOB") {
		return dataTypeBlob
	}
	if strings.Contains(t, "JSON") {
		return dataTypeJson
	}

	return dataTypeChar
}

func (c *sqliteConn) OpenQuery(sqlText string) (rowCursor, error) {
	ctx := context.Background()
	st, err := c.conn.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	stmt, ok := st.(*sqlite3.SQLiteStmt)
	if !ok {
		_ = st.Close()
		return nil, fmt.Errorf("sqlite: unexpected driver.Stmt type %T", st)
	}
	dr, err := stmt.QueryContext(ctx, nil)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	sr, ok := dr.(*sqlite3.SQLiteRows)
	if !ok {
		_ = dr.Close()
		return nil, fmt.Errorf("sqlite: unexpected driver.Rows type %T", dr)
	}

	names := sr.Columns()
	columns := make([]dbQueryColumn, 0, len(names))
	var rowsIface driver.Rows = sr
	ct, hasCT := rowsIface.(driver.RowsColumnTypeDatabaseTypeName)
	for i, name := range names {
		dbType := ""
		if hasCT {
			dbType = ct.ColumnTypeDatabaseTypeName(i)
		}
		columns = append(columns, dbQueryColumn{
			name:     name,
			dataType: sqliteDataType(dbType),
		})
	}

	cur := &sqliteCur{conn: c.conn, rows: sr, columns: columns}
	// INSERT/UPDATE/DELETE 无结果列时，Query 返回后尚未 step，sqlite3_changes 要在语句执行后才有效。
	// 先拉完空结果，首包 HEADER 才能带上与 Exec 一致的受影响行数。
	if len(columns) == 0 {
		if err := cur.stepNoColumnResult(); err != nil {
			_ = sr.Close()
			return nil, err
		}
	}
	return cur, nil
}

// stepNoColumnResult 对无列结果集执行一次 Next 直至 EOF，使 sqlite3_changes 在首包 HEADER 前已更新。
func (q *sqliteCur) stepNoColumnResult() error {
	dest := []driver.Value{}
	if err := q.rows.Next(dest); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	q.done = true
	q.affectedRows = q.conn.DriverChanges()
	return nil
}

type sqliteCur struct {
	conn         *sqlite3.SQLiteConn
	rows         *sqlite3.SQLiteRows
	columns      []dbQueryColumn
	affectedRows int64
	done         bool
}

func (q *sqliteCur) Close() error {
	return q.rows.Close()
}

func (q *sqliteCur) Header() *dbQueryHeader {
	return &dbQueryHeader{
		columns:      q.columns,
		affectedRows: q.affectedRows,
	}
}

func (q *sqliteCur) NextRow() (*dbQueryRow, bool, error) {
	if q.done {
		return nil, false, nil
	}
	dest := make([]driver.Value, len(q.columns))
	if err := q.rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			q.done = true
			return nil, false, nil
		}
		return nil, false, err
	}

	values := make([]dbQueryValue, 0, len(dest))
	for _, v := range dest {
		values = append(values, buildQueryValue(v))
	}
	return &dbQueryRow{values: values}, true, nil
}

func openSqliteConn(dsn string) (driverConn, error) {
	d := &sqlite3.SQLiteDriver{}
	dc, err := d.Open(dsn)
	if err != nil {
		return nil, err
	}
	conn, ok := dc.(*sqlite3.SQLiteConn)
	if !ok {
		_ = dc.Close()
		return nil, fmt.Errorf("sqlite: unexpected driver.Conn type %T", dc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &sqliteConn{conn: conn}, nil
}
