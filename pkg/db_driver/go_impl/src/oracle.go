package main

import (
	"context"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

type oraConn struct {
	conn *go_ora.Connection
}

func (c *oraConn) Close() error { return c.conn.Close() }

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
		columns = append(columns, dbQueryColumn{name: name, columnType: rows.ColumnTypeDatabaseTypeName(i)})
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
