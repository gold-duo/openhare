package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	pgInt2        = "int2"
	pgInt4        = "int4"
	pgInt8        = "int8"
	pgFloat4      = "float4"
	pgFloat8      = "float8"
	pgNumeric     = "numeric"
	pgSmallint    = "smallint"
	pgInteger     = "integer"
	pgBigint      = "bigint"
	pgReal        = "real"
	pgDouble      = "double precision"
	pgSerial      = "serial"
	pgBigserial   = "bigserial"
	pgSmallserial = "smallserial"

	pgChar         = "character"
	pgVarchar      = "character varying"
	pgText         = "text"
	pgName         = "name"
	pgBpchar       = "bpchar"
	pgVarcharAlias = "varchar"
	pgCharAlias    = "char"

	pgDate        = "date"
	pgTime        = "time"
	pgTimeTz      = "time with time zone"
	pgTimestamp   = "timestamp"
	pgTimestampTz = "timestamp with time zone"
	pgTimestamptz = "timestamptz"
	pgInterval    = "interval"

	pgBytea = "bytea"

	pgBool    = "boolean"
	pgBoolean = "bool"

	pgJson  = "json"
	pgJsonb = "jsonb"

	pgUuid = "uuid"

	pgArray = "ARRAY"
)

type pgConn struct {
	conn *pgx.Conn
}

func (c *pgConn) Close() error {
	return c.conn.Close(context.Background())
}

func pgDataType(typeName string) int32 {
	switch typeName {
	case pgInt2, pgInt4, pgInt8, pgFloat4, pgFloat8, pgNumeric,
		pgSmallint, pgInteger, pgBigint, pgReal, pgDouble,
		pgSerial, pgBigserial, pgSmallserial:
		return dataTypeNumber

	case pgChar, pgVarchar, pgText, pgName, pgBpchar,
		pgVarcharAlias, pgCharAlias, pgUuid:
		return dataTypeChar

	case pgDate, pgTime, pgTimeTz, pgTimestamp, pgTimestampTz,
		pgTimestamptz, pgInterval:
		return dataTypeTime

	case pgBytea:
		return dataTypeBlob

	case pgBool, pgBoolean:
		return dataTypeDataSet

	case pgJson, pgJsonb:
		return dataTypeJson

	default:
		if len(typeName) > 0 && typeName[len(typeName)-1] == ']' {
			return dataTypeBlob
		}
		return dataTypeChar
	}
}

func (c *pgConn) OpenQuery(sqlText string) (rowCursor, error) {
	rows, err := c.conn.Query(context.Background(), sqlText)
	if err != nil {
		return nil, err
	}

	fields := rows.FieldDescriptions()
	columns := make([]dbQueryColumn, 0, len(fields))
	for _, f := range fields {
		typeName := c.getTypeName(f.DataTypeOID)
		columns = append(columns, dbQueryColumn{
			name:     string(f.Name),
			dataType: pgDataType(typeName),
		})
	}

	cur := &pgCur{
		rows:         rows,
		columns:      columns,
		affectedRows: rows.CommandTag().RowsAffected(),
	}
	return cur, nil
}

func (c *pgConn) getTypeName(oid uint32) string {
	if dt, ok := c.conn.TypeMap().TypeForOID(oid); ok {
		return dt.Name
	}
	return ""
}

type pgCur struct {
	rows         pgx.Rows
	columns      []dbQueryColumn
	affectedRows int64
}

func (q *pgCur) Close() error {
	q.rows.Close()
	return nil
}

func (q *pgCur) Header() *dbQueryHeader {
	return &dbQueryHeader{
		columns:      q.columns,
		affectedRows: q.affectedRows,
	}
}

func (q *pgCur) NextRow() (*dbQueryRow, bool, error) {
	if !q.rows.Next() {
		if err := q.rows.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	n := len(q.columns)
	raw := make([]any, n)
	for i := range raw {
		raw[i] = new(any)
	}

	if err := q.rows.Scan(raw...); err != nil {
		return nil, false, err
	}

	values := make([]dbQueryValue, 0, n)
	for _, v := range raw {
		ptr := v.(*any)
		values = append(values, buildQueryValue(*ptr))
	}
	return &dbQueryRow{values: values}, true, nil
}

func openPgConn(dsn string) (driverConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}

	return &pgConn{conn: conn}, nil
}
