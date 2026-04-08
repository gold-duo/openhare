package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// Oracle 类型名称常量（来源：github.com/sijms/go-ora/v2）
// 参考：go_ora 驱动源码中的 TNSType 枚举定义
const (
	// Number types
	oraNUMBER   = "NUMBER"
	oraBInteger = "BInteger"
	oraFLOAT    = "FLOAT"
	oraUINT     = "UINT"
	oraIBFloat  = "IBFloat"
	oraIBDouble = "IBDouble"
	oraBFloat   = "BFloat"
	oraBDouble  = "BDouble"

	// Character types
	oraNCHAR          = "NCHAR"
	oraVARCHAR        = "VARCHAR"
	oraLONG           = "LONG"
	oraLongVarChar    = "LongVarChar"
	oraCHAR           = "CHAR"
	oraCHARZ          = "CHARZ"
	oraOCIString      = "OCIString"
	oraOCIClobLocator = "OCIClobLocator"

	// Date/Time types
	oraDATE             = "DATE"
	oraOCIDate          = "OCIDate"
	oraTimeStampDTY     = "TimeStampDTY"
	oraTimeStampTZ_DTY  = "TimeStampTZ_DTY"
	oraIntervalYM_DTY   = "IntervalYM_DTY"
	oraIntervalDS_DTY   = "IntervalDS_DTY"
	oraTimeTZ           = "TimeTZ"
	oraTIMESTAMP        = "TIMESTAMP"
	oraTIMESTAMPTZ      = "TIMESTAMPTZ"
	oraIntervalYM       = "IntervalYM"
	oraIntervalDS       = "IntervalDS"
	oraTimeStampLTZ_DTY = "TimeStampLTZ_DTY"
	oraTimeStampeLTZ    = "TimeStampeLTZ"

	// Binary types
	oraRAW            = "RAW"
	oraLongRaw        = "LongRaw"
	oraVarRaw         = "VarRaw"
	oraLongVarRaw     = "LongVarRaw"
	oraOCIBlobLocator = "OCIBlobLocator"
	oraOCIFileLocator = "OCIFileLocator"

	// JSON/XML types
	oraXMLType    = "XMLType"
	oraOCIXMLType = "OCIXMLType"

	// Special types
	oraREFCURSOR = "REFCURSOR"
	oraRESULTSET = "RESULTSET"
)

// oraConn 直接使用 go-ora 的 *Connection，不经过 database/sql.DB。
type oraConn struct {
	conn *go_ora.Connection
}

func (c *oraConn) Close() error { return c.conn.Close() }

func oracleDataType(typeName string) int32 {
	switch typeName {
	// Number types
	case oraNUMBER, oraBInteger, oraFLOAT, oraUINT, oraIBFloat, oraIBDouble, oraBFloat, oraBDouble:
		return dataTypeNumber

	// Character types
	case oraNCHAR, oraVARCHAR, oraLONG, oraLongVarChar, oraCHAR, oraCHARZ, oraOCIString, oraOCIClobLocator:
		return dataTypeChar

	// Date/Time types
	case oraDATE, oraOCIDate, oraTimeStampDTY, oraTimeStampTZ_DTY, oraIntervalYM_DTY,
		oraIntervalDS_DTY, oraTimeTZ, oraTIMESTAMP, oraTIMESTAMPTZ, oraIntervalYM,
		oraIntervalDS, oraTimeStampLTZ_DTY, oraTimeStampeLTZ:
		return dataTypeTime

	// Binary types
	case oraRAW, oraLongRaw, oraVarRaw, oraLongVarRaw, oraOCIBlobLocator, oraOCIFileLocator:
		return dataTypeBlob

	// JSON/XML types
	case oraXMLType, oraOCIXMLType:
		return dataTypeJson

	// Special types
	case oraREFCURSOR, oraRESULTSET:
		return dataTypeDataSet

	default:
		return dataTypeChar
	}
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
	cur := &oraCur{
		stmt: stmt, rows: rows, columns: columns,
	}
	// Query_ 完成后 Summary 仍有效；纯 DML（如 INSERT）往往在此即可得到行数（CurRowNumber 或 successIter）。
	cur.affectedRows = stmt.QueryRowsAffected()
	return cur, nil
}

type oraCur struct {
	stmt         *go_ora.Stmt
	rows         *go_ora.DataSet
	columns      []dbQueryColumn
	affectedRows int64
	done         bool
}

func (q *oraCur) Close() error {
	var err error
	if q.rows != nil {
		err = q.rows.Close()
	}
	if q.stmt != nil {
		q.affectedRows = q.stmt.QueryRowsAffected()
		if e := q.stmt.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (q *oraCur) Header() *dbQueryHeader {
	return &dbQueryHeader{
		columns:      q.columns,
		affectedRows: q.affectedRows,
	}
}

func (q *oraCur) NextRow() (*dbQueryRow, bool, error) {
	if q.done {
		return nil, false, nil
	}
	dest := make([]driver.Value, len(q.columns))
	if err := q.rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			q.done = true
			q.affectedRows = q.stmt.QueryRowsAffected()
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
