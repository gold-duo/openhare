// ignore_for_file: constant_identifier_names

import 'dart:convert';

import 'package:collection/collection.dart';
import 'package:mysql/mysql.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';
import 'package:sql_parser/parser.dart' as sp;
import 'package:db_driver/src/db_driver_conn_meta.dart';

class MysqlQueryValue extends BaseQueryValue {
  final QueryValue _value;

  MysqlQueryValue(this._value);

  @override
  String? getString() {
    return switch (_value) {
      QueryValue_NULL() => null,
      QueryValue_Bytes(:final field0) =>
        utf8.decode(field0, allowMalformed: true), // todo: support gbk, latin1?
      QueryValue_Int(:final field0) => field0.toString(),
      QueryValue_UInt(:final field0) => field0.toString(),
      QueryValue_Float(:final field0) => field0.toString(),
      QueryValue_Double(:final field0) => field0.toString(),
      QueryValue_DateTime(:final field0) => field0.toString(),
    };
  }

  @override
  List<int> getBytes() {
    return switch (_value) {
      QueryValue_Bytes(:final field0) => field0.toList(),
      _ => <int>[],
    };
  }
}

class MysqlQueryColumn extends BaseQueryColumn {
  final QueryColumn _column;

  MysqlQueryColumn(this._column);

  @override
  String get name => _column.name;

  static const MYSQL_TYPE_DECIMAL = 0;
  static const MYSQL_TYPE_TINY = 1;
  static const MYSQL_TYPE_SHORT = 2;
  static const MYSQL_TYPE_LONG = 3;
  static const MYSQL_TYPE_FLOAT = 4;
  static const MYSQL_TYPE_DOUBLE = 5;
  static const MYSQL_TYPE_NULL = 6;
  static const MYSQL_TYPE_TIMESTAMP = 7;
  static const MYSQL_TYPE_LONGLONG = 8;
  static const MYSQL_TYPE_INT24 = 9;
  static const MYSQL_TYPE_DATE = 10;
  static const MYSQL_TYPE_TIME = 11;
  static const MYSQL_TYPE_DATETIME = 12;
  static const MYSQL_TYPE_YEAR = 13;
  static const MYSQL_TYPE_NEWDATE = 14;
  static const MYSQL_TYPE_VARCHAR = 15;
  static const MYSQL_TYPE_BIT = 16;
  static const MYSQL_TYPE_TIMESTAMP2 = 17;
  static const MYSQL_TYPE_DATETIME2 = 18;
  static const MYSQL_TYPE_TIME2 = 19;
  static const MYSQL_TYPE_TYPED_ARRAY = 20;
  static const MYSQL_TYPE_VECTOR = 242;
  static const MYSQL_TYPE_UNKNOWN = 243;
  static const MYSQL_TYPE_JSON = 245;
  static const MYSQL_TYPE_NEWDECIMAL = 246;
  static const MYSQL_TYPE_ENUM = 247;
  static const MYSQL_TYPE_SET = 248;
  static const MYSQL_TYPE_TINY_BLOB = 249;
  static const MYSQL_TYPE_MEDIUM_BLOB = 250;
  static const MYSQL_TYPE_LONG_BLOB = 251;
  static const MYSQL_TYPE_BLOB = 252;
  static const MYSQL_TYPE_VAR_STRING = 253;
  static const MYSQL_TYPE_STRING = 254;
  static const MYSQL_TYPE_GEOMETRY = 255;

  @override
  DataType dataType() {
    return switch (_column.columnType) {
      MYSQL_TYPE_JSON => DataType.json, // JSON

      MYSQL_TYPE_BIT ||
      MYSQL_TYPE_TINY_BLOB ||
      MYSQL_TYPE_MEDIUM_BLOB ||
      MYSQL_TYPE_LONG_BLOB ||
      MYSQL_TYPE_BLOB =>
        DataType.blob, // BLOB, TINY_BLOB, MEDIUM_BLOB, LONG_BLOB

      MYSQL_TYPE_VARCHAR ||
      MYSQL_TYPE_VAR_STRING ||
      MYSQL_TYPE_STRING =>
        DataType.char, // STRING, VARCHAR, VAR_STRING

      MYSQL_TYPE_TIMESTAMP ||
      MYSQL_TYPE_DATE ||
      MYSQL_TYPE_TIME ||
      MYSQL_TYPE_DATETIME ||
      MYSQL_TYPE_YEAR ||
      MYSQL_TYPE_TIMESTAMP2 ||
      MYSQL_TYPE_DATETIME2 ||
      MYSQL_TYPE_TIME2 =>
        DataType.time, // DATE, DATETIME, TIMESTAMP

      MYSQL_TYPE_DECIMAL ||
      MYSQL_TYPE_TINY ||
      MYSQL_TYPE_SHORT ||
      MYSQL_TYPE_LONG ||
      MYSQL_TYPE_FLOAT ||
      MYSQL_TYPE_DOUBLE ||
      MYSQL_TYPE_LONGLONG ||
      MYSQL_TYPE_INT24 ||
      MYSQL_TYPE_NEWDECIMAL =>
        DataType.number, // number

      _ => DataType.char,
    };
  }
}

class MySQLConnection extends BaseConnection {
  final ConnWrapper _conn;
  late String? _sessionId;
  final String _dsn;

  MySQLConnection(this._conn, this._dsn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.mysql, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final dsn = Uri(
      scheme: "mysql",
      userInfo: '${meta.user}:${Uri.encodeComponent(meta.password)}',
      host: meta.getHost(),
      port: meta.getPort() ?? 3306,
      path: schema ?? "",
    ).toString();
    final conn = await ConnWrapper.open(dsn: dsn);
    final mc = MySQLConnection(conn, dsn);
    await mc.loadSessionId();
    return mc;
  }

  @override
  Future<void> close() async {
    await _conn.close();
    _conn.dispose();
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1");
  }

  Future<void> loadSessionId() async {
    final results = await query("SELECT CONNECTION_ID() AS session_id;");
    final row = results.rows.first;
    _sessionId = row.getString("session_id");
  }

  @override
  Future<void> killQuery() async {
    // create new connection to kill query
    MySQLConnection? tmp;
    try {
      final tmpConn = await ConnWrapper.open(dsn: _dsn);
      tmp = MySQLConnection(tmpConn, _dsn);
      await tmp.query("KILL QUERY $_sessionId");
    } finally {
      await tmp?.close();
    }
  }

  @override
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql) async* {
    List<BaseQueryColumn>? columns;

    try {
      await for (final item in _conn.query(query: sql)) {
        switch (item) {
          case QueryStreamItem_Header(:final field0):
            columns = field0.columns
                .map<BaseQueryColumn>((c) => MysqlQueryColumn(c))
                .toList();
            yield QueryStreamItemHeader(
              columns: columns,
              affectedRows: field0.affectedRows,
            );
          case QueryStreamItem_Row(:final field0):
            if (columns == null) {
              throw StateError('Received row before header');
            }
            yield QueryStreamItemRow(
              row: QueryResultRow(
                columns,
                field0.values.map((v) => MysqlQueryValue(v)).toList(),
              ),
            );
          case QueryStreamItem_Error(:final field0):
            throw Exception(field0);
        }
      }
    } catch (e) {
      rethrow;
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT VERSION() AS version");
    final rows = results.rows;
    return rows.first.getString("version") ?? "";
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final schemaList = await schemas();

    // ref: https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html
    final results = await query("""SELECT 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE
FROM 
    information_schema.TABLES t
JOIN 
    information_schema.COLUMNS c 
    ON t.TABLE_NAME = c.TABLE_NAME 
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
WHERE 
    t.TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VIEW')
ORDER BY
    t.TABLE_SCHEMA,
    t.TABLE_NAME, 
    c.ORDINAL_POSITION;
""");
    final rows = results.rows;
    final schemaRows =
        rows.groupListsBy((result) => result.getString("TABLE_SCHEMA")!);

    List<MetaDataNode> schemaNodes = List.empty(growable: true);
    for (final schema in schemaList) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      List<MetaDataNode> tableNodes = List.empty(growable: true);
      final tableRows = schemaRows[schema];
      if (tableRows != null) {
        final byTable =
            tableRows.groupListsBy((result) => result.getString("TABLE_NAME")!);
        for (final table in byTable.keys) {
          final tableNode = MetaDataNode(MetaType.table, table);
          tableNodes.add(tableNode);

          final columnRows = byTable[table]!;
          final columnNodes = columnRows
              .map((result) => MetaDataNode(
                  MetaType.column, result.getString("COLUMN_NAME")!)
                ..withProp(MetaDataPropType.dataType,
                    getDataType(result.getString("DATA_TYPE")!)))
              .toList();
          tableNode.items = columnNodes;
        }
      }
      schemaNode.items = tableNodes;
    }
    return schemaNodes;
  }

  @override
  Future<List<String>> schemas() async {
    List<String> schemas = List.empty(growable: true);
    final results = await query("show databases");
    final rows = results.rows;
    for (final result in rows) {
      schemas.add(result.getString("Database") ?? "");
    }
    return schemas;
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    await query("USE `$schema`");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema!);
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT DATABASE()");
    final rows = results.rows;
    final currentSchema = rows.first.getString("DATABASE()");
    return currentSchema;
  }

  static DataType getDataType(String dataType) {
    return switch (dataType) {
      "int" ||
      "bigint" ||
      "smallint" ||
      "tinyint" ||
      "decimal" ||
      "double" ||
      "float" =>
        DataType.number,
      "char" || "varchar" => DataType.char,
      "datetime" || "time" || "timestamp" => DataType.time,
      "text" ||
      "blob" ||
      "longblob" ||
      "longtext" ||
      "mediumblob" ||
      "mediumtext" =>
        DataType.blob,
      "json" => DataType.json,
      "set" || "enum" => DataType.dataSet,
      _ => DataType.blob,
    };
  }
}
