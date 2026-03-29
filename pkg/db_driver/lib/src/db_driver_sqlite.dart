import 'dart:convert';

import 'package:collection/collection.dart';
import 'package:sql_parser/parser.dart' as sp;
import 'package:sqlite/sqlite.dart';

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class SqliteQueryValue extends BaseQueryValue {
  final QueryValue _value;

  SqliteQueryValue(this._value);

  @override
  String? getString() {
    return switch (_value) {
      QueryValue_NULL() => null,
      QueryValue_Bytes(:final field0) =>
        utf8.decode(field0, allowMalformed: true),
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

class SqliteQueryColumn extends BaseQueryColumn {
  final QueryColumn _column;

  SqliteQueryColumn(this._column);

  @override
  String get name => _column.name;

  @override
  DataType dataType() => SQLiteConnection._getDataType(_column.columnType);
}

class SQLiteConnection extends BaseConnection {
  final ConnWrapper _conn;

  SQLiteConnection(this._conn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.sqlite, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    var dsn = meta.getDbFile();
    if (dsn.startsWith("file://")) {
      dsn = Uri.parse(dsn).toFilePath();
    }
    final conn = await ConnWrapper.open(dsn: dsn.isEmpty ? ":memory:" : dsn);
    final sqliteConn = SQLiteConnection(conn);
    return sqliteConn;
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

  @override
  Future<void> killQuery() async {
    // sqlite 单进程连接，暂不支持跨连接 cancel
    return;
  }

  @override
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql) async* {
    List<BaseQueryColumn>? columns;

    await for (final item in _conn.query(query: sql)) {
      switch (item) {
        case QueryStreamItem_Header(:final field0):
          columns = field0.columns
              .map<BaseQueryColumn>((c) => SqliteQueryColumn(c))
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
              field0.values.map((v) => SqliteQueryValue(v)).toList(),
            ),
          );
        case QueryStreamItem_Error(:final field0):
          throw Exception(field0);
      }
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT sqlite_version() AS version;");
    return results.rows.first.getString("version") ?? "";
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final results = await query("""SELECT
    'main' AS TABLE_SCHEMA,
    m.name AS TABLE_NAME,
    p.name AS COLUMN_NAME,
    p.type AS DATA_TYPE
FROM
    sqlite_master AS m
JOIN
    pragma_table_info(m.name) AS p
WHERE
    m.type IN ('table', 'view')
    AND m.name NOT LIKE 'sqlite_%'
ORDER BY
    m.name,
    p.cid;""");

    final rows = results.rows;
    final schemaNodes = <MetaDataNode>[];
    final schemaRows =
        rows.groupListsBy((result) => result.getString("TABLE_SCHEMA")!);

    for (final schema in schemaRows.keys) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
      final tableRows = schemaRows[schema]!
          .groupListsBy((result) => result.getString("TABLE_NAME")!);
      for (final table in tableRows.keys) {
        final tableNode = MetaDataNode(MetaType.table, table);
        tableNodes.add(tableNode);

        final columnRows = tableRows[table]!;
        final columnNodes = columnRows
            .map((result) =>
                MetaDataNode(MetaType.column, result.getString("COLUMN_NAME")!)
                  ..withProp(
                    MetaDataPropType.dataType,
                    _getDataType(result.getString("DATA_TYPE") ?? ""),
                  ))
            .toList();
        tableNode.items = columnNodes;
      }

      schemaNode.items = tableNodes;
    }

    return schemaNodes;
  }

  static DataType _getDataType(String dataType) {
    final t = dataType.toUpperCase().trim();
    if (t.contains("INT") ||
        t.contains("REAL") ||
        t.contains("FLOA") ||
        t.contains("DOUB") ||
        t.contains("NUMERIC") ||
        t.contains("DECIMAL")) {
      return DataType.number;
    }
    if (t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT")) {
      return DataType.char;
    }
    if (t.contains("DATE") || t.contains("TIME")) {
      return DataType.time;
    }
    if (t.contains("BLOB")) {
      return DataType.blob;
    }
    if (t.contains("JSON")) {
      return DataType.json;
    }
    return DataType.char;
  }

  @override
  Future<List<String>> schemas() async {
    return ["main"];
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    onSchemaChanged("main");
  }

  @override
  Future<String?> getCurrentSchema() async {
    return "main";
  }
}
