// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:rust_impl/rust_impl.dart' as rust_impl;
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';
import 'package:sql_parser/parser.dart' as sp;
import 'package:db_driver/src/db_driver_conn_meta.dart';

class MysqlQueryValue extends BaseQueryValue {
  final rust_impl.DbQueryValue _value;

  MysqlQueryValue(this._value);

  @override
  String? getString() {
    return _value.asString();
  }

  @override
  List<int> getBytes() {
    return _value.asBytes()?.toList() ?? <int>[];
  }
}

class MysqlQueryColumn extends BaseQueryColumn {
  final rust_impl.DbQueryColumn _column;

  MysqlQueryColumn(this._column);

  @override
  String get name => _column.name;

  @override
  DataType dataType() {
    return switch (_column.dataType) {
      rust_impl.DbDataType.number => DataType.number,
      rust_impl.DbDataType.char => DataType.char,
      rust_impl.DbDataType.time => DataType.time,
      rust_impl.DbDataType.blob => DataType.blob,
      rust_impl.DbDataType.json => DataType.json,
      rust_impl.DbDataType.dataSet => DataType.dataSet,
    };
  }
}

class MySQLConnection extends BaseConnection {
  final rust_impl.ImplConnection _conn;
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
    final conn = await rust_impl.ImplConnection.openMySql(dsn);
    final mc = MySQLConnection(conn, dsn);
    await mc.loadSessionId();
    return mc;
  }

  @override
  Future<void> close() async {
    await _conn.close();
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
    if (_sessionId == null) return;
    MySQLConnection? tmp;
    try {
      final tmpConn = await rust_impl.ImplConnection.openMySql(_dsn);
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
      await for (final item in _conn.streamQuery(sql)) {
        switch (item) {
          case rust_impl.DbQueryHeader():
            columns = item.columns
                .map<BaseQueryColumn>((c) => MysqlQueryColumn(c))
                .toList();
            yield QueryStreamItemHeader(
              columns: columns,
              affectedRows: item.affectedRows,
            );
          case rust_impl.DbQueryRow():
            if (columns == null) {
              throw StateError('Received row before header');
            }
            yield QueryStreamItemRow(
              row: QueryResultRow(
                columns,
                item.values.map((v) => MysqlQueryValue(v)).toList(),
              ),
            );
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
