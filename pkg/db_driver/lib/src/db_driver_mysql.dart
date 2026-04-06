// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:go_impl/go_impl.dart' as impl;
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';
import 'package:sql_parser/parser.dart' as sp;
import 'package:db_driver/src/db_driver_conn_meta.dart';

class MysqlQueryValue extends BaseQueryValue {
  final impl.DbQueryValue _cell;

  MysqlQueryValue(this._cell);

  @override
  String? getString() => _cell.asString();

  @override
  List<int> getBytes() => _cell.asBytes();
}

class MysqlQueryColumn extends BaseQueryColumn {
  final impl.DbQueryColumn _column;

  MysqlQueryColumn(this._column);

  @override
  String get name => _column.name;

  @override
  DataType dataType() {
    return switch (_column.dataType) {
      impl.DbDataType.number => DataType.number,
      impl.DbDataType.char => DataType.char,
      impl.DbDataType.time => DataType.time,
      impl.DbDataType.blob => DataType.blob,
      impl.DbDataType.json => DataType.json,
      impl.DbDataType.dataSet => DataType.dataSet,
    };
  }
}

class MySQLConnection extends BaseConnection {
  final impl.ImplConnection _conn;
  final String _dsn;
  String? _sessionId;

  MySQLConnection(this._conn, this._dsn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.mysql, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final database = schema ?? meta.getValue("database", "");
    final host = meta.getHost();
    final port = meta.getPort() ?? 3306;
    final user = meta.user;
    final password = Uri.encodeComponent(meta.password);

    final dsn = '$user:$password@tcp($host:$port)/$database';

    final conn = await impl.ImplConnection.openMysql(dsn);
    final mc = MySQLConnection(conn, dsn);
    await mc._loadSessionId();
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

  Future<void> _loadSessionId() async {
    final results = await query("SELECT CONNECTION_ID() AS session_id");
    _sessionId = results.rows.first.getString("session_id");
  }

  @override
  Future<void> killQuery() async {
    if (_sessionId == null) return;

    MySQLConnection? tmp;
    try {
      final tmpConn = await impl.ImplConnection.openMysql(_dsn);
      tmp = MySQLConnection(tmpConn, _dsn);
      await tmp.query("KILL QUERY $_sessionId");
    } finally {
      await tmp?.close();
    }
  }

  @override
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql) async* {
    List<BaseQueryColumn>? columns;

    await for (final item in _conn.streamQuery(sql)) {
      switch (item) {
        case impl.DbQueryHeader():
          columns = item.columns
              .map<BaseQueryColumn>((c) => MysqlQueryColumn(c))
              .toList(growable: false);
          yield QueryStreamItemHeader(
            columns: columns,
            affectedRows: item.affectedRows,
          );
        case impl.DbQueryRow():
          final currentColumns = columns;
          if (currentColumns == null) {
            throw StateError('No header received before row');
          }
          yield QueryStreamItemRow(
            row: QueryResultRow(
              currentColumns,
              item.values
                  .map<BaseQueryValue>((c) => MysqlQueryValue(c))
                  .toList(growable: false),
            ),
          );
      }
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT VERSION() AS version");
    return results.rows.first.getString("version") ?? "";
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
    c.ORDINAL_POSITION;""");

    final rows = results.rows;
    final schemaRows =
        rows.groupListsBy((result) => result.getString("TABLE_SCHEMA")!);

    final schemaNodes = <MetaDataNode>[];
    for (final schema in schemaList) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
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
                    _getDataType(result.getString("DATA_TYPE")!)))
              .toList();
          tableNode.items = columnNodes;
        }
      }
      schemaNode.items = tableNodes;
    }
    return schemaNodes;
  }

  static DataType _getDataType(String dataType) {
    final t = dataType.toLowerCase().trim();
    return switch (t) {
      "int" ||
      "bigint" ||
      "smallint" ||
      "tinyint" ||
      "decimal" ||
      "double" ||
      "float" ||
      "mediumint" ||
      "bit" =>
        DataType.number,
      "char" || "varchar" || "binary" || "varbinary" => DataType.char,
      "datetime" || "time" || "timestamp" || "date" || "year" => DataType.time,
      "text" ||
      "blob" ||
      "longblob" ||
      "longtext" ||
      "mediumblob" ||
      "mediumtext" ||
      "tinyblob" ||
      "tinytext" =>
        DataType.blob,
      "json" => DataType.json,
      "set" || "enum" => DataType.dataSet,
      _ => DataType.blob,
    };
  }

  @override
  Future<List<String>> schemas() async {
    final results = await query("SHOW DATABASES");
    return results.rows
        .map((r) => r.getString("Database") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    final escaped = schema.replaceAll('`', '``');
    await query("USE `$escaped`");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema ?? "");
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT DATABASE() AS current_schema");
    return results.rows.first.getString("current_schema");
  }
}
