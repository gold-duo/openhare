// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:go_impl/go_impl.dart' as impl;
import 'package:sql_parser/parser.dart' as sp;

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class PGQueryValue extends BaseQueryValue {
  final impl.DbQueryValue _cell;

  PGQueryValue(this._cell);

  @override
  String? getString() => _cell.asString();

  @override
  List<int> getBytes() => _cell.asBytes();
}

class PGQueryColumn extends BaseQueryColumn {
  final impl.DbQueryColumn _column;

  PGQueryColumn(this._column);

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

class PGConnection extends BaseConnection {
  final impl.ImplConnection _conn;
  final String _dsn;
  int? _backendPid;

  PGConnection(this._conn, this._dsn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.pg, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final database = meta.getValue("database", "postgres");
    final sslmode = meta.getValue("sslmode", "disable");
    final connectTimeout = meta.getIntValue("connectTimeout", 10);
    final queryTimeout = meta.getIntValue("queryTimeout", 600);

    final dsn = Uri(
      scheme: 'postgres',
      userInfo: '${meta.user}:${Uri.encodeComponent(meta.password)}',
      host: meta.getHost(),
      port: meta.getPort() ?? 5432,
      path: '/$database',
      queryParameters: {
        'sslmode': sslmode,
        'connect_timeout': connectTimeout.toString(),
      },
    ).toString();

    final conn = await impl.ImplConnection.openPg(dsn);
    final pg = PGConnection(conn, dsn);

    await pg.query("SET statement_timeout = '${queryTimeout}s'");
    await pg._loadBackendPid();

    if (schema != null && schema.isNotEmpty) {
      await pg.setCurrentSchema(schema);
    }

    return pg;
  }

  Future<void> _loadBackendPid() async {
    final results = await query("SELECT pg_backend_pid() AS pid");
    _backendPid = int.tryParse(results.rows.first.getString("pid") ?? "");
  }

  @override
  Future<void> close() async {
    await _conn.close();
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1");
  }

  @override
  Future<void> killQuery() async {
    if (_backendPid == null) return;

    PGConnection? tmp;
    try {
      final tmpConn = await impl.ImplConnection.openPg(_dsn);
      tmp = PGConnection(tmpConn, _dsn);
      await tmp.query("SELECT pg_cancel_backend($_backendPid)");
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
              .map<BaseQueryColumn>((c) => PGQueryColumn(c))
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
                  .map<BaseQueryValue>((c) => PGQueryValue(c))
                  .toList(growable: false),
            ),
          );
      }
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT version() AS version");
    return results.rows.first.getString("version") ?? "";
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final results = await query("""SELECT 
    t.table_schema,
    t.table_name,
    c.column_name,
    c.data_type
FROM 
    information_schema.tables t
JOIN 
    information_schema.columns c 
    ON t.table_name = c.table_name 
    AND t.table_schema = c.table_schema
WHERE 
    t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY
    t.table_schema,
    t.table_name, 
    c.ordinal_position;""");

    final rows = results.rows;
    final schemaNodes = <MetaDataNode>[];

    final schemaRows =
        rows.groupListsBy((result) => result.getString("table_schema")!);

    for (final schema in schemaRows.keys) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
      final tableRows = schemaRows[schema]!
          .groupListsBy((result) => result.getString("table_name")!);

      for (final table in tableRows.keys) {
        final tableNode = MetaDataNode(MetaType.table, table);
        tableNodes.add(tableNode);

        final columnRows = tableRows[table]!;
        final columnNodes = columnRows
            .map((result) =>
                MetaDataNode(MetaType.column, result.getString("column_name")!)
                  ..withProp(MetaDataPropType.dataType,
                      _getDataType(result.getString("data_type")!)))
            .toList();
        tableNode.items = columnNodes;
      }

      schemaNode.items = tableNodes;
    }

    return schemaNodes;
  }

  static DataType _getDataType(String dataType) {
    final t = dataType.toLowerCase().trim();
    return switch (t) {
      "integer" ||
      "bigint" ||
      "smallint" ||
      "serial" ||
      "bigserial" ||
      "smallserial" ||
      "numeric" ||
      "decimal" ||
      "real" ||
      "double precision" =>
        DataType.number,
      "character varying" ||
      "varchar" ||
      "character" ||
      "char" ||
      "text" =>
        DataType.char,
      "timestamp without time zone" ||
      "timestamp with time zone" ||
      "timestamptz" ||
      "timestamp" ||
      "date" ||
      "time without time zone" ||
      "time with time zone" ||
      "time" ||
      "interval" =>
        DataType.time,
      "bytea" => DataType.blob,
      "json" || "jsonb" => DataType.json,
      "boolean" => DataType.dataSet,
      "uuid" => DataType.char,
      "array" || "user-defined" => DataType.blob,
      _ => DataType.char,
    };
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    final escaped = schema.replaceAll("'", "''");
    await query("SET search_path TO '$escaped'");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema ?? "");
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT current_schema();");
    return results.rows.first.getString("current_schema");
  }

  @override
  Future<List<String>> schemas() async {
    final results = await query("SELECT nspname FROM pg_namespace;");
    return results.rows
        .map((r) => r.getString("nspname") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }
}
