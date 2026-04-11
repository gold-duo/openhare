// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:go_impl/go_impl.dart' as impl;
import 'package:sql_parser/parser.dart' as sp;

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class PGConnection extends GoImplConnection {
  final String _dsn;
  int? _backendPid;

  PGConnection(super._conn, this._dsn);

  static const Set<String> _pgSystemSchemasLower = {
    'information_schema',
    'pg_catalog',
    'pg_logical',
    'pg_toast',
  };

  static String _pgSystemSchemasNotInSql() =>
      _pgSystemSchemasLower.map((s) => "'$s'").join(', ');

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
  Future<String> version() async {
    final results = await query("SELECT version() AS version");
    return results.rows.first.getString("version") ?? "";
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
    final results = await query(
      'SELECT nspname FROM pg_namespace '
      'WHERE LOWER(nspname) NOT IN (${_pgSystemSchemasNotInSql()}) '
      'ORDER BY nspname;',
    );
    return results.rows
        .map((r) => r.getString("nspname") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final schemaList = await schemas();

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
    final schemaRows =
        rows.groupListsBy((result) => result.getString("table_schema")!);

    final schemaNodes = <MetaDataNode>[];
    for (final schema in schemaList) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
      final tableRowsForSchema = schemaRows[schema];
      if (tableRowsForSchema != null) {
        final byTable = tableRowsForSchema
            .groupListsBy((result) => result.getString("table_name")!);
        for (final table in byTable.keys) {
          final tableNode = MetaDataNode(MetaType.table, table);
          tableNodes.add(tableNode);

          final columnRows = byTable[table]!;
          final columnNodes = columnRows
              .map((result) => MetaDataNode(
                  MetaType.column, result.getString("column_name")!)
                ..withProp(MetaDataPropType.dataType,
                    _getDataType(result.getString("data_type")!)))
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
}
