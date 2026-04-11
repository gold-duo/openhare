// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:go_impl/go_impl.dart' as impl;
import 'package:sql_parser/parser.dart' as sp;

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class OracleConnection extends GoImplConnection {
  OracleConnection(super._conn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.oracle, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final service = meta.getValue("service", "FREEPDB1");
    final dsn = Uri(
      scheme: "oracle",
      userInfo: '${meta.user}:${Uri.encodeComponent(meta.password)}',
      host: meta.getHost(),
      port: meta.getPort() ?? 1521,
      path: service,
    ).toString();

    final conn = await impl.ImplConnection.openOracle(dsn);
    final oc = OracleConnection(conn);

    if (schema != null && schema.isNotEmpty) {
      await oc.setCurrentSchema(schema);
    }

    return oc;
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1 FROM dual");
  }

  @override
  Future<void> killQuery() async {
    return;
  }

  @override
  Future<String> version() async {
    try {
      final results = await query(
          "SELECT banner AS version FROM v\\$version WHERE rownum = 1");
      return results.rows.first.getString("VERSION") ?? "";
    } catch (_) {
      final results = await query(
          "SELECT version AS version FROM product_component_version WHERE rownum = 1");
      return results.rows.first.getString("VERSION") ?? "";
    }
  }

  String _escapeIdent(String ident) {
    final escaped = ident.replaceAll('"', '""');
    return '"$escaped"';
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    await query("ALTER SESSION SET CURRENT_SCHEMA = ${_escapeIdent(schema)}");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema ?? "");
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query(
        "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') AS CURRENT_SCHEMA FROM dual");
    return results.rows.first.getString("CURRENT_SCHEMA");
  }

  @override
  Future<List<String>> schemas() async {
    final results = await query(
        "SELECT username AS SCHEMA_NAME FROM all_users ORDER BY username");
    return results.rows
        .map((r) => r.getString("SCHEMA_NAME") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final schemaList = await schemas();

    final results = await query("""SELECT
    owner AS TABLE_SCHEMA,
    table_name AS TABLE_NAME,
    column_name AS COLUMN_NAME,
    data_type AS DATA_TYPE
FROM
    all_tab_columns
ORDER BY
    owner,
    table_name,
    column_id""");

    final rows = results.rows;
    final schemaRows =
        rows.groupListsBy((result) => result.getString("TABLE_SCHEMA")!);

    final schemaNodes = <MetaDataNode>[];
    for (final schema in schemaList) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
      final tableRowsForSchema = schemaRows[schema];
      if (tableRowsForSchema != null) {
        final byTable = tableRowsForSchema
            .groupListsBy((result) => result.getString("TABLE_NAME")!);
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
    final t = dataType.toUpperCase().trim();
    return switch (t) {
      "NUMBER" ||
      "BINARY_FLOAT" ||
      "BINARY_DOUBLE" ||
      "FLOAT" ||
      "INTEGER" ||
      "DECIMAL" =>
        DataType.number,
      "VARCHAR2" ||
      "NVARCHAR2" ||
      "CHAR" ||
      "NCHAR" ||
      "CLOB" ||
      "NCLOB" =>
        DataType.char,
      "DATE" ||
      "TIMESTAMP" ||
      "TIMESTAMP WITH TIME ZONE" ||
      "TIMESTAMP WITH LOCAL TIME ZONE" =>
        DataType.time,
      "BLOB" || "RAW" || "LONG RAW" || "BFILE" => DataType.blob,
      "JSON" => DataType.json,
      _ => DataType.char,
    };
  }
}
