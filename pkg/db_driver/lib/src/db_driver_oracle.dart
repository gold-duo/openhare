// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:oracle/oracle.dart' as oracle_impl;
import 'package:sql_parser/parser.dart';
import 'package:uuid/uuid.dart';

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class OracleQueryValue extends BaseQueryValue {
  final oracle_impl.OracleCell _cell;

  OracleQueryValue(this._cell);

  @override
  String? getString() => _cell.asString();

  @override
  List<int> getBytes() => _cell.asBytes();
}

class OracleQueryColumn extends BaseQueryColumn {
  final oracle_impl.OracleColumn _column;

  OracleQueryColumn(this._column);

  @override
  String get name => _column.name;

  @override
  DataType dataType() => OracleConnection._getDataType(_column.typeName);
}

class OracleConnection extends BaseConnection {
  final oracle_impl.OracleConnection _conn;

  OracleConnection(this._conn);

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

    final conn = await oracle_impl.OracleConnection.open(dsn);
    final oc = OracleConnection(conn);

    if (schema != null && schema.isNotEmpty) {
      await oc.setCurrentSchema(schema);
    }

    return oc;
  }

  @override
  Future<void> close() async {
    await _conn.close();
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1 FROM dual");
  }

  @override
  Future<void> killQuery() async {
    // todo: implement kill query
    return;
  }

  @override
  Future<BaseQueryResult> query(String sql, {int? limit}) async {
    final queryId = Uuid().v4();
    List<BaseQueryColumn>? resultColumns;
    BigInt? resultAffectedRows;
    final rows = <QueryResultRow>[];

    await for (final item in queryStream(sql, limit: limit)) {
      switch (item) {
        case QueryStreamItemHeader(:final columns, :final affectedRows):
          resultColumns = columns;
          resultAffectedRows = affectedRows;
        case QueryStreamItemRow(:final row):
          rows.add(row);
      }
    }

    if (resultColumns == null || resultAffectedRows == null) {
      throw StateError('No header received');
    }

    return BaseQueryResult(queryId, resultColumns, rows, resultAffectedRows);
  }

  int _resolveBatchSize(int? limit) {
    if (limit == null || limit <= 0) {
      return 256;
    }
    if (limit < 32) {
      return 32;
    }
    if (limit > 512) {
      return 512;
    }
    return limit;
  }

  @override
  Stream<BaseQueryStreamItem> queryStream(String sql, {int? limit}) async* {
    final sd = parser(DialectType.oracle, sql);
    if (limit != null && sd.canLimit) {
      sql = sd.wrapLimit(limit: limit);
    }

    final queryId = Uuid().v4();
    sql = "/* call by openhare, uuid: $queryId */ $sql";

    final batchSize = _resolveBatchSize(limit);
    List<BaseQueryColumn>? columns;
    await for (final item in _conn.queryStream(sql, batchSize: batchSize)) {
      switch (item) {
        case oracle_impl.OracleQueryStreamHeader():
          columns = item.columns
              .map<BaseQueryColumn>((c) => OracleQueryColumn(c))
              .toList(growable: false);
          yield QueryStreamItemHeader(
            columns: columns,
            affectedRows: item.affectedRows,
          );
        case oracle_impl.OracleQueryStreamRow():
          final currentColumns = columns;
          if (currentColumns == null) {
            throw StateError('No header received before row');
          }
          yield QueryStreamItemRow(
            row: QueryResultRow(
              currentColumns,
              item.cells
                  .map<BaseQueryValue>((c) => OracleQueryValue(c))
                  .toList(growable: false),
            ),
          );
      }
    }
    if (sd.changeSchema) {
      final currentSchema = await getCurrentSchema();
      onSchemaChanged(currentSchema ?? "");
    }
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

  @override
  Future<List<MetaDataNode>> metadata() async {
    // ref: all_tab_columns
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
                  ..withProp(MetaDataPropType.dataType,
                      _getDataType(result.getString("DATA_TYPE")!)))
            .toList();
        tableNode.items = columnNodes;
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
}
