// ignore_for_file: constant_identifier_names

import 'package:collection/collection.dart';
import 'package:go_impl/go_impl.dart' as impl;
import 'package:sql_parser/parser.dart' as sp;

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class MssqlQueryValue extends BaseQueryValue {
  final impl.DbQueryValue _cell;

  MssqlQueryValue(this._cell);

  @override
  String? getString() => _cell.asString();

  @override
  List<int> getBytes() => _cell.asBytes();
}

class MssqlQueryColumn extends BaseQueryColumn {
  final impl.DbQueryColumn _column;

  MssqlQueryColumn(this._column);

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

class MSSQLConnection extends BaseConnection {
  final impl.ImplConnection _conn;
  final String _dsn;
  String? _spid;

  MSSQLConnection(this._conn, this._dsn);

  @override
  sp.SQLDefiner parser(String sql) => sp.parser(sp.DialectType.mssql, sql);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final database = (schema != null && schema.isNotEmpty)
        ? schema
        : meta.getValue("database", "master");
    final encrypt = meta.getValue("encrypt", "true");
    final trustServerCertificate =
        meta.getValue("trustServerCertificate", "true");

    final dsn = Uri(
      scheme: 'sqlserver',
      userInfo: '${meta.user}:${Uri.encodeComponent(meta.password)}',
      host: meta.getHost(),
      port: meta.getPort() ?? 1433,
      queryParameters: {
        'database': database,
        'encrypt': encrypt,
        'trustServerCertificate': trustServerCertificate,
      },
    ).toString();

    final conn = await impl.ImplConnection.openMssql(dsn);
    final mc = MSSQLConnection(conn, dsn);
    await mc._loadSpid();
    return mc;
  }

  Future<void> _loadSpid() async {
    final results = await query("SELECT @@SPID AS spid;");
    _spid = results.rows.firstOrNull?.getString("spid");
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
    if (_spid == null || _spid!.isEmpty) return;

    MSSQLConnection? tmp;
    try {
      final tmpConn = await impl.ImplConnection.openMssql(_dsn);
      tmp = MSSQLConnection(tmpConn, _dsn);
      await tmp.query("KILL $_spid;");
    } finally {
      await tmp?.close();
    }
  }

  String _escapeIdent(String ident) {
    final escaped = ident.replaceAll(']', ']]');
    return '[$escaped]';
  }

  @override
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql) async* {
    List<BaseQueryColumn>? columns;

    await for (final item in _conn.streamQuery(sql)) {
      switch (item) {
        case impl.DbQueryHeader():
          columns = item.columns
              .map<BaseQueryColumn>((c) => MssqlQueryColumn(c))
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
                  .map<BaseQueryValue>((c) => MssqlQueryValue(c))
                  .toList(growable: false),
            ),
          );
      }
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT @@VERSION AS version;");
    return results.rows.first.getString("version") ?? "";
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final results = await query("""SELECT
    t.TABLE_CATALOG AS TABLE_SCHEMA,
    t.TABLE_NAME AS TABLE_NAME,
    c.COLUMN_NAME AS COLUMN_NAME,
    c.DATA_TYPE AS DATA_TYPE
FROM
    INFORMATION_SCHEMA.TABLES t
JOIN
    INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_CATALOG = c.TABLE_CATALOG
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
ORDER BY
    t.TABLE_CATALOG,
    t.TABLE_NAME,
    c.ORDINAL_POSITION;""");

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
    final t = dataType.toLowerCase();
    return switch (t) {
      "int" ||
      "bigint" ||
      "smallint" ||
      "tinyint" ||
      "decimal" ||
      "numeric" ||
      "money" ||
      "smallmoney" ||
      "float" ||
      "real" =>
        DataType.number,
      "char" ||
      "varchar" ||
      "nchar" ||
      "nvarchar" ||
      "text" ||
      "ntext" ||
      "uniqueidentifier" =>
        DataType.char,
      "date" ||
      "time" ||
      "datetime" ||
      "smalldatetime" ||
      "datetime2" ||
      "datetimeoffset" =>
        DataType.time,
      "binary" || "varbinary" || "image" => DataType.blob,
      "xml" => DataType.json,
      "bit" => DataType.dataSet,
      _ => DataType.char,
    };
  }

  /// Maps [go-mssqldb](https://github.com/microsoft/go-mssqldb) `ColumnType.DatabaseTypeName()` to UI data types.
  static DataType columnDataTypeFromDriverName(String typeName) {
    final t = typeName.toUpperCase().trim();
    if (t.isEmpty) return DataType.char;
    // Before matching INT (e.g. `DATETIME2` contains "INT").
    if (t.contains('DATE') || t.contains('TIME')) return DataType.time;
    if (t.contains('INT') && !t.contains('POINT')) return DataType.number;
    if (t.contains('DECIMAL') || t.contains('NUMERIC') || t.contains('MONEY')) {
      return DataType.number;
    }
    if (t.contains('FLOAT') || t.contains('REAL')) return DataType.number;
    if (t.contains('BINARY') ||
        t.contains('IMAGE') ||
        t.contains('VARBINARY')) {
      return DataType.blob;
    }
    if (t == 'BIT') return DataType.dataSet;
    if (t.contains('XML')) return DataType.json;
    if (t.contains('CHAR') ||
        t.contains('TEXT') ||
        t.contains('UNIQUEIDENTIFIER')) {
      return DataType.char;
    }
    return DataType.char;
  }

  @override
  Future<List<String>> schemas() async {
    final results = await query(
        "SELECT name AS SCHEMA_NAME FROM sys.databases ORDER BY name;");
    return results.rows
        .map((r) => r.getString("SCHEMA_NAME") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    await query("USE ${_escapeIdent(schema)};");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema ?? "");
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT DB_NAME() AS CURRENT_SCHEMA;");
    return results.rows.first.getString("CURRENT_SCHEMA");
  }
}
