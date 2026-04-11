import 'dart:math';

import 'package:db_driver/src/db_driver_metadata.dart';
import 'package:sql_parser/parser.dart';
import 'package:uuid/uuid.dart';
import 'package:go_impl/go_impl.dart' as impl;

enum DataType {
  number,
  char,
  time,
  blob,
  json,
  dataSet,
  ;
}

abstract class BaseQueryColumn {
  String get name;
  DataType dataType();
}

abstract class BaseQueryValue {
  String? getString();
  List<int> getBytes();

  String? getSummary() {
    String? value = getString();
    if (value == null) {
      return null;
    }
    return value
        .substring(0, min(value.length, 50))
        .replaceAll('\r\n', '\n')
        .replaceAll("\r", "\n")
        .replaceAll("\n", " ");
  }
}

class QueryResultRow {
  List<BaseQueryColumn> columns;
  Map<String, int> index;
  List<BaseQueryValue> values;

  QueryResultRow(this.columns, this.values)
      : index = {for (var i = 0; i < columns.length; i++) columns[i].name: i};

  BaseQueryValue? getValue(String column) {
    int? colIndex = index[column];
    if (colIndex == null || colIndex >= values.length) return null;
    return values[colIndex];
  }

  BaseQueryColumn? getColumn(String column) {
    int? colIndex = index[column];
    if (colIndex == null || colIndex >= values.length) return null;
    return columns[colIndex];
  }

  String? getString(String column) {
    final v = getValue(column);
    if (v == null) {
      return null;
    }
    return v.getString();
  }
}

class BaseQueryResult {
  String queryId;
  List<BaseQueryColumn> columns;
  List<QueryResultRow> rows;
  BigInt affectedRows;

  BaseQueryResult(this.queryId, this.columns, this.rows, this.affectedRows);
}

abstract class BaseQueryStreamItem {
  const BaseQueryStreamItem();
}

class QueryStreamItemHeader extends BaseQueryStreamItem {
  final List<BaseQueryColumn> columns;
  final BigInt affectedRows;

  const QueryStreamItemHeader({
    required this.columns,
    required this.affectedRows,
  });
}

class QueryStreamItemRow extends BaseQueryStreamItem {
  final QueryResultRow row;

  const QueryStreamItemRow({required this.row});
}

abstract class BaseConnection {
  void Function(String)? onSchemaChangedCallback;

  BaseConnection();

  Future<void> ping();
  Future<void> killQuery();
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql);
  Future<void> close();
  Future<List<MetaDataNode>> metadata();
  Future<List<String>> schemas();
  Future<String?> getCurrentSchema();
  Future<void> setCurrentSchema(String schema);
  Future<String> version();
  SQLDefiner parser(String sql);

  void listen(
      {Function()? onCloseCallback,
      Function(String)? onSchemaChangedCallback}) {
    this.onSchemaChangedCallback = onSchemaChangedCallback;
  }

  void onSchemaChanged(String schema) => onSchemaChangedCallback?.call(schema);

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
          // 增加防护：如果limit > 0，则只取limit条数据.
          if (limit != null && limit > 0 && rows.length >= limit) {
            continue;
          } else {
            rows.add(row);
          }
      }
    }

    if (resultColumns == null || resultAffectedRows == null) {
      throw StateError('No header received');
    }

    return BaseQueryResult(queryId, resultColumns, rows, resultAffectedRows);
  }

  Stream<BaseQueryStreamItem> queryStream(String sql, {int? limit}) async* {
    final sd = parser(sql);
    // 去掉末尾分隔符
    sql = sd.trimDelimiter(sql);
    // 包裹limit
    if (limit != null && limit > 0 && sd.canLimit) {
      sql = sd.wrapLimit(sql, limit);
    }
    // 添加注释
    final queryId = Uuid().v4();
    sql = '/* call by openhare, uuid: $queryId */ $sql';

    yield* queryStreamInternal(sql);
    if (sd.changeSchema) {
      final currentSchema = await getCurrentSchema();
      onSchemaChanged(currentSchema ?? '');
    }
  }
}

class GoImplQueryValue extends BaseQueryValue {
  final impl.DbQueryValue _cell;

  GoImplQueryValue(this._cell);

  @override
  String? getString() => _cell.asString();

  @override
  List<int> getBytes() => _cell.asBytes();
}

class GoImplQueryColumn extends BaseQueryColumn {
  final impl.DbQueryColumn _column;

  GoImplQueryColumn(this._column);

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

class GoImplConnection extends BaseConnection {
  final impl.ImplConnection _conn;

  GoImplConnection(this._conn);

  @override
  Future<void> ping() async {
    throw UnimplementedError();
  }

  @override
  Future<void> killQuery() async {
    throw UnimplementedError();
  }

  @override
  SQLDefiner parser(String sql) {
    throw UnimplementedError();
  }

  @override
  Future<void> setCurrentSchema(String schema) {
    throw UnimplementedError();
  }

  @override
  Future<String?> getCurrentSchema() {
    throw UnimplementedError();
  }

  @override
  Future<String> version() {
    throw UnimplementedError();
  }

  @override
  Future<List<MetaDataNode>> metadata() {
    throw UnimplementedError();
  }

  @override
  Future<List<String>> schemas() {
    throw UnimplementedError();
  }

  @override
  Future<void> close() async {
    await _conn.close();
  }

  @override
  Stream<BaseQueryStreamItem> queryStreamInternal(String sql) async* {
    List<BaseQueryColumn>? columns;

    await for (final item in _conn.streamQuery(sql)) {
      switch (item) {
        case impl.DbQueryHeader():
          columns = item.columns
              .map<BaseQueryColumn>((c) => GoImplQueryColumn(c))
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
                  .map<BaseQueryValue>((c) => GoImplQueryValue(c))
                  .toList(growable: false),
            ),
          );
      }
    }
  }
}
