library;

import 'dart:async';
import 'dart:typed_data';

import 'src/rust/api/db.dart' as rust;
import 'src/rust/api/mysql.dart' as mysql_api;
import 'src/rust/api/sqlite.dart' as sqlite_api;

export 'src/rust/api/db.dart'
    show
        QueryColumn,
        QueryHeader,
        QueryRow,
        QueryStreamItem,
        QueryValue,
        DataType;
export 'src/rust/frb_generated.dart' show RustLib;

final class ImplConnection {
  ImplConnection._(this._handle, this._type);
  final dynamic _handle;
  final _ConnectionType _type;

  static Future<ImplConnection> openMySql(String dsn) async {
    final conn = await mysql_api.MySqlConnection.open(dsn: dsn);
    return ImplConnection._(conn, _ConnectionType.mysql);
  }

  static Future<ImplConnection> openSqlite(String dsn) async {
    final conn = await sqlite_api.SqliteConnection.open(dsn: dsn);
    return ImplConnection._(conn, _ConnectionType.sqlite);
  }

  Stream<DbQueryEvent> streamQuery(String sql) async* {
    Stream<rust.QueryStreamItem> stream;
    switch (_type) {
      case _ConnectionType.mysql:
        stream = (_handle as mysql_api.MySqlConnection).query(query: sql);
        break;
      case _ConnectionType.sqlite:
        stream = (_handle as sqlite_api.SqliteConnection).query(query: sql);
        break;
    }

    await for (final item in stream) {
      switch (item) {
        case rust.QueryStreamItem_Header(:final field0):
          yield DbQueryHeader(
            affectedRows: field0.affectedRows,
            columns: field0.columns
                .map(
                  (c) => DbQueryColumn(
                    name: c.name,
                    dataType: _convertDataType(c.dataType),
                  ),
                )
                .toList(),
          );
        case rust.QueryStreamItem_Row(:final field0):
          yield DbQueryRow(
            values: field0.values.map(DbQueryValue.fromRust).toList(),
          );
        case rust.QueryStreamItem_Error(:final field0):
          throw Exception(field0);
      }
    }
  }

  Future<void> close() async {
    switch (_type) {
      case _ConnectionType.mysql:
        await (_handle as mysql_api.MySqlConnection).close();
        break;
      case _ConnectionType.sqlite:
        await (_handle as sqlite_api.SqliteConnection).close();
        break;
    }
  }

  static DbDataType _convertDataType(rust.DataType type) {
    return switch (type) {
      rust.DataType.number => DbDataType.number,
      rust.DataType.char => DbDataType.char,
      rust.DataType.time => DbDataType.time,
      rust.DataType.blob => DbDataType.blob,
      rust.DataType.json => DbDataType.json,
      rust.DataType.dataSet => DbDataType.dataSet,
    };
  }
}

enum _ConnectionType { mysql, sqlite }

enum DbDataType { number, char, time, blob, json, dataSet }

sealed class DbQueryEvent {
  const DbQueryEvent();
}

final class DbQueryHeader extends DbQueryEvent {
  const DbQueryHeader({required this.affectedRows, required this.columns});

  final BigInt affectedRows;
  final List<DbQueryColumn> columns;
}

final class DbQueryRow extends DbQueryEvent {
  const DbQueryRow({required this.values});

  final List<DbQueryValue> values;
}

final class DbQueryColumn {
  const DbQueryColumn({required this.name, required this.dataType});

  final String name;
  final DbDataType dataType;
}

final class DbQueryValue {
  const DbQueryValue(this._queryValueType, this._value);

  factory DbQueryValue.fromRust(rust.QueryValue value) {
    return switch (value) {
      rust.QueryValue_NULL() => DbQueryValue(DbQueryValueType.null_, null),
      rust.QueryValue_Bytes(:final field0) => DbQueryValue(
        DbQueryValueType.bytes,
        field0,
      ),
      rust.QueryValue_Int(:final field0) => DbQueryValue(
        DbQueryValueType.int,
        field0,
      ),
      rust.QueryValue_UInt(:final field0) => DbQueryValue(
        DbQueryValueType.uint,
        field0,
      ),
      rust.QueryValue_Float(:final field0) => DbQueryValue(
        DbQueryValueType.float,
        field0,
      ),
      rust.QueryValue_Double(:final field0) => DbQueryValue(
        DbQueryValueType.double,
        field0,
      ),
      rust.QueryValue_DateTime(:final field0) => DbQueryValue(
        DbQueryValueType.dateTime,
        field0,
      ),
    };
  }

  final DbQueryValueType _queryValueType;
  final Object? _value;

  int? asInt() =>
      _queryValueType == DbQueryValueType.int ? _value as int? : null;

  int? asUInt() => _queryValueType == DbQueryValueType.uint
      ? (_value as BigInt?)?.toInt()
      : null;

  double? asDouble() =>
      (_queryValueType == DbQueryValueType.double ||
          _queryValueType == DbQueryValueType.float)
      ? (_value as num?)?.toDouble()
      : null;

  String? asString() {
    return switch (_queryValueType) {
      DbQueryValueType.null_ => null,
      DbQueryValueType.bytes => String.fromCharCodes(_value as Uint8List),
      DbQueryValueType.int => (_value as int?)?.toString(),
      DbQueryValueType.uint => (_value as BigInt?)?.toString(),
      DbQueryValueType.float => (_value as num?)?.toString(),
      DbQueryValueType.double => (_value as num?)?.toString(),
      DbQueryValueType.dateTime => (_value as int?)?.toString(),
    };
  }

  Uint8List? asBytes() =>
      _queryValueType == DbQueryValueType.bytes ? _value as Uint8List? : null;

  DateTime? asDateTimeUtc() => _queryValueType == DbQueryValueType.dateTime
      ? DateTime.fromMillisecondsSinceEpoch((_value as int?) ?? 0, isUtc: true)
      : null;
}

enum DbQueryValueType { null_, bytes, int, uint, float, double, dateTime }
