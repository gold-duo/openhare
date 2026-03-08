import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'oracle_bindings_generated.dart';

const String _libName = 'oracle';

final DynamicLibrary _dylib = () {
  if (Platform.isMacOS) {
    return DynamicLibrary.open('$_libName.framework/$_libName');
  }
  if (Platform.isIOS) {
    return DynamicLibrary.open('$_libName.framework/$_libName');
  }
  if (Platform.isAndroid || Platform.isLinux) {
    return DynamicLibrary.open('lib$_libName.so');
  }
  if (Platform.isWindows) {
    return DynamicLibrary.open('$_libName.dll');
  }
  throw UnsupportedError('Unknown platform: ${Platform.operatingSystem}');
}();

final OracleBindings _bindings = OracleBindings(_dylib);

class OracleConnection {
  final int handle;
  OracleConnection._(this.handle);

  static Future<OracleConnection> open(String dsn) async {
    final h = await _OracleNative.instance.open(dsn);
    return OracleConnection._(h);
  }

  Future<OracleQueryResult> query(String sql) async {
    return _OracleNative.instance.query(handle, sql);
  }

  Stream<OracleQueryStreamItem> queryStream(
    String sql, {
    int batchSize = 256,
  }) {
    return _OracleNative.instance
        .queryStream(handle, sql, batchSize: batchSize);
  }

  Future<void> close() => _OracleNative.instance.close(handle);
}

class OracleQueryResult {
  final List<OracleColumn> columns;
  final List<List<OracleCell>> rows;
  final BigInt affectedRows;

  const OracleQueryResult({
    required this.columns,
    required this.rows,
    required this.affectedRows,
  });

  factory OracleQueryResult.fromNativeMap(Map<String, Object?> data) {
    final rawColumns = (data['columns'] as List<dynamic>? ?? const [])
        .cast<Map<Object?, Object?>>();
    final columns = rawColumns
        .map((c) => OracleColumn(
              name: (c['name'] as String?) ?? '',
              typeName: (c['column_type'] as String?) ?? '',
            ))
        .toList();

    final affectedRows = _parseAffectedRows(data['affected_rows']);

    final rawRows =
        (data['rows'] as List<dynamic>? ?? const []).cast<Map<Object?, Object?>>();
    final rows = rawRows.map((rowAny) {
      final rowCells =
          (rowAny['values'] as List<dynamic>? ?? const []).cast<Map<Object?, Object?>>();
      return rowCells.map(OracleCell.fromWireMap).toList(growable: false);
    }).toList();

    return OracleQueryResult(
      columns: columns,
      rows: rows,
      affectedRows: affectedRows,
    );
  }
}

sealed class OracleQueryStreamItem {
  const OracleQueryStreamItem();
}

class OracleQueryStreamHeader extends OracleQueryStreamItem {
  final List<OracleColumn> columns;
  final BigInt affectedRows;
  const OracleQueryStreamHeader({
    required this.columns,
    required this.affectedRows,
  });
}

class OracleQueryStreamRow extends OracleQueryStreamItem {
  final List<OracleCell> cells;
  const OracleQueryStreamRow(this.cells);
}

class OracleColumn {
  final String name;
  final String typeName;
  const OracleColumn({required this.name, required this.typeName});
}

enum OracleCellKind {
  null_,
  bytes,
  string,
  datetime,
  int_,
  uint,
  float_,
  double_,
  other,
}

const int _wireKindNull = 0;
const int _wireKindBytes = 1;
const int _wireKindInt = 2;
const int _wireKindUint = 3;
const int _wireKindFloat = 4;
const int _wireKindDouble = 5;
const int _wireKindDateTime = 6;
const int _wireKindString = 7;

class OracleCell {
  final OracleCellKind kind;
  final Object? value;

  const OracleCell(this.kind, this.value);

  factory OracleCell.fromWireMap(Map<Object?, Object?> cell) {
    final kindCode = (cell['kind'] as num?)?.toInt() ?? 0;
    final v = cell['value'];
    return switch (kindCode) {
      _wireKindNull => const OracleCell(OracleCellKind.null_, null),
      _wireKindBytes =>
        OracleCell(OracleCellKind.bytes, v is Uint8List ? v : Uint8List(0)),
      _wireKindString => OracleCell(OracleCellKind.string, (v as String?) ?? ''),
      _wireKindInt => OracleCell(OracleCellKind.int_, (v as num?)?.toInt() ?? 0),
      _wireKindUint =>
        OracleCell(OracleCellKind.uint, (v as num?)?.toInt() ?? 0),
      _wireKindFloat =>
        OracleCell(OracleCellKind.float_, (v as num?)?.toDouble() ?? 0.0),
      _wireKindDouble =>
        OracleCell(OracleCellKind.double_, (v as num?)?.toDouble() ?? 0.0),
      _wireKindDateTime =>
        OracleCell(OracleCellKind.datetime, (v as num?)?.toInt() ?? 0),
      _ => OracleCell(OracleCellKind.other, v),
    };
  }

  Uint8List asBytes() {
    return switch (kind) {
      OracleCellKind.bytes => (value as Uint8List?) ?? Uint8List(0),
      OracleCellKind.string =>
        Uint8List.fromList(utf8.encode((value as String?) ?? '')),
      _ => Uint8List(0),
    };
  }

  int? asInt() {
    return switch (kind) {
      OracleCellKind.null_ => null,
      OracleCellKind.int_ => value as int?,
      OracleCellKind.uint => value as int?,
      _ => null,
    };
  }

  int? asUInt() {
    return switch (kind) {
      OracleCellKind.null_ => null,
      OracleCellKind.uint => value as int?,
      OracleCellKind.int_ => value as int?,
      _ => null,
    };
  }

  double? asDouble() {
    return switch (kind) {
      OracleCellKind.null_ => null,
      OracleCellKind.float_ => (value as num?)?.toDouble(),
      OracleCellKind.double_ => (value as num?)?.toDouble(),
      OracleCellKind.int_ => (value as num?)?.toDouble(),
      OracleCellKind.uint => (value as num?)?.toDouble(),
      _ => null,
    };
  }

  DateTime? asDateTimeUtc() {
    return switch (kind) {
      OracleCellKind.null_ => null,
      OracleCellKind.datetime => DateTime.fromMillisecondsSinceEpoch(
          (value as int?) ?? 0,
          isUtc: true,
        ),
      _ => null,
    };
  }

  String? asString() {
    return switch (kind) {
      OracleCellKind.null_ => null,
      OracleCellKind.bytes => utf8.decode(asBytes(), allowMalformed: true),
      OracleCellKind.string => (value as String?) ?? '',
      OracleCellKind.datetime => asDateTimeUtc()?.toIso8601String() ?? '',
      OracleCellKind.int_ => ((value as int?) ?? 0).toString(),
      OracleCellKind.uint => ((value as int?) ?? 0).toString(),
      OracleCellKind.float_ => ((value as num?)?.toDouble() ?? 0.0).toString(),
      OracleCellKind.double_ => ((value as num?)?.toDouble() ?? 0.0).toString(),
      OracleCellKind.other => value?.toString() ?? '',
    };
  }
}

class _OracleNative {
  _OracleNative._();
  static final _OracleNative instance = _OracleNative._();

  Future<SendPort>? _sendPortFuture;
  int _nextId = 0;

  final Map<int, Completer<Object?>> _pending = {};

  Future<int> open(String dsn) async {
    final sendPort = await (_sendPortFuture ??= _spawnHelper());
    final id = _nextId++;
    final c = Completer<Object?>();
    _pending[id] = c;
    sendPort.send(_OpenRequest(id, dsn));
    final v = await c.future;
    return v as int;
  }

  Future<OracleQueryResult> query(int handle, String sql) async {
    final sendPort = await (_sendPortFuture ??= _spawnHelper());
    final id = _nextId++;
    final c = Completer<Object?>();
    _pending[id] = c;
    sendPort.send(_QueryRequest(id, handle, sql));
    final v = await c.future;
    return OracleQueryResult.fromNativeMap((v as Map).cast<String, Object?>());
  }

  Stream<OracleQueryStreamItem> queryStream(
    int handle,
    String sql, {
    int batchSize = 256,
  }) async* {
    final queryHandle = _nativeQueryOpen(handle, sql);
    try {
      final header = _nativeQueryHeader(queryHandle);
      final rawColumns = (header['columns'] as List<dynamic>? ?? const [])
          .cast<Map<String, dynamic>>();
      final columns = rawColumns
          .map((c) => OracleColumn(
                name: (c['name'] as String?) ?? '',
                typeName: (c['column_type'] as String?) ?? '',
              ))
          .toList(growable: false);
      final affectedRows = _parseAffectedRows(header['affected_rows']);

      yield OracleQueryStreamHeader(
        columns: columns,
        affectedRows: affectedRows,
      );

      while (true) {
        final batch = _nativeQueryNextBatch(queryHandle, batchSize);
        if (batch == null) break;
        for (final row in batch) {
          final values = (row['values'] as List<dynamic>? ?? const [])
              .cast<Map<String, Object?>>();
          final cells = values
              .map((cellMap) => OracleCell.fromWireMap(cellMap))
              .toList(growable: false);
          yield OracleQueryStreamRow(cells);
        }
      }
    } finally {
      _nativeQueryClose(queryHandle);
    }
  }

  Future<void> close(int handle) async {
    final sendPort = await (_sendPortFuture ??= _spawnHelper());
    final id = _nextId++;
    final c = Completer<Object?>();
    _pending[id] = c;
    sendPort.send(_CloseRequest(id, handle));
    await c.future;
  }

  Future<SendPort> _spawnHelper() async {
    final completer = Completer<SendPort>();
    final receivePort = ReceivePort()
      ..listen((dynamic data) {
        if (data is SendPort) {
          completer.complete(data);
          return;
        }
        if (data is _Response) {
          final pending = _pending.remove(data.id);
          if (pending == null) return;
          if (data is _ErrorResponse) {
            pending.completeError(Exception(data.message));
          } else if (data is _ValueResponse) {
            pending.complete(data.value);
          } else {
            pending.complete(null);
          }
          return;
        }
        throw UnsupportedError('Unsupported message type: ${data.runtimeType}');
      });

    await Isolate.spawn(_helperMain, receivePort.sendPort);
    return completer.future;
  }
}

sealed class _Request {
  final int id;
  const _Request(this.id);
}

class _OpenRequest extends _Request {
  final String dsn;
  const _OpenRequest(super.id, this.dsn);
}

class _QueryRequest extends _Request {
  final int handle;
  final String sql;
  const _QueryRequest(super.id, this.handle, this.sql);
}

class _CloseRequest extends _Request {
  final int handle;
  const _CloseRequest(super.id, this.handle);
}

sealed class _Response {
  final int id;
  const _Response(this.id);
}

class _ValueResponse extends _Response {
  final Object? value;
  const _ValueResponse(super.id, this.value);
}

class _OkResponse extends _Response {
  const _OkResponse(super.id);
}

class _ErrorResponse extends _Response {
  final String message;
  const _ErrorResponse(super.id, this.message);
}

void _helperMain(SendPort mainSendPort) {
  final helperReceivePort = ReceivePort()
    ..listen((dynamic data) {
      if (data is _OpenRequest) {
        try {
          final handle = _nativeOpen(data.dsn);
          mainSendPort.send(_ValueResponse(data.id, handle));
        } catch (e) {
          mainSendPort.send(_ErrorResponse(data.id, e.toString()));
        }
        return;
      }
      if (data is _QueryRequest) {
        try {
          final payload = _nativeQuery(data.handle, data.sql);
          mainSendPort.send(_ValueResponse(data.id, payload));
        } catch (e) {
          mainSendPort.send(_ErrorResponse(data.id, e.toString()));
        }
        return;
      }
      if (data is _CloseRequest) {
        try {
          _nativeClose(data.handle);
          mainSendPort.send(_OkResponse(data.id));
        } catch (e) {
          mainSendPort.send(_ErrorResponse(data.id, e.toString()));
        }
        return;
      }
      throw UnsupportedError('Unsupported message type: ${data.runtimeType}');
    });

  mainSendPort.send(helperReceivePort.sendPort);
}

int _nativeOpen(String dsn) {
  final dsnPtr = dsn.toNativeUtf8();
  final errOut = calloc<Pointer<Char>>();
  final handle = _bindings.oracle_open(dsnPtr.cast<Char>(), errOut);
  malloc.free(dsnPtr);

  final errPtr = errOut.value;
  calloc.free(errOut);

  if (handle == 0) {
    final msg = errPtr == nullptr
        ? 'oracle_open failed'
        : errPtr.cast<Utf8>().toDartString();
    if (errPtr != nullptr) _bindings.oracle_free_string(errPtr);
    throw Exception(msg);
  }

  if (errPtr != nullptr) _bindings.oracle_free_string(errPtr);
  return handle;
}

Map<String, Object?> _nativeQuery(int handle, String sql) {
  final queryHandle = _nativeQueryOpen(handle, sql);
  try {
    final header = _nativeQueryHeader(queryHandle);
    final columns = (header['columns'] as List<dynamic>? ?? const [])
        .cast<Map<String, dynamic>>();
    final affectedRows = header['affected_rows'];
    final rows = <Map<String, Object?>>[];

    while (true) {
      final batch = _nativeQueryNextBatch(queryHandle, 256);
      if (batch == null) break;
      rows.addAll(batch);
    }

    return {
      'columns': columns,
      'rows': rows,
      'affected_rows': affectedRows,
    };
  } finally {
    _nativeQueryClose(queryHandle);
  }
}

int _nativeQueryOpen(int handle, String sql) {
  final sqlPtr = sql.toNativeUtf8();
  final errOut = calloc<Pointer<Char>>();
  final queryHandle =
      _bindings.oracle_query_open(handle, sqlPtr.cast<Char>(), errOut);
  malloc.free(sqlPtr);

  final errPtr = errOut.value;
  calloc.free(errOut);

  if (queryHandle == 0) {
    final msg = errPtr == nullptr
        ? 'oracle_query_open failed'
        : errPtr.cast<Utf8>().toDartString();
    if (errPtr != nullptr) _bindings.oracle_free_string(errPtr);
    throw Exception(msg);
  }
  if (errPtr != nullptr) _bindings.oracle_free_string(errPtr);
  return queryHandle;
}

void _nativeQueryClose(int queryHandle) {
  _bindings.oracle_query_close(queryHandle);
}

Map<String, Object?> _nativeQueryHeader(int queryHandle) {
  final errOut = calloc<Pointer<Char>>();
  final ptr = _bindings.oracle_query_header(queryHandle, errOut);
  final errPtr = errOut.value;
  calloc.free(errOut);
  if (errPtr != nullptr) {
    final msg = errPtr.cast<Utf8>().toDartString();
    _bindings.oracle_free_string(errPtr);
    throw Exception(msg);
  }
  if (ptr == nullptr) {
    throw Exception('oracle_query_header failed');
  }

  try {
    final nativeHeader = ptr.ref;
    final columns = <Map<String, Object?>>[];
    final colCount = nativeHeader.column_count;
    if (colCount > 0 && nativeHeader.columns != nullptr) {
      final colPtr = nativeHeader.columns;
      for (var i = 0; i < colCount; i++) {
        final col = (colPtr + i).ref;
        columns.add({
          'name': _charPtrToString(col.name) ?? '',
          'column_type': _charPtrToString(col.column_type) ?? '',
        });
      }
    }
    return {
      'columns': columns,
      'affected_rows': nativeHeader.affected_rows,
    };
  } finally {
    _bindings.oracle_query_free_header(ptr);
  }
}

List<Map<String, Object?>>? _nativeQueryNextBatch(
  int queryHandle,
  int batchSize,
) {
  final errOut = calloc<Pointer<Char>>();
  final ptr = _bindings.oracle_query_next_batch(queryHandle, batchSize, errOut);
  final errPtr = errOut.value;
  calloc.free(errOut);
  if (errPtr != nullptr) {
    final msg = errPtr.cast<Utf8>().toDartString();
    _bindings.oracle_free_string(errPtr);
    throw Exception(msg);
  }
  if (ptr == nullptr) {
    throw Exception('oracle_query_next_batch failed');
  }
  try {
    final nativeBatch = ptr.ref;
    if (nativeBatch.done != 0) return null;

    final rows = <Map<String, Object?>>[];
    final rowCount = nativeBatch.row_count;
    if (rowCount <= 0 || nativeBatch.rows == nullptr) {
      return rows;
    }

    final rowPtr = nativeBatch.rows;
    for (var i = 0; i < rowCount; i++) {
      final row = (rowPtr + i).ref;
      final values = <Map<String, Object?>>[];
      if (row.value_count > 0 && row.values != nullptr) {
        final valuePtr = row.values;
        for (var j = 0; j < row.value_count; j++) {
          final value = (valuePtr + j).ref;
          values.add(_nativeValueToWireMap(value));
        }
      }
      rows.add({'values': values});
    }
    return rows;
  } finally {
    _bindings.oracle_query_free_batch(ptr);
  }
}

void _nativeClose(int handle) {
  _bindings.oracle_close(handle);
}

String? _charPtrToString(Pointer<Char> ptr) {
  if (ptr == nullptr) return null;
  return ptr.cast<Utf8>().toDartString();
}

Map<String, Object?> _nativeValueToWireMap(oracle_query_value_t value) {
  switch (value.kind) {
    case _wireKindNull:
      return const {'kind': _wireKindNull, 'value': null};
    case _wireKindBytes:
      final bytes = _readBytes(value.bytes_value, value.bytes_len);
      return {'kind': _wireKindBytes, 'value': bytes};
    case _wireKindInt:
      return {'kind': _wireKindInt, 'value': value.int_value};
    case _wireKindUint:
      return {'kind': _wireKindUint, 'value': value.uint_value};
    case _wireKindFloat:
      return {'kind': _wireKindFloat, 'value': value.float_value.toDouble()};
    case _wireKindDouble:
      return {'kind': _wireKindDouble, 'value': value.double_value};
    case _wireKindDateTime:
      return {'kind': _wireKindDateTime, 'value': value.datetime_value};
    case _wireKindString:
      final bytes = _readBytes(value.bytes_value, value.bytes_len);
      return {
        'kind': _wireKindString,
        'value': utf8.decode(bytes, allowMalformed: true),
      };
    default:
      final bytes = _readBytes(value.bytes_value, value.bytes_len);
      return {'kind': _wireKindBytes, 'value': bytes};
  }
}

Uint8List _readBytes(Pointer<Uint8> ptr, int length) {
  if (ptr == nullptr || length <= 0) return Uint8List(0);
  return Uint8List.fromList(ptr.asTypedList(length));
}

BigInt _parseAffectedRows(Object? value) {
  if (value == null) return BigInt.zero;
  if (value is int) return BigInt.from(value);
  if (value is num) return BigInt.from(value.toInt());
  if (value is String) return BigInt.tryParse(value) ?? BigInt.zero;
  return BigInt.zero;
}
