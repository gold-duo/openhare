library;

import 'dart:async';

import 'package:postgres/postgres.dart';

/// Header item for streaming query result.
class PGStreamHeader {
  final ResultSchema schema;
  final int affectedRows;

  PGStreamHeader(this.schema, this.affectedRows);
}

/// Stream item: header or row. Use `is PGStreamHeader` / `is ResultRow` to distinguish.
typedef PGStreamItem = Object;

class PGConn {
  Connection conn;

  PGConn(this.conn);

  static Future<PGConn> open({
    required Endpoint endpoint,
    Duration? connectTimeout,
    Duration? queryTimeout,
  }) async {
    final conn = await Connection.open(endpoint,
        settings: ConnectionSettings(
          sslMode: SslMode.disable, //todo: 需要判断是否需要开启ssl
          connectTimeout: connectTimeout,
          queryTimeout: queryTimeout,
        ));
    return PGConn(conn);
  }

  Future<Result> query({required String query}) async {
    return conn.execute(query);
  }

  /// 流式执行查询，使用 prepare+bind 逐行返回结果。
  /// 先发送 header（schema），然后逐行 yield，实现真正的流式。
  Stream<PGStreamItem> queryStream({required String query}) async* {
    final stmt = await conn.prepare(Sql(query));
    try {
      final stream = stmt.bind(null);
      final rowController = StreamController<ResultRow>(sync: true);
      final subscription = stream.listen(
        rowController.add,
        onDone: rowController.close,
        onError: rowController.addError,
      );

      final schema = await subscription.schema;
      // INSERT/UPDATE/DELETE 无返回列，可等待 affectedRows 获取真实数量
      // SELECT 有返回列，为保持流式不等待，affectedRows 为 0
      final affectedRows = schema.columns.isEmpty
          ? await subscription.affectedRows
          : 0;
      yield PGStreamHeader(schema, affectedRows);

      await for (final row in rowController.stream) {
        yield row;
      }
    } finally {
      await stmt.dispose();
    }
  }

  Future<void> close() {
    return conn.close();
  }
}
