import 'package:sql_parser/src/parser/parser.dart';
import 'package:sql_parser/src/parser/match.dart';
import 'package:sql_parser/src/lexer/token.dart';

import 'lexer.dart';

class OracleSplitter extends Splitter {
  OracleSplitter(String content) : super(OracleLexer(content));

  @override
  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    Token? splitWhereFunc() => l.scanWhere(
          (tok) => (tok.id == TokenType.punctuation && tok.content == delimiter),
        );

    return splitWhere(splitWhereFunc, skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

class OracleSQLDefiner extends SQLDefiner {
  final String content;
  OracleSQLDefiner(this.content);

  @override
  SQLType get sqlType {
    // DQL: 查询类.
    if (Matcher(OracleLexer(content)).match("{select|with|explain|desc|describe} {*}")) {
      return SQLType.dql;
    }

    // DML: 数据变更类.
    if (Matcher(OracleLexer(content)).match("{insert|update|delete|merge|call} {*}")) {
      return SQLType.dml;
    }

    // DDL: 结构定义类.
    if (Matcher(OracleLexer(content)).match("{create|alter|drop|truncate|rename|comment} {*}")) {
      return SQLType.ddl;
    }

    // DCL: 权限控制类.
    if (Matcher(OracleLexer(content)).match("{grant|revoke} {*}")) {
      return SQLType.dcl;
    }

    return SQLType.other;
  }

  @override
  bool get isDangerousSQL {
    if (Matcher(OracleLexer(content)).match("{truncate|drop} {*}")) {
      return true;
    }
    if (Matcher(OracleLexer(content)).match("{delete|update} {*}")) {
      if (!Matcher(OracleLexer(content)).match("{delete|update} {*} where {*}")) {
        return true;
      }
    }
    return false;
  }

  @override
  bool get canLimit {
    return Matcher(OracleLexer(content)).match("select {*}");
  }

  @override
  bool get changeSchema {
    return Matcher(OracleLexer(content)).match("alter session set current_schema {*}");
  }

  @override
  String wrapLimit({int limit = 10, int offset = 0}) {
    if (!Matcher(OracleLexer(content)).match("select {*}")) {
      return content;
    }

    // 去掉结尾空白/注释/分号后再包裹.
    final sql = OracleLexer(content).trimEndWhere((token) {
      return token.id == TokenType.whitespace || token.id == TokenType.comment || token.id == TokenType.punctuation;
    });

    if (offset <= 0) {
      return "SELECT * FROM ($sql) dt_1 WHERE ROWNUM <= $limit";
    }

    final maxRowNum = offset + limit;
    return "SELECT * FROM (SELECT dt_1.*, ROWNUM rn_ FROM ($sql) dt_1 WHERE ROWNUM <= $maxRowNum) WHERE rn_ > $offset";
  }
}