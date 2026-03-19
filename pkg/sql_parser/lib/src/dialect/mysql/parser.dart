import 'package:sql_parser/src/parser/parser.dart';
import 'package:sql_parser/src/parser/match.dart';
import 'package:sql_parser/src/lexer/token.dart';

import 'lexer.dart';

class MysqlSplitter extends Splitter {
  MysqlSplitter(String content) : super(MySQLLexer(content));

  @override
  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    Token? splitWhereFunc() => l.scanWhere(
          (tok) => (tok.id == TokenType.punctuation && tok.content == delimiter),
        );

    return splitWhere(splitWhereFunc, skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

class MysqlSQLDefiner extends SQLDefiner {
  final String content;
  MysqlSQLDefiner(this.content);

  @override
  SQLType get sqlType {
    // DQL: 数据查询相关.
    if (Matcher(MySQLLexer(content)).match("{select|show|desc|describe|explain} {*}")) {
      return SQLType.dql;
    }

    // with 语句需要特殊处理.
    if (Matcher(MySQLLexer(content)).match("with {*}")) {
      if (Matcher(MySQLLexer(content)).match("with {*} select {*} from {*}")) {
        return SQLType.dql;
      }
      return SQLType.dml;
    }

    // DML: 数据变更相关.
    if (Matcher(MySQLLexer(content)).match("{insert|update|delete|replace|load|call|do} {*}")) {
      return SQLType.dml;
    }

    // DDL: 结构定义相关.
    if (Matcher(MySQLLexer(content)).match("{create|alter|drop|truncate|rename|analyze|optimize|repair} {*}")) {
      return SQLType.ddl;
    }

    // DCL: 权限控制相关.
    if (Matcher(MySQLLexer(content)).match("{grant|revoke} {*}")) {
      return SQLType.dcl;
    }

    // MySQL 里事务/会话/运维类语句较多，统一归类为 other.
    return SQLType.other;
  }

  @override
  bool get isDangerousSQL {
    // truncate, drop, delete|update without where.
    if (Matcher(MySQLLexer(content)).match("{truncate|drop} {*} ")) {
      return true;
    }
    if (Matcher(MySQLLexer(content)).match("{delete|update} {*}")) {
      if (!Matcher(MySQLLexer(content)).match("{delete|update} {*} where {*}")) {
        return true;
      }
    }
    return false;
  }

  @override
  bool get canLimit {
    return Matcher(MySQLLexer(content)).match("select {*}");
  }

  @override
  bool get changeSchema {
    return Matcher(MySQLLexer(content)).match("use {*}");
  }

  @override
  String wrapLimit({int limit = 10, int offset = 0}) {
    if (Matcher(MySQLLexer(content)).match("select {*}")) {
      // 去掉结尾的注释和分号，这样才能被子查询包裹
      final sql = MySQLLexer(content).trimEndWhere((token) {
        return token.id == TokenType.whitespace || token.id == TokenType.comment || token.id == TokenType.punctuation;
      });
      return "SELECT * FROM ($sql) AS dt_1 LIMIT $limit OFFSET $offset;";
    }
    return content;
  }
}
