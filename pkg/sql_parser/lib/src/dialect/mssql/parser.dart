import 'package:sql_parser/src/parser/parser.dart';
import 'package:sql_parser/src/parser/match.dart';
import 'package:sql_parser/src/lexer/token.dart';

import 'lexer.dart';

class MssqlSplitter extends Splitter {
  MssqlSplitter(String content) : super(MssqlLexer(content));

  @override
  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    Token? splitWhereFunc() => l.scanWhere(
          (tok) => (tok.id == TokenType.punctuation && tok.content == delimiter),
        );
    return splitWhere(splitWhereFunc, skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

class MssqlSQLDefiner extends SQLDefiner {
  final String content;
  MssqlSQLDefiner(this.content);

  @override
  SQLType get sqlType {
    if (Matcher(MssqlLexer(content)).match("{select|with} {*}")) {
      if (Matcher(MssqlLexer(content)).match("with {*} select {*}")) {
        return SQLType.dql;
      }
      if (Matcher(MssqlLexer(content)).match("select {*}")) {
        return SQLType.dql;
      }
      return SQLType.dml;
    }

    if (Matcher(MssqlLexer(content)).match("{insert|update|delete|merge} {*}")) {
      return SQLType.dml;
    }

    if (Matcher(MssqlLexer(content)).match("{create|alter|drop|truncate} {*}")) {
      return SQLType.ddl;
    }

    if (Matcher(MssqlLexer(content)).match("{grant|revoke|deny} {*}")) {
      return SQLType.dcl;
    }

    return SQLType.other;
  }

  @override
  bool get isDangerousSQL {
    if (Matcher(MssqlLexer(content)).match("{truncate|drop} {*}")) {
      return true;
    }
    if (Matcher(MssqlLexer(content)).match("{delete|update} {*}")) {
      if (!Matcher(MssqlLexer(content)).match("{delete|update} {*} where {*}")) {
        return true;
      }
    }
    return false;
  }

  @override
  bool get canLimit {
    return Matcher(MssqlLexer(content)).match("select {*}");
  }

  @override
  bool get changeSchema {
    return Matcher(MssqlLexer(content)).match("use {*}");
  }

  @override
  String wrapLimit({int limit = 100}) {
    // todo: 实现 MSSQL 分页查询, 直接包裹子查询的方式不行，内部不能用order by
    if (!Matcher(MssqlLexer(content)).match("select {*}")) {
      return content;
    }

    final sql = MssqlLexer(content).trimEndWhere((token) {
      return token.id == TokenType.whitespace ||
          token.id == TokenType.comment ||
          (token.id == TokenType.punctuation && token.content == ";");
    });
    return "SELECT TOP ($limit) * FROM ($sql) AS dt_1;";
  }
}
