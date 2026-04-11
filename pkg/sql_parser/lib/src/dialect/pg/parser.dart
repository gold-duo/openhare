import 'package:sql_parser/src/parser/parser.dart';
import 'package:sql_parser/src/parser/match.dart';
import 'package:sql_parser/src/lexer/token.dart';

import 'lexer.dart';

class PgSplitter extends Splitter {
  PgSplitter(String content) : super(PgLexer(content));

  @override
  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    Token? splitWhereFunc() => l.scanWhere(
          (tok) => (tok.id == TokenType.punctuation && tok.content == delimiter),
        );
    return splitWhere(splitWhereFunc, skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

class PgSQLDefiner extends SQLDefiner {
  final String content;
  PgSQLDefiner(this.content);

  @override
  SQLType get sqlType {
    if (Matcher(PgLexer(content)).match("{select|show|explain|values} {*}")) {
      return SQLType.dql;
    }

    if (Matcher(PgLexer(content)).match("with {*}")) {
      if (Matcher(PgLexer(content)).match("with {*} select {*}")) {
        return SQLType.dql;
      }
      return SQLType.dml;
    }

    if (Matcher(PgLexer(content)).match("{insert|update|delete|merge|copy|call} {*}")) {
      return SQLType.dml;
    }

    if (Matcher(PgLexer(content)).match("{create|alter|drop|truncate|rename|comment|reindex|refresh} {*}")) {
      return SQLType.ddl;
    }

    if (Matcher(PgLexer(content)).match("{grant|revoke} {*}")) {
      return SQLType.dcl;
    }

    return SQLType.other;
  }

  @override
  bool get isDangerousSQL {
    if (Matcher(PgLexer(content)).match("{truncate|drop} {*}")) {
      return true;
    }
    if (Matcher(PgLexer(content)).match("{delete|update} {*}")) {
      if (!Matcher(PgLexer(content)).match("{delete|update} {*} where {*}")) {
        return true;
      }
    }
    return false;
  }

  @override
  bool get canLimit {
    if (sqlType != SQLType.dql) {
      return false;
    }
    return Matcher(PgLexer(content)).match("select {*}");
  }

  @override
  bool get changeSchema {
    return Matcher(PgLexer(content)).match("set search_path {*}") ||
        Matcher(PgLexer(content)).match("set search_path to {*}");
  }

  @override
  String wrapLimit(String sql, int limit) {
    if (!Matcher(PgLexer(content)).match("select {*}")) {
      return sql;
    }
    return "SELECT * FROM ($sql) AS dt_1 LIMIT $limit";
  }

  @override
  String trimDelimiter(String sql) {
    final sql = PgLexer(content).trimEndWhere((token) {
      return token.id == TokenType.whitespace ||
          token.id == TokenType.comment ||
          (token.id == TokenType.punctuation && token.content == ";");
    });
    return sql;
  }
}
