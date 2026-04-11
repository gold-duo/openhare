import 'package:sql_parser/src/parser/parser.dart';
import 'package:sql_parser/src/parser/match.dart';
import 'package:sql_parser/src/lexer/token.dart';

import 'lexer.dart';

class SqliteSplitter extends Splitter {
  SqliteSplitter(String content) : super(SqliteLexer(content));

  @override
  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    Token? splitWhereFunc() => l.scanWhere(
          (tok) => (tok.id == TokenType.punctuation && tok.content == delimiter),
        );
    return splitWhere(splitWhereFunc, skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

class SqliteSQLDefiner extends SQLDefiner {
  final String content;
  SqliteSQLDefiner(this.content);

  @override
  SQLType get sqlType {
    if (Matcher(SqliteLexer(content)).match("{select|pragma|explain|with} {*}")) {
      if (Matcher(SqliteLexer(content)).match("with {*} select {*}")) {
        return SQLType.dql;
      }
      if (Matcher(SqliteLexer(content)).match("with {*}")) {
        return SQLType.dml;
      }
      return SQLType.dql;
    }

    if (Matcher(SqliteLexer(content)).match("{insert|update|delete|replace} {*}")) {
      return SQLType.dml;
    }

    if (Matcher(SqliteLexer(content)).match("{create|alter|drop|reindex|vacuum|attach|detach|truncate|rename} {*}")) {
      return SQLType.ddl;
    }

    return SQLType.other;
  }

  @override
  bool get isDangerousSQL {
    if (Matcher(SqliteLexer(content)).match("{drop|truncate|vacuum} {*}")) {
      return true;
    }
    if (Matcher(SqliteLexer(content)).match("{delete|update} {*}")) {
      if (!Matcher(SqliteLexer(content)).match("{delete|update} {*} where {*}")) {
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
    return Matcher(SqliteLexer(content)).match("select {*}");
  }

  @override
  bool get changeSchema {
    return Matcher(SqliteLexer(content)).match("attach {*} as {*}") ||
        Matcher(SqliteLexer(content)).match("detach {*}") ||
        Matcher(SqliteLexer(content)).match("detach database {*}");
  }

  @override
  String wrapLimit(String sql, int limit) {
    if (!Matcher(SqliteLexer(content)).match("select {*}")) {
      return sql;
    }
    return "SELECT * FROM ($sql) AS dt_1 LIMIT $limit";
  }

  @override
  String trimDelimiter(String sql) {
    final sql = SqliteLexer(content).trimEndWhere((token) {
      return token.id == TokenType.whitespace ||
          token.id == TokenType.comment ||
          (token.id == TokenType.punctuation && token.content == ";");
    });
    return sql;
  }
}
