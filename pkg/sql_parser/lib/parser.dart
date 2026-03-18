library;

export 'src/lexer/lexer.dart';
export 'src/parser/parser.dart';
export 'src/parser/match.dart';
export 'src/lexer/token.dart';
export 'src/lexer/scanner.dart';
export 'src/dialect/mysql/keyword.dart';

import 'package:sql_parser/src/parser/match.dart';

import 'src/dialect/mysql/lexer.dart';
import 'src/dialect/mysql/parser.dart';
import 'src/lexer/lexer.dart';
import 'src/parser/parser.dart';

// 定义方言类型枚举
enum DialectType { mysql }

Lexer createLexer(DialectType dialect, String content) {
  switch (dialect) {
    case DialectType.mysql:
      return MySQLLexer(content);
  }
}

List<SQLChunk> splitSQL(DialectType dialect, String content,
    {String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
  switch (dialect) {
    case DialectType.mysql:
      return MysqlSplitter(content).split(skipWhitespace: skipWhitespace, skipComment: skipComment);
  }
}

bool match(DialectType dialect, String content, String pattern) {
  switch (dialect) {
    case DialectType.mysql:
      return Matcher(createLexer(dialect, content)).match(pattern);
  }
}

SQLDefiner parser(DialectType dialect, String content) {
  switch (dialect) {
    case DialectType.mysql:
      return MysqlSQLDefiner(content);
  }
}