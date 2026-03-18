import '../lexer/lexer.dart';
import '../lexer/token.dart';
import '../lexer/scanner.dart';

enum SQLType {
  dql,
  dml,
  ddl,
  dcl,
  other,
}

enum RiskLevel {
  low,
  medium,
  high,
}

class SQLChunk {
  String content;
  Pos start;
  Pos end;

  SQLChunk(this.start, this.end, this.content);

  SQLChunk.empty() : this(Pos.none(), Pos.none(), "");

  @override
  String toString() =>
      "cursor:[${start.cursor} - ${end.cursor}]; pos:[${start.line}:${start.row} - ${end.line}:${end.row}]; content: $content";
}

abstract class Splitter {
  Lexer l;
  Splitter(this.l);

  List<SQLChunk> splitWhere(
    Token? Function() splitWhereFunc, {
    bool skipWhitespace = false,
    bool skipComment = false,
  }) {
    bool skipFunc(Token token) {
      if (skipComment && token.id == TokenType.comment) {
        return true;
      }
      if (skipWhitespace && token.id == TokenType.whitespace) {
        return true;
      }
      return false;
    }

    /*
      找下一个分号, 存在两种情况:
        1. 没有分号会一直匹配到结束,
        2. 若有分号则将分号及之前的字符串片段切下来，剩余部分继续找分号直到情况1.
    */
    List<SQLChunk> sqlList = List.empty(growable: true);
    while (true) {
      // 获取第一个token的位置，并跳过指定字符.
      Pos? startPos = l.first(skipFunc)?.startPos;
      if (startPos == null) {
        return sqlList;
      }
      Token? tok = splitWhereFunc();
      if (tok == null) {
        sqlList.add(SQLChunk(startPos, l.scanner.pos, l.scanner.subString(startPos, l.scanner.pos)));
        return sqlList;
      }
      sqlList.add(SQLChunk(startPos, tok.endPos, l.scanner.subString(startPos, tok.endPos)));

      // 当最后一个字符是";", 直接跳过了.
      if (!l.scanner.hasNext()) {
        return sqlList;
      }
    }
  }

  List<SQLChunk> split({String delimiter = ";", bool skipWhitespace = false, bool skipComment = false}) {
    throw Exception("Not implemented");
  }
}

abstract class DBTypeMatcher {
  SQLType get sqlType {
    throw Exception("Not implemented");
  }
}

abstract class SQLLimitWrapper {
  String wrapLimit({int limit = 100, int offset = 0}) {
    throw Exception("Not implemented");
  }
}

abstract class SQLDefiner {
  SQLType get sqlType {
    throw Exception("Not implemented");
  }

  bool get isDangerousSQL {
    throw Exception("Not implemented");
  }

  bool get canLimit {
    throw Exception("Not implemented");
  }

  bool get changeSchema {
    throw Exception("Not implemented");
  }

  String wrapLimit({int limit = 100, int offset = 0}) {
    throw Exception("Not implemented");
  }
}
