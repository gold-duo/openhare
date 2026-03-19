import 'package:sql_parser/parser.dart';
import 'package:sql_parser/src/lexer/token.dart';
import 'package:sql_parser/src/parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('oracle lexer keyword and comment', () {
    final l = createLexer(
      DialectType.oracle,
      "--line comment\nselect * from dual /* tail */",
    );

    final first = l.firstTrim();
    expect(first, isNotNull);
    expect(first!.id, TokenType.keyword);
    expect(first.content, "select");
  });

  test('oracle splitter split by semicolon', () {
    final chunks = splitSQL(
      DialectType.oracle,
      "select * from dual;update t set a=1 where id=1;",
      skipWhitespace: true,
      skipComment: true,
    );
    expect(chunks.length, 2);
    expect(chunks.first.content.toLowerCase().startsWith("select"), isTrue);
    expect(chunks.last.content.toLowerCase().startsWith("update"), isTrue);
  });

  test('oracle definer sql type', () {
    expect(parser(DialectType.oracle, "select * from dual").sqlType, SQLType.dql);
    expect(parser(DialectType.oracle, "merge into t1 using t2 on (t1.id=t2.id)").sqlType, SQLType.dml);
    expect(parser(DialectType.oracle, "create table t1(id number)").sqlType, SQLType.ddl);
    expect(parser(DialectType.oracle, "grant select on t1 to u1").sqlType, SQLType.dcl);
  });

  test('oracle definer dangerous sql', () {
    expect(parser(DialectType.oracle, "delete from t1").isDangerousSQL, isTrue);
    expect(parser(DialectType.oracle, "delete from t1 where id=1").isDangerousSQL, isFalse);
  });

  test('oracle wrap limit', () {
    final wrapped1 = parser(DialectType.oracle, "select * from dual;").wrapLimit(limit: 10);
    expect(wrapped1, "SELECT * FROM (select * from dual) dt_1 WHERE ROWNUM <= 10");

    final wrapped2 = parser(DialectType.oracle, "select * from dual").wrapLimit(limit: 10, offset: 5);
    expect(
      wrapped2,
      "SELECT * FROM (SELECT dt_1.*, ROWNUM rn_ FROM (select * from dual) dt_1 WHERE ROWNUM <= 15) WHERE rn_ > 5",
    );
  });
}
