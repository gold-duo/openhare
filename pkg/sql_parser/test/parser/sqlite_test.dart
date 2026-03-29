import 'package:sql_parser/parser.dart';
import 'package:sql_parser/src/lexer/token.dart';
import 'package:sql_parser/src/parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('sqlite lexer keyword/comment/bracket/backtick', () {
    final l = createLexer(
      DialectType.sqlite,
      "-- line\nselect [t1].[col], `x` from \"t1\"",
    );

    final first = l.firstTrim();
    expect(first, isNotNull);
    expect(first!.id, TokenType.keyword);
    expect(first.content, "select");
  });

  test('sqlite splitter split by semicolon', () {
    final chunks = splitSQL(
      DialectType.sqlite,
      "select * from t1;update t1 set a=1 where id=1;",
      skipWhitespace: true,
      skipComment: true,
    );
    expect(chunks.length, 2);
    expect(chunks.first.content.toLowerCase().startsWith("select"), isTrue);
    expect(chunks.last.content.toLowerCase().startsWith("update"), isTrue);
  });

  test('sqlite sql type', () {
    expect(parser(DialectType.sqlite, "select * from t1").sqlType, SQLType.dql);
    expect(parser(DialectType.sqlite, "pragma table_info(t1)").sqlType, SQLType.dql);
    expect(parser(DialectType.sqlite, "with cte as (select 1) select * from cte").sqlType, SQLType.dql);
    expect(parser(DialectType.sqlite, "with cte as (select 1) update t1 set a=1").sqlType, SQLType.dml);
    expect(parser(DialectType.sqlite, "create table t1(id integer)").sqlType, SQLType.ddl);
  });

  test('sqlite dangerous sql', () {
    expect(parser(DialectType.sqlite, "update t1 set a=1").isDangerousSQL, isTrue);
    expect(parser(DialectType.sqlite, "update t1 set a=1 where id=1").isDangerousSQL, isFalse);
    expect(parser(DialectType.sqlite, "drop table t1").isDangerousSQL, isTrue);
  });

  test('sqlite change schema', () {
    expect(parser(DialectType.sqlite, "attach 'a.db' as db2").changeSchema, isTrue);
    expect(parser(DialectType.sqlite, "detach database db2").changeSchema, isTrue);
    expect(parser(DialectType.sqlite, "select 1").changeSchema, isFalse);
  });

  test('sqlite wrap limit', () {
    final wrapped = parser(DialectType.sqlite, "select * from t1;").wrapLimit(limit: 20);
    expect(wrapped, "SELECT * FROM (select * from t1) AS dt_1 LIMIT 20");
  });
}
