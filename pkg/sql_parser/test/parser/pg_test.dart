import 'package:sql_parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('pg lexer keyword/comment', () {
    final l = createLexer(
      DialectType.pg,
      "-- line\nselect * from t1",
    );

    final first = l.firstTrim();
    expect(first, isNotNull);
    expect(first!.id, TokenType.keyword);
    expect(first.content, "select");
  });

  test('pg splitter with dollar quote body', () {
    final sql = r"""
CREATE FUNCTION f() RETURNS void AS $func$
BEGIN
  RAISE NOTICE 'a;b';
END;
$func$ LANGUAGE plpgsql;
SELECT 1;
""";
    final chunks = splitSQL(DialectType.pg, sql, skipWhitespace: true, skipComment: true);
    expect(chunks.length, 2);
    expect(chunks.first.content.toLowerCase().startsWith("create function"), isTrue);
    expect(chunks.last.content.toLowerCase().startsWith("select"), isTrue);
  });

  test('pg sql type', () {
    expect(parser(DialectType.pg, "select * from t1").sqlType, SQLType.dql);
    expect(parser(DialectType.pg, "with cte as (select 1) select * from cte").sqlType, SQLType.dql);
    expect(parser(DialectType.pg, "with cte as (select 1) update t1 set a=1").sqlType, SQLType.dml);
    expect(parser(DialectType.pg, "create table t1(id int)").sqlType, SQLType.ddl);
    expect(parser(DialectType.pg, "grant select on t1 to u1").sqlType, SQLType.dcl);
  });

  test('pg dangerous sql', () {
    expect(parser(DialectType.pg, "update t1 set a=1").isDangerousSQL, isTrue);
    expect(parser(DialectType.pg, "update t1 set a=1 where id=1").isDangerousSQL, isFalse);
  });

  test('pg change schema', () {
    expect(parser(DialectType.pg, "set search_path to public").changeSchema, isTrue);
    expect(parser(DialectType.pg, "set search_path public").changeSchema, isTrue);
    expect(parser(DialectType.pg, "select 1").changeSchema, isFalse);
  });

  test('pg wrap limit', () {
    final wrapped = parser(DialectType.pg, "select * from t1;").wrapLimit(20);
    expect(wrapped, "SELECT * FROM (select * from t1) AS dt_1 LIMIT 20");
  });
}
