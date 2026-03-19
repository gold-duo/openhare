import 'package:sql_parser/parser.dart';
import 'package:sql_parser/src/lexer/token.dart';
import 'package:sql_parser/src/parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('mssql lexer keyword/comment/bracket ident', () {
    final l = createLexer(
      DialectType.mssql,
      "-- line comment\nselect [user]]name] from [dbo].[t1]",
    );

    final first = l.firstTrim();
    expect(first, isNotNull);
    expect(first!.id, TokenType.keyword);
    expect(first.content, "select");
  });

  test('mssql splitter split by semicolon', () {
    final chunks = splitSQL(
      DialectType.mssql,
      "select * from [dbo].[t1];update [dbo].[t1] set a=1 where id=1;",
      skipWhitespace: true,
      skipComment: true,
    );
    expect(chunks.length, 2);
    expect(chunks.first.content.toLowerCase().startsWith("select"), isTrue);
    expect(chunks.last.content.toLowerCase().startsWith("update"), isTrue);
  });

  test('mssql sql type', () {
    expect(parser(DialectType.mssql, "select * from t1").sqlType, SQLType.dql);
    expect(parser(DialectType.mssql, "with cte as (select 1) select * from cte").sqlType, SQLType.dql);
    expect(parser(DialectType.mssql, "with cte as (select 1) update t1 set a=1").sqlType, SQLType.dml);
    expect(parser(DialectType.mssql, "create table t1(id int)").sqlType, SQLType.ddl);
    expect(parser(DialectType.mssql, "grant select on t1 to u1").sqlType, SQLType.dcl);
  });

  test('mssql dangerous sql', () {
    expect(parser(DialectType.mssql, "update t1 set a=1").isDangerousSQL, isTrue);
    expect(parser(DialectType.mssql, "update t1 set a=1 where id=1").isDangerousSQL, isFalse);
    expect(parser(DialectType.mssql, "drop table t1").isDangerousSQL, isTrue);
  });

  test('mssql change schema', () {
    expect(parser(DialectType.mssql, "use master").changeSchema, isTrue);
    expect(parser(DialectType.mssql, "select 1").changeSchema, isFalse);
  });

  test('mssql wrap limit', () {
    final wrapped = parser(DialectType.mssql, "select * from t1;").wrapLimit(limit: 20, offset: 0);
    expect(
      wrapped,
      "SELECT TOP (20) * FROM (select * from t1) AS dt_1;",
    );
  });

  test('mssql wrap limit with offset', () {
    final wrapped = parser(DialectType.mssql, "select * from t1;").wrapLimit(limit: 20, offset: 5);
    expect(
      wrapped,
      "SELECT * FROM (SELECT dt_1.*, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn_ FROM (select * from t1) AS dt_1) AS dt_2 WHERE dt_2.rn_ > 5 AND dt_2.rn_ <= 25;",
    );
  });
}
