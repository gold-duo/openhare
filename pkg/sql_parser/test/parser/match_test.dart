import 'package:sql_parser/parser.dart';
import 'package:test/test.dart';

void testMatch(String query, String pattern, bool expected) {
  final actual = match(DialectType.mysql, query, pattern);
  expect(actual, expected, reason: 'query: "$query", pattern: "$pattern"');
}

void main() {
  test('exact match', () {
    testMatch("select * from t1", "select * from t1", true);
  });

  test('skip matcher {N}', () {
    testMatch("select * from t1", "select * from {1}", true);
    testMatch("select * from t1", "select {1} from t1", true);
  });

  test('find matcher {*}', () {
    testMatch("select * from t1", "select * from {*}", true);
    testMatch("select * from t1", "select {*} from t1", true);
  });

  test('mismatch - wrong token', () {
    testMatch("select * from t1", "select1 * from t1", false);
  });

  test('mismatch - skip overflow', () {
    testMatch("select * from t1", "{1} select * from t1", false);
  });

  test('mismatch - skip count wrong', () {
    testMatch("select * from t1", "select {2} from t1", false);
  });

  test('mismatch - wrong keyword', () {
    testMatch("select * from t1", "select * from1 t1", false);
  });

  test('find across multiple tokens', () {
    testMatch("select id, name, age from t1", "select {*} from t1", true);
    testMatch("select id, name, age from t1", "select {*} from t1", true);
    testMatch("select id, name, age from t1", "select {*} from {*}", true);
    testMatch("select id, name, age from t1", "select {5} from {*}", true);
    testMatch("select id, name, age from t1 where id = 1", "select {*} from {*} where", true);
  });

  test('case insensitive', () {
    testMatch("SELECT * from t1", "select * from t1", true);
    testMatch("select * from t1", "SELECT * from t1", true);
  });

  test('update with find matcher', () {
    testMatch("update t1 set name='test' where id =2", "update {*} set {*} where {*}", true);
    testMatch("update t1 set name='test' where id =2", "update {*} set {*} where1 {*}", false);
  });

  test('parenthesized block - subquery does not leak', () {
    // {*} from 应该匹配外层 from，不会穿透到子查询里的 from
    testMatch("select * from (select id from t2) as sub", "select {*} from {*}", true);
    testMatch("select * from (select id from t2) as sub where 1=1", "select {*} from {*} where {*}", true);
  });

  test('parenthesized block - counted as one token', () {
    // (select id from t2) 整块算 1 个 token
    testMatch("select * from (select id from t2) as sub", "select * from {1} as sub", true);
    // count(*) 整块算 1 个 token, 加上 count 共 2 个
    testMatch("select count(*) from t1", "select {2} from t1", true);
  });

  test('parenthesized block - nested parens', () {
    testMatch("select func((a+b), c) from t1", "select {*} from t1", true);
    // func 1个 + (...) 1个 = 2
    testMatch("select func((a+b), c) from t1", "select {2} from t1", true);
  });

  test('parenthesized block - insert values', () {
    testMatch("insert into t1 (a, b) values (1, 2)", "insert into {*} values {*}", true);
    testMatch("insert into t1 (a, b) select * from t2", "insert into {*} select {*}", true);
  });

  test('or matcher {a|b}', () {
    testMatch("select * from t1", "{select|insert} {*}", true);
    testMatch("insert into t1 values (1)", "{select|insert} {*}", true);
    testMatch("update t1 set a=1", "{select|insert} {*}", false);
  });

  test('or matcher - case insensitive', () {
    testMatch("SELECT * from t1", "{select|insert} {*}", true);
    testMatch("INSERT into t1 values (1)", "{Select|Insert} {*}", true);
  });

  test('or matcher - as find target', () {
    // {*} {from|into} 搜索直到遇到 from 或 into
    testMatch("select id, name from t1", "select {*} {from|into} t1", true);
    testMatch("insert into t1 values (1)", "{select|insert} {*} {from|into} {*}", true);
  });

  test('or matcher - dml classification', () {
    testMatch("select * from t1", "{select|insert|update|delete} {*}", true);
    testMatch("insert into t1 (a) values (1)", "{select|insert|update|delete} {*}", true);
    testMatch("update t1 set a=1 where id=1", "{select|insert|update|delete} {*}", true);
    testMatch("delete from t1 where id=1", "{select|insert|update|delete} {*}", true);
    testMatch("create table t1 (id int)", "{select|insert|update|delete} {*}", false);
  });

  test('empty query', () {
    testMatch("", "select", false);
    testMatch("", "{*}", true);
    testMatch("", "{1}", false);
  });

  test('empty pattern', () {
    testMatch("select * from t1", "", true);
    testMatch("", "", true);
  });

  test('{*} target not found - EOF', () {
    testMatch("select a b c", "select {*} from", false);
    testMatch("select", "{*} from", false);
  });

  test('{*} matches zero tokens before target', () {
    testMatch("select from t1", "select {*} from t1", true);
  });

  test('unclosed parenthesis', () {
    // 未闭合括号遇到 EOF，block 截止到末尾
    testMatch("select * from (subquery", "select * from {1}", true);
    testMatch("select * from (a (b", "select * from {1}", true);
  });

  test('block does not match literal', () {
    // (a) 是一个 MatcherBlock，不会被 EqualMatcher "a" 匹配
    testMatch("select (a) from t1", "select a from t1", false);
  });

  test('block does not match or', () {
    testMatch("select (a) from t1", "select {a|b} from t1", false);
  });

  test('query with quoted strings', () {
    testMatch("select 'hello' from t1", "select {*} from t1", true);
    testMatch("select 'hello' from t1", "select {1} from t1", true);
  });

  test('query with numbers', () {
    testMatch("select 123 from t1", "select {1} from t1", true);
    testMatch("select 1, 2, 3 from t1", "select {*} from t1", true);
  });

  test('query with comments', () {
    testMatch("select /* comment */ * from t1", "select * from t1", true);
    testMatch("select -- line comment\n* from t1", "select * from t1", true);
    // 行注释到输入末尾（无换行）：`--` 后整段仍为注释，不应把其中的 token 当 SQL。
    testMatch("select -- line to eof", "select", true);
  });

  test('mysql dangerous sql: delete with where only inside line comment', () {
    expect(parser(DialectType.mysql, 'delete from t1 -- where id =1;').isDangerousSQL, isTrue);
    expect(parser(DialectType.mysql, 'delete from t1 where id =1').isDangerousSQL, isFalse);
  });

  test('{0} skips zero tokens', () {
    testMatch("select * from t1", "select {0} * from t1", true);
  });

  test('pattern longer than query', () {
    testMatch("select", "select * from t1", false);
    testMatch("select *", "select * from t1", false);
  });

  test('invalid pattern - {*} followed by {*}', () {
    expect(
      () => match(DialectType.mysql, "select", "{*} {*}"),
      throwsArgumentError,
    );
  });

  test('invalid pattern - {*} followed by {N}', () {
    expect(
      () => match(DialectType.mysql, "select", "{*} {3}"),
      throwsArgumentError,
    );
  });

  test('invalid pattern - {abc} bad number', () {
    expect(
      () => match(DialectType.mysql, "select", "{abc}"),
      throwsFormatException,
    );
  });
}
