import 'package:sql_parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('mysql wrap limit select star from table', () {
    final wrapped = parser(DialectType.mysql, "select * from t1;").wrapLimit(limit: 20);
    expect(wrapped, "SELECT * FROM (select * from t1) AS dt_1 LIMIT 20");
  });

  test('mysql wrap limit keeps trailing closing paren on function call', () {
    final wrapped = parser(DialectType.mysql, "select sleep(10)").wrapLimit(limit: 100);
    expect(wrapped, "SELECT * FROM (select sleep(10)) AS dt_1 LIMIT 100");
  });

  test('mysql wrap limit keeps trailing closing paren', () {
    final wrapped = parser(DialectType.mysql, "select * from (select 1)").wrapLimit(limit: 5);
    expect(wrapped, "SELECT * FROM (select * from (select 1)) AS dt_1 LIMIT 5");
  });
}
