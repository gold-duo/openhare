import 'package:sql_parser/parser.dart';
import 'package:test/test.dart';

void main() {
  test('test lexer scan', () {
    var l = createLexer(
        DialectType.mysql,
        "aaa_123 \"abc\"   \"a\\\"bc\" 'abc' select a\$aa `\"abc` 123 123.1abc.1 ;");
    Token tok = l.scan();
    expect(tok.id, TokenType.ident);
    expect(tok.content, "aaa_123");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.doubleQValue);
    expect(tok.content, "\"abc\"");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, "   ");

    tok = l.scan();
    expect(tok.id, TokenType.doubleQValue);
    expect(tok.content, "\"a\\\"bc\"");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.singleQValue);
    expect(tok.content, "'abc'");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.keyword);
    expect(tok.content, "select");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.ident);
    expect(tok.content, "a\$aa");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.backQValue);
    expect(tok.content, "`\"abc`");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.number);
    expect(tok.content, "123");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.number);
    expect(tok.content, "123.1");

    tok = l.scan();
    expect(tok.id, TokenType.ident);
    expect(tok.content, "abc");

    tok = l.scan();
    expect(tok.id, TokenType.number);
    expect(tok.content, ".1");

    tok = l.scan();
    expect(tok.id, TokenType.whitespace);
    expect(tok.content, " ");

    tok = l.scan();
    expect(tok.id, TokenType.punctuation);
    expect(tok.content, ";");

    tok = l.scan();
    expect(tok.id, TokenType.eof);
    expect(tok.content, "");
  });
  test('test lexer scanWhere', () {
    var l = createLexer(
        DialectType.mysql,
        "aaa_123 \"abc\"   \"a\\\"bc\" 'abc' select a\$aa `\"abc` 123 123.1abc.1 ;");

    Token? token = l.scanWhere(
        (token) => (token.id == TokenType.number && token.content == "123.1"));
    expect(token!.id, TokenType.number);

    token = l.scanWhere(
        (token) => (token.id == TokenType.punctuation && token.content == ";"));
    expect(token!.id, TokenType.punctuation);

    token = l.scanWhere(
        (token) => (token.id == TokenType.ident && token.content == "no"));
    expect(token, null);
  });

  test("test lexer first", () {
    var l = createLexer(DialectType.mysql, "/* test */    select;");
    Token? token = l.first((token) =>
        (token.id == TokenType.whitespace || token.id == TokenType.comment));
    expect(token!.id, TokenType.keyword);
    expect(token.content, "select");

    l = createLexer(DialectType.mysql, "-- abc\n select;");
    token = l.first((token) =>
        (token.id == TokenType.whitespace || token.id == TokenType.comment));
    expect(token!.id, TokenType.keyword);
    expect(token.content, "select");

    l = createLexer(DialectType.mysql, "# abc\r\n select;");
    token = l.first((token) =>
        (token.id == TokenType.whitespace || token.id == TokenType.comment));
    expect(token!.id, TokenType.keyword);
    expect(token.content, "select");

    l = createLexer(DialectType.mysql, "-- cba\r\n# cba\r select;");
    token = l.firstTrim();
    expect(token!.id, TokenType.keyword);
    expect(token.content, "select");
  });

  test("test lexer trimEndWhere", () {
    // 场景1: 仅裁剪尾部空白
    var l = createLexer(DialectType.mysql, "select   ");
    var content = l.trimEndWhere((token) => token.id == TokenType.whitespace);
    expect(content, "select");

    // 场景2: 同时裁剪尾部空白和注释
    l = createLexer(DialectType.mysql, "select   /* tail */");
    content = l.trimEndWhere((token) =>
        token.id == TokenType.whitespace || token.id == TokenType.comment);
    expect(content, "select");

    // 场景3: 尾部不命中回调时不裁剪
    l = createLexer(DialectType.mysql, "select;");
    content = l.trimEndWhere((token) => token.id == TokenType.whitespace);
    expect(content, "select;");

    // 场景4: 全部命中时裁剪为空字符串
    l = createLexer(DialectType.mysql, "  ");
    content = l.trimEndWhere((token) => token.id == TokenType.whitespace);
    expect(content, isEmpty);

    // 场景5: 保留中间原始格式，仅裁剪尾部
    l = createLexer(DialectType.mysql, "select  \n\t`a`  from\n tbl   /* tail */   ");
    content = l.trimEndWhere((token) =>
        token.id == TokenType.whitespace || token.id == TokenType.comment);
    expect(content, "select  \n\t`a`  from\n tbl");

    // 场景6: 按内容自定义裁剪（连续分号）
    l = createLexer(DialectType.mysql, "select;;;");
    content = l.trimEndWhere((token) =>
        token.id == TokenType.punctuation && token.content == ";");
    expect(content, "select");

    // 场景7: 从尾部向前，遇到第一个不匹配即停止
    l = createLexer(DialectType.mysql, "select /* tail-comment */ ;   ");
    content = l.trimEndWhere((token) =>
        token.id == TokenType.whitespace || token.id == TokenType.comment);
    expect(content, "select /* tail-comment */ ;");

    // 场景8: 尾部注释与换行一起裁剪
    l = createLexer(DialectType.mysql, "select\r\n/* tail */");
    content = l.trimEndWhere((token) =>
        token.id == TokenType.whitespace || token.id == TokenType.comment);
    expect(content, "select");

    // 场景9: 空输入
    l = createLexer(DialectType.mysql, "");
    content = l.trimEndWhere((token) => true);
    expect(content, isEmpty);
  });
}
