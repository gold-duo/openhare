import 'package:sql_parser/src/lexer/lexer.dart';
import 'package:sql_parser/src/lexer/token_builder.dart';
import 'package:sql_parser/src/lexer/character.dart';
import 'package:sql_parser/src/lexer/token.dart';
import 'keyword.dart';

// 比标准的多一种情况, `#`也算注释.
class _MySQLCommentBuilder implements TokenBuilder {
  _MySQLCommentBuilder();

  (bool, TokenType?) matchNewLine(LexerContext ctx) {
    while (ctx.scanner.hasNext() && ctx.scanner.next()) {
      // \n
      if (ctx.scanner.curChar() == Char.$n) {
        return (true, TokenType.comment);
      }
      // \r 或者 \r\n
      if (ctx.scanner.curChar() == Char.$r) {
        // \r\n
        if (ctx.scanner.hasNext() && ctx.scanner.nextChar() == Char.$n) {
          ctx.scanner.next();
        }
        return (true, TokenType.comment);
      }
    }
    return (false, null);
  }

  (bool, TokenType?) matchRightComment(LexerContext ctx) {
    while (ctx.scanner.hasNext() && ctx.scanner.next()) {
      // */
      if (ctx.scanner.curChar() == Char.star &&
          ctx.scanner.hasNext() &&
          ctx.scanner.nextChar() == Char.slash) {
        ctx.scanner.next();
        return (true, TokenType.comment);
      }
    }
    return (false, null);
  }

  @override
  (bool, TokenType?) matchToken(LexerContext ctx) {
    // find newline
    if (ctx.scanner.startWith("-- ") || ctx.scanner.startWith("# ")) {
      ctx.scanner.nextN(3);
      return matchNewLine(ctx);
    }
    // find */
    if (ctx.scanner.startWith("/*")) {
      ctx.scanner.nextN(2);
      return matchRightComment(ctx);
    }
    return (false, null);
  }
}


class MySQLLexer extends Lexer {
  static final TokenBuilder _builder = TokenRooter(<TokenBuilder>[
    EOFTokenBuilder(),
    SpaceTokenBuilder(),
    KeyWordTokenBuilder(keywords),
    SingleQValueTokenBuilder(),
    DoubleQValueTokenBuilder(),
    BackQValueTokenBuilder(),
    NumberTokenBuilder(),
    _MySQLCommentBuilder(),
    PunctuationTokenBuilder(),
  ]);

  MySQLLexer(String content) : super(MySQLLexer._builder, content);
}
