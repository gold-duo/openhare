import 'package:sql_parser/src/lexer/lexer.dart';
import 'package:sql_parser/src/lexer/token_builder.dart';
import 'package:sql_parser/src/lexer/character.dart';
import 'package:sql_parser/src/lexer/token.dart';
import 'keyword.dart';

class _PgDollarQuotedTokenBuilder implements TokenBuilder {
  _PgDollarQuotedTokenBuilder();

  bool _isTagStart(int char) {
    return Char.isLowercaseLatin(char) || Char.isUppercaseLatin(char) || char == Char.$_;
  }

  bool _isTagPart(int char) {
    return _isTagStart(char) || Char.isDigit(char);
  }

  @override
  (bool, TokenType?) matchToken(LexerContext ctx) {
    if (ctx.scanner.curChar() != Char.$$) {
      return (false, null);
    }

    final openStart = ctx.startPos.copy();

    if (!ctx.scanner.hasNext() || !ctx.scanner.next()) {
      return (false, null);
    }

    if (ctx.scanner.curChar() != Char.$$) {
      if (!_isTagStart(ctx.scanner.curChar())) {
        return (false, null);
      }
      while (true) {
        if (ctx.scanner.curChar() == Char.$$) {
          break;
        }
        if (!_isTagPart(ctx.scanner.curChar())) {
          return (false, null);
        }
        if (!ctx.scanner.hasNext() || !ctx.scanner.next()) {
          return (false, null);
        }
      }
    }

    final delimiter = ctx.scanner.subString(openStart, ctx.scanner.pos);
    while (ctx.scanner.hasNext() && ctx.scanner.next()) {
      if (ctx.scanner.startWith(delimiter)) {
        ctx.scanner.nextN(delimiter.length - 1);
        return (true, TokenType.singleQValue);
      }
    }
    return (false, null);
  }
}

class PgLexer extends Lexer {
  static final TokenBuilder _builder = TokenRooter(<TokenBuilder>[
    EOFTokenBuilder(),
    SpaceTokenBuilder(),
    _PgDollarQuotedTokenBuilder(),
    KeyWordTokenBuilder(keywords),
    SingleQValueTokenBuilder(),
    DoubleQValueTokenBuilder(),
    NumberTokenBuilder(),
    CommentBuilder(),
    PunctuationTokenBuilder(),
  ]);

  PgLexer(String content) : super(_builder, content);
}
