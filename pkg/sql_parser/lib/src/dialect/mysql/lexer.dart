import '../../lexer/lexer.dart';
import '../../lexer/token_builder.dart';
import '../../dialect/mysql/keyword.dart';

class MySQLLexer extends Lexer {
  static final TokenBuilder _builder = TokenRooter(<TokenBuilder>[
    EOFTokenBuilder(),
    SpaceTokenBuilder(),
    KeyWordTokenBuilder(keywords),
    SingleQValueTokenBuilder(),
    DoubleQValueTokenBuilder(),
    BackQValueTokenBuilder(),
    NumberTokenBuilder(),
    CommentBuilder(),
    PunctuationTokenBuilder(),
  ]);

  MySQLLexer(String content) : super(MySQLLexer._builder, content);
}
