
import 'package:sql_parser/src/lexer/lexer.dart';
import 'package:sql_parser/src/lexer/token_builder.dart';
import 'keyword.dart';

class MssqlLexer extends Lexer {
  static final TokenBuilder _builder = TokenRooter(<TokenBuilder>[
    EOFTokenBuilder(),
    SpaceTokenBuilder(),
    BracketValueTokenBuilder(),
    KeyWordTokenBuilder(keywords),
    SingleQValueTokenBuilder(),
    DoubleQValueTokenBuilder(),
    NumberTokenBuilder(),
    CommentBuilder(),
    PunctuationTokenBuilder(),
  ]);

  MssqlLexer(String content) : super(_builder, content);
}
