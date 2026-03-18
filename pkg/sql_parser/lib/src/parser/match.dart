import '../lexer/lexer.dart';
import '../lexer/token.dart';

sealed class MatcherEntry {}

class MatcherToken extends MatcherEntry {
  final Token token;
  MatcherToken(this.token);
}

class MatcherBlock extends MatcherEntry {
  final List<MatcherEntry> entries;
  MatcherBlock(this.entries);
}

class MacherScanner {
  final Lexer _lexer;
  bool _isEof = false;

  MacherScanner(this._lexer);

  bool get isEof => _isEof;

  Token? _nextToken() {
    while (true) {
      final token = _lexer.scan();
      if (token.id == TokenType.eof) {
        _isEof = true;
        return null;
      }
      // 跳过空白字符、注释和无效 token.
      if (token.id == TokenType.whitespace || token.id == TokenType.comment) {
        continue;
      }
      return token;
    }
  }

  MatcherEntry? next() {
    final token = _nextToken();
    if (token == null) return null;
    if (token.content == '(') {
      return _consumeBlock();
    }
    return MatcherToken(token);
  }

  /// 遇到 `(` 后，递归收集内部 token 直到配对的 `)`，
  /// 嵌套的 `(...)` 会形成子 MatcherBlock。
  MatcherBlock _consumeBlock() {
    final entries = <MatcherEntry>[];
    while (true) {
      final token = _nextToken();
      if (token == null) break;
      if (token.content == ')') break;
      if (token.content == '(') {
        entries.add(_consumeBlock());
      } else {
        entries.add(MatcherToken(token));
      }
    }
    return MatcherBlock(entries);
  }
}

abstract class _Matcher {
  bool match(MacherScanner iter);
}

class _EqualMatcher implements _Matcher {
  final String value;
  _EqualMatcher(this.value);

  @override
  bool match(MacherScanner iter) {
    final entry = iter.next();
    if (entry is! MatcherToken) return false;
    return entry.token.content.toLowerCase() == value.toLowerCase();
  }
}

class _SkipMatcher implements _Matcher {
  final int times;
  _SkipMatcher(this.times);

  @override
  bool match(MacherScanner iter) {
    for (var i = 0; i < times; i++) {
      if (iter.next() == null) return false;
    }
    return true;
  }
}

class _OrMatcher implements _Matcher {
  final Set<String> _values;
  _OrMatcher(List<String> values) : _values = values.map((v) => v.toLowerCase()).toSet();

  @override
  bool match(MacherScanner iter) {
    final entry = iter.next();
    if (entry is! MatcherToken) return false;
    return _values.contains(entry.token.content.toLowerCase());
  }
}

class _FindMatcher implements _Matcher {
  _Matcher? target;
  _FindMatcher(this.target);

  @override
  bool match(MacherScanner iter) {
    if (target == null) return true;
    while (true) {
      if (target!.match(iter)) return true;
      if (iter.isEof) return false;
    }
  }
}

_Matcher _parseMatcher(String field) {
  if (field.startsWith('{') && field.endsWith('}')) {
    final body = field.substring(1, field.length - 1);
    if (body == '*') {
      return _FindMatcher(null);
    }
    if (body.contains('|')) {
      return _OrMatcher(body.split('|'));
    }
    return _SkipMatcher(int.parse(body));
  }
  return _EqualMatcher(field);
}

/// Match a token stream from [lexer] against a [pattern].
///
/// Pattern syntax (space-separated):
/// - Literal tokens match case-insensitively (e.g. `SELECT`, `from`, `*`)
/// - `{N}` skips exactly N tokens
/// - `{*}` searches forward until the next literal token is found,
///   or matches unconditionally if it's the last element
/// - `{a|b|c}` matches any one of the listed tokens (case-insensitive)
///
/// Parenthesized blocks `(...)` are collapsed into a single token,
/// so matchers only operate on the outer-level structure.

class Matcher {
  final Lexer _lexer;
  Matcher(this._lexer);

  bool match(String pattern) {
    final trimmed = pattern.trim();
    if (trimmed.isEmpty) return true;

    final fields = trimmed.split(RegExp(r'\s+'));
    final matchers = <_Matcher>[];

    for (var i = 0; i < fields.length; i++) {
      final matcher = _parseMatcher(fields[i]);
      if (matcher is _FindMatcher && i < fields.length - 1) {
        i++;
        final next = _parseMatcher(fields[i]);
        if (next is _SkipMatcher || next is _FindMatcher) {
          throw ArgumentError('{*} must be followed by a literal token or {a|b} pattern, got: ${fields[i]}');
        }
        matcher.target = next;
      }
      matchers.add(matcher);
    }

    final iter = MacherScanner(_lexer);
    for (final matcher in matchers) {
      if (!matcher.match(iter)) return false;
    }
    return true;
  }
}
