abstract class AITool {
  /// 工具名称，用于标识工具
  String get name;

  /// 工具描述，用于 AI 理解工具的功能
  String get description;

  /// 输入参数的 JSON Schema
  Map<String, dynamic> get inputJsonSchema;
}

/// SQL 查询工具定义
///
/// 定义 SQL 查询工具的 schema，实际执行逻辑在 chat.dart 中
class QueryTool extends AITool {
  QueryTool();

  @override
  String get name => 'execute_query';

  @override
  String get description => '''
Execute a SQL query on the currently selected database connection.

Accepts a SQL statement and returns the query result (including column metadata and rows).

Only SELECT queries are allowed; do not use this tool for INSERT, UPDATE, DELETE, or other data-modifying operations.

The result will include at most the first 100 rows.
''';

  @override
  Map<String, dynamic> get inputJsonSchema => {
    'type': 'object',
    'properties': {
      'query': {
        'type': 'string',
        'description': 'The SQL query string to execute, for example: SELECT * FROM users LIMIT 10',
      },
    },
    'required': ['query'],
  };
}
