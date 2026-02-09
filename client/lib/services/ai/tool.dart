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
  String get description =>
      '在当前选中的数据库连接上执行 SQL 查询。输入 SQL 语句，返回查询结果（包括列信息和数据行）。只能执行 SELECT 查询，不能执行 INSERT、UPDATE、DELETE 等修改数据的操作。';

  @override
  Map<String, dynamic> get inputJsonSchema => {
        'type': 'object',
        'properties': {
          'query': {
            'type': 'string',
            'description': '要执行的 SQL 查询语句，例如：SELECT * FROM users LIMIT 10',
          },
        },
        'required': ['query'],
      };

}
