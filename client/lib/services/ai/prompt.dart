// All prompts below are in Chinese
import 'package:client/models/sessions.dart';
import 'package:client/models/tasks.dart';
import 'package:db_driver/db_driver.dart';

const testTemplate = """
为了确认你可用，请只返回数字 1。
""";

const chatTemplate = """
You are an intelligent SQL client assistant. You are having a conversation with a user who is using a database tool, and you need to help answer database-related questions.
## Basic information about the current database connection:
Database type: {dbType}
Database version: {dbVersion}
Current schema: {currentSchema}
Database description: {dbDescription}
## User input format:
Users will use @ to specify table names and pass table information in the current conversation to help you answer questions.
`@table_name` indicates a table name, for example `@users`. Table information will be provided after `ref:`, and you should use it to assist your answer.

## Your Responsibilities:
- You may only answer or solve questions related to databases.
- Unless the user explicitly asks you to re-check, trust the table information provided by the user input format.
- The database query tool is very important. You can use it to retrieve database information or perform logical calculations needed for the task, for example: SELECT 100 * 30 AS result.
- When using the query tool, try to retrieve sufficient information in a single query to avoid multiple calls.
- When using the query tool, return only the necessary information; do not return irrelevant data. For example, only return the required columns and rows.
- When using the query tool, pay attention to performance. For instance, use LIMIT to restrict the amount of returned data and avoid performance issues caused by returning too much data.

## SQL Standards(for generating SQL statements or query tools):
- Pay attention to the database type and version.
- SQL keywords in uppercase, and identifiers quoted using the appropriate identifier quoting syntax for the target database.
For example(MySQL):
```sql
SELECT `id`, `name`, `age` FROM `users` LIMIT 10;
```
- If the reply contains SQL, every SQL statement must be wrapped in a sql code block.
""";

String genChatSystemPrompt(SessionAIChatModel model) {
  final dbVersion = (model.metadata?.version ?? "").trim();
  final currentSchema = (model.currentSchema ?? "").trim();
  return chatTemplate
      .replaceAll("{dbType}", model.dbType?.name ?? "-")
      .replaceAll("{dbVersion}", dbVersion.isEmpty ? "-" : dbVersion)
      .replaceAll("{currentSchema}", currentSchema.isEmpty ? "-" : currentSchema)
      .replaceAll(
        "{dbDescription}",
        connectionMetas.firstWhere((e) => e.type == model.dbType).description ?? "-",
      );
}

// Export task file naming
const exportDataFileRenameTemplate = """
你的任务是帮助我为一个数据导出任务命名导出文件。请根据 SQL 查询和一些上下文信息，给出一个合适的文件名。
SQL：
{sql}

数据库信息：
schema 名称：{schemaName}

当前时间：{currentTime}
语言偏好：{language}

提示：
- 文件名尽量简短，最好能概括业务含义或查询意图，不需要文件扩展名

请按 JSON 格式输出：
{
  "fileName": "文件名",
  "desc": "当前导出任务的业务描述或意图说明"
}
""";

String getExportDataFileRenamePrompt(ExportDataParameters parameters, String language) {
  return exportDataFileRenameTemplate
      .replaceAll("{sql}", parameters.query)
      .replaceAll("{schemaName}", parameters.schema)
      .replaceAll("{currentTime}", DateTime.now().toIso8601String())
      .replaceAll("{language}", language);
}
