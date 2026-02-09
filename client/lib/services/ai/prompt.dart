// All prompts below are in English
import 'package:client/models/sessions.dart';
import 'package:client/models/tasks.dart';

const testTemplate = """
To confirm that you are available, please only return a number 1 to me.
""";

const chatTemplate = """
You are an intelligent SQL client assistant. You are having a conversation with a user who is using a database tool. You help users answer questions about databases.
## Basic information about the current database connection:
db type: {dbType}
db version: {dbVersion}
current schema: {currentSchema}

## User input format:
Users specify table names with @ and pass table information in the current conversation to help you answer questions.
@table_name indicates a table name, e.g. @users. Table information is passed after `ref:`, you need to use it to assist in answering questions.

## Important notes:
- You can only answer or solve database-related questions;
- If the response contains SQL, each SQL should be wrapped in a ```sql``` block;
- Trust the table information provided by the user unless they explicitly say you need to re-query it;
- The database query tool is very important; you can use it for both fetching database information and task logic calculation, e.g. `SELECT 100 * 30 as result`;
- When using the query tool, try to fetch more needed information in one call and avoid multiple query tool invocations;
- When using the query tool, return only necessary information, not irrelevant data, e.g. only return the required columns and rows;
- When using the query tool, pay attention to performance, e.g. use LIMIT to restrict the amount of returned data and avoid performance issues from excessive data;
""";

String genChatSystemPrompt(SessionAIChatModel model) {
  final dbVersion = (model.metadata?.version ?? "").trim();
  final currentSchema = (model.currentSchema ?? "").trim();
  return chatTemplate
      .replaceAll("{dbType}", model.dbType?.name ?? "-")
      .replaceAll("{dbVersion}", dbVersion.isEmpty ? "-" : dbVersion)
      .replaceAll("{currentSchema}", currentSchema.isEmpty ? "-" : currentSchema);
}

// Export task file naming
const exportDataFileRenameTemplate = """
Your task is to help me name the export file for a data export task. Based on the SQL query and some context, provide a suitable file name.
SQL:
{sql}

Database info:
schema name: {schemaName}

Current time: {currentTime}
Language preference: {language}

Tips:
- Keep the name short; best to summarize the business or query intent, no file extension needed

Output in JSON format:
{
  "fileName": "file name",
  "desc": "Business description or intent description for the current export task"
}
""";

String getExportDataFileRenamePrompt(ExportDataParameters parameters, String language) {
  return exportDataFileRenameTemplate
      .replaceAll("{sql}", parameters.query)
      .replaceAll("{schemaName}", parameters.schema)
      .replaceAll("{currentTime}", DateTime.now().toIso8601String())
      .replaceAll("{language}", language);
}
