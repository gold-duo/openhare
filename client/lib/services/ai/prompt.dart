// 下面的prompt 都用英文
import 'package:client/models/sessions.dart';
import 'package:client/models/tasks.dart';

const testTemplate = """
To confirm that you are available, please only return a number 1 to me.
""";

const chatTemplate = """
你是一个智能SQL客户端助手. 你正在与一个使用数据库工具的用户对话. 你正在帮助用户回答关于数据库的问题.
## 数据库信息:
db type: {dbType}

## 用户输入的格式:
用户会通过@符号指定表名并在当前对话里将表信息传递给你, 给你辅助回答问题.
@table_name 表示表名, 例如: @users.
ref:
table_name信息:
- 表名: users
- 表描述: 用户表
- 表字段:
  - id: 用户ID
  - name: 用户名
  - email: 用户邮箱
  - created_at: 创建时间
  - updated_at: 更新时间

## 注意点: 
- 你只能回答或解决与数据库相关的问题;
- 如果回复包含SQL, 每个SQL应该被包裹在一个 ```sql``` 块中;
- 数据库的query查询是非常重要的工具, 你除了使用它进行数据库信息获取外，还可以用它来进行任务逻辑计算, 例如：`SELECT 100 * 30 as result`;
- 在使用query工具时尽可能一次获取更多想要的信息, 避免多次调用query工具;
- 在使用query工具时要保持返回必要信息, 不要返回无关信息，例如：只返回需要的列和行;
- 在使用query工具时要注意性能问题,例如: 可使用limit等限制返回数据量, 避免返回过多数据导致性能问题;
""";

String genChatSystemPrompt(SessionAIChatModel model) {
  String prompt = chatTemplate;
  if (model.dbType != null) {
    prompt = prompt.replaceAll("{dbType}", model.dbType!.name);
  }
  return prompt;
}

// 导入任务的文件命名
const exportDataFileRenameTemplate = """
你的任务是帮我给数据导出任务的导出文件命名, 你需要根据SQL查询和一些背景信息, 给出一个合适的文件名。
SQL:
{sql}

数据库信息:
schema名称: {schemaName}

当前时间: {currentTime}
语言偏好: {language}

tips: 
- 名称不要太长, 最好概况业务或者查询意图，不需要后缀

输出json格式:
{
  "fileName": "文件名",
  "desc": "当前导出任务对应的业务描述或者意图描述"
}
""";

String getExportDataFileRenamePrompt(ExportDataParameters parameters, String language) {
  return exportDataFileRenameTemplate
      .replaceAll("{sql}", parameters.query)
      .replaceAll("{schemaName}", parameters.schema)
      .replaceAll("{currentTime}", DateTime.now().toIso8601String())
      .replaceAll("{language}", language);
}
