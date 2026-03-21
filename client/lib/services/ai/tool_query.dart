import 'package:client/models/ai.dart';
import 'package:client/models/sessions.dart';
import 'package:client/repositories/ai/chat.dart';
import 'package:client/services/ai/llm_sdk.dart';
import 'package:client/services/ai/tool.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/services/sessions/session_conn.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:sql_parser/parser.dart';

/// LLM `execute_query`：策略判断、待确认、执行与续跑。
final class SqlExecuteQueryToolExecutor implements AIChatToolExecutor {
  SqlExecuteQueryToolExecutor(this.model);

  SqlExecuteQueryToolExecutor.fromToolCall(AIChatMessageToolCall toolCall)
    : model = AIChatMessageToolCallsModel(
        id: AIChatMessageId.generate(),
        toolCall: AIChatMessageToolCallQueryModel(
          query: toolCall.arguments['query'] as String,
          execState: AIChatToolQueryState.awaitingUserConfirm,
        ),
      );

  static const String toolName = 'execute_query';

  AIChatMessageToolCallsModel model;

  @override
  String get name => toolName;

  @override
  void initMessage(Ref ref, AIChatId chatId) {
    ref.read(aiChatRepoProvider).addMessage(chatId, AIChatMessageItem.toolsResult(model));
  }

  @override
  bool checkNeedsAwaitUserConfirm(Ref ref, AIChatId chatId) {
    if (model.toolCall.query.isEmpty) {
      return false;
    }
    final sessionId = SessionId(value: chatId.value);
    final session = ref.read(sessionsServicesProvider.notifier).getSession(sessionId);
    DialectType dialect = DialectType.mysql;
    if (session?.instanceId != null) {
      final inst = ref.read(instancesServicesProvider.notifier).getInstanceById(session!.instanceId!);
      dialect = inst?.dbType.dialectType ?? DialectType.mysql;
    }

    final trimmed = model.toolCall.query.trim();
    if (trimmed.isEmpty) return true;
    try {
      final chunks = splitSQL(dialect, trimmed, skipWhitespace: true, skipComment: true);
      final parts = chunks.map((c) => c.content.trim()).where((c) => c.isNotEmpty).toList();
      final statements = parts.isEmpty ? <String>[trimmed] : parts;
      for (final s in statements) {
        final sd = parser(dialect, s);
        if (sd.sqlType != SQLType.dql || sd.isDangerousSQL) return true;
      }
      return false;
    } catch (_) {
      return true;
    }
  }

  @override
  void setStatus(Ref ref, AIChatId chatId, AIChatToolQueryState status, {required void Function() onInvalidate}) {
    // 已经是最终状态了,不再更新(理论上不会进这个分支)
    if (model.toolCall.isFinished || model.toolCall.isFailed) {
      return;
    }
    model = model.copyWith(
      toolCall: model.toolCall.copyWith(execState: status),
    );
    ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(model));
    onInvalidate();
  }

  @override
  Future<void> run(Ref ref, AIChatId chatId, {required void Function() onInvalidate}) async {
    if (!model.toolCall.isApproved) {
      return;
    }

    final query = model.toolCall.query;
    // 更新状态为running
    model = model.copyWith(
      toolCall: model.toolCall.copyWith(execState: AIChatToolQueryState.running),
    );
    ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(model));
    onInvalidate();

    // 执行查询
    final sessionId = SessionId(value: chatId.value);
    try {
      final sessionModel = ref.read(sessionsServicesProvider.notifier).getSession(sessionId);
      if (sessionModel == null || sessionModel.connId == null) {
        throw Exception('会话未连接');
      }
      final startTime = DateTime.now();
      final connServices = ref.read(sessionConnsServicesProvider.notifier);
      final queryResult = await connServices.query(sessionModel.connId!, query);
      final executeTime = DateTime.now().difference(startTime);

      model = model.copyWith(
        toolCall: queryResult == null
            ? AIChatMessageToolCallQueryModel(
                query: query,
                errorMessage: '查询返回空结果', // todo: 对空结果的处理
                executeTime: executeTime,
                execState: AIChatToolQueryState.failed,
              )
            : AIChatMessageToolCallQueryModel(
                query: query,
                queryResult: queryResult,
                executeTime: executeTime,
                execState: AIChatToolQueryState.finished,
              ),
      );
      ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(model));
      onInvalidate();
    } catch (e) {
      model = model.copyWith(
        toolCall: AIChatMessageToolCallQueryModel(
          query: query,
          errorMessage: e.toString(),
          execState: AIChatToolQueryState.failed,
        ),
      );
      ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(model));
      onInvalidate();
    }
  }
}
