import 'package:client/models/ai.dart';
import 'package:client/models/sessions.dart';
import 'package:client/repositories/ai/agent.dart';
import 'package:client/repositories/ai/chat.dart';
import 'package:client/services/ai/llm_sdk.dart';
import 'package:client/services/ai/tool.dart' as tool_lib;
import 'package:client/services/sessions/session_conn.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/foundation.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'package:client/utils/state_value.dart';

part 'chat.g.dart';

@Riverpod(keepAlive: true)
class AIChatService extends _$AIChatService {
  @override
  int build() {
    return 0;
  }

  void _invalidateSelf() {
    state++;
  }

  AIChatModel? getAIChatById(AIChatId id) {
    return ref.watch(aiChatRepoProvider).getAIChatById(id);
  }

  void create(AIChatModel model) {
    ref.read(aiChatRepoProvider).create(model);
  }

  List<AIChatMessageItem> _getChatMessage(AIChatId id) {
    return ref.read(aiChatRepoProvider).getAIChatById(id)!.messages;
  }

  void _updateState(AIChatId id, AIChatState state) {
    ref.read(aiChatRepoProvider).updateState(id, state);
    _invalidateSelf();
  }

  Future<void> _executeQueryTool(
    AIChatId id,
    String query,
    SessionId sessionId,
  ) async {
    if (query.isEmpty) {
      throw Exception('query 参数不能为空');
    }
    final model = AIChatMessageToolCallsModel(
      id: AIChatMessageId.generate(),
      toolCall: AIChatMessageToolCallQueryModel(
        query: query,
        result: StateValue<BaseQueryResult>.running(),
      ),
    );

    // 先记录toolcall的执行状态
    ref.read(aiChatRepoProvider).addMessage(id, AIChatMessageItem.toolsResult(model));
    _invalidateSelf();

    try {
      // 获取 session 和 connId
      final sessionModel = ref.read(sessionsServicesProvider.notifier).getSession(sessionId);
      if (sessionModel == null || sessionModel.connId == null) {
        throw Exception('会话未连接');
      }
      // todo: 统一时间计算
      // 记录开始时间
      final startTime = DateTime.now();

      // 直接调用 conn 的 query 方法
      final connServices = ref.read(sessionConnsServicesProvider.notifier);
      final queryResult = await connServices.query(sessionModel.connId!, query);

      // 计算执行时间
      final executeTime = DateTime.now().difference(startTime);

      final completedModel = model.copyWith(
        toolCall: AIChatMessageToolCallQueryModel(
          query: query,
          result: queryResult != null
              ? StateValue<BaseQueryResult>.done(queryResult)
              : StateValue<BaseQueryResult>.error('查询返回空结果'), // todo: 不应该返回空结果，应该返回错误信息。对空的处理不太合理。
          executeTime: executeTime,
        ),
      );
      ref.read(aiChatRepoProvider).updateMessageById(id, model.id, AIChatMessageItem.toolsResult(completedModel));
      _invalidateSelf();
    } catch (e) {
      final errorModel = model.copyWith(
        toolCall: AIChatMessageToolCallQueryModel(
          query: query,
          result: StateValue<BaseQueryResult>.error(e.toString()),
        ),
      );
      ref.read(aiChatRepoProvider).updateMessageById(id, model.id, AIChatMessageItem.toolsResult(errorModel));
      _invalidateSelf();
    }
  }

  /// 执行工具调用，返回 AIChatMessageToolCallQueryModel 列表
  Future<void> _executeToolCalls(
    List<AIChatMessageToolCall> toolCalls,
    AIChatId chatId,
  ) async {
    final sessionId = SessionId(value: chatId.value);

    for (final toolCall in toolCalls) {
      try {
        // 获取工具参数
        final arguments = toolCall.arguments;
        // 根据工具名称执行对应的工具
        if (toolCall.name == 'execute_query') {
          final queryValue = arguments['query'];
          final query = queryValue is String ? queryValue : (queryValue?.toString() ?? '');

          await _executeQueryTool(chatId, query, sessionId);
        } else {
          debugPrint('❌ [Chat] 未知的工具: ${toolCall.name}');
        }
      } catch (e) {
        debugPrint('❌ [Chat] 工具执行失败: ${toolCall.name}');
        debugPrint('    - 错误: $e');
      }
    }

    return;
  }

  /// 进行AI对话，请求接口，存储消息并刷新使用 provider 来动态刷新页面
  Future<void> chat(
    AIChatId id,
    LLMAgentId agentId,
    String systemPrompt, {
    String? message,
    String? refText,
  }) async {
    final repo = ref.read(aiChatRepoProvider);
    final model = repo.getAIChatById(id);
    if (model == null) {
      return;
    }

    // 1.如果有则更新用户提问的消息
    if (message != null) {
      repo.addMessage(
        id,
        AIChatMessageItem.userMessage(
          AIChatUserMessageModel(
            id: AIChatMessageId.generate(),
            content: message,
            ref: refText,
          ),
        ),
      );
    }

    _updateState(id, AIChatState.waiting);

    LLMAgentModel? lastUsedLLMAgent = ref.read(lLMAgentRepoProvider).getLastUsedLLMAgent();
    if (lastUsedLLMAgent == null) {
      return;
    }

    final llmSdk = LLMProvider.create(
      lastUsedLLMAgent.setting,
      systemPrompt,
      tools: [tool_lib.QueryTool()],
    );

    // 节流：限制 UI 刷新频率
    DateTime? lastUpdateTime;
    const throttleDuration = Duration(milliseconds: 200);

    while (true) {
      AIChatAssistantMessageModel? lastMessage = AIChatAssistantMessageModel(
        id: AIChatMessageId.generate(),
        content: '',
        status: State.running,
      );
      try {
        final messages = _getChatMessage(id);
        final chatStream = llmSdk.stream(messages);
        List<AIChatMessageToolCall> toolCalls = [];
        await for (final chunk in chatStream) {
          final content = chunk.content;
          toolCalls = chunk.toolCalls ?? [];
          lastMessage = lastMessage!.copyWith(
            content: content,
            thinking: chunk.thinking,
          );
          // 数据始终更新到 repo
          repo.updateMessageById(id, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));

          // 节流 UI 更新
          final now = DateTime.now();
          if (lastUpdateTime == null) {
            lastUpdateTime = now;
            _invalidateSelf();
          } else if (now.difference(lastUpdateTime) >= throttleDuration) {
            lastUpdateTime = now;
            _invalidateSelf();
          }
        }

        if (lastMessage != null) {
          lastMessage = lastMessage.copyWith(
            status: State.done,
          );
          repo.updateMessageById(id, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));
          _invalidateSelf();
        }

        if (toolCalls.isNotEmpty) {
          await _executeToolCalls(toolCalls, id);
          continue;
        }
        break;
      } catch (e) {
        lastMessage = lastMessage!.copyWith(
          error: e.toString(),
          status: State.failing,
        );
        repo.updateMessageById(id, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));
        _invalidateSelf();
        break;
      }
    }
    _updateState(id, AIChatState.idle);
  }

  void retryChat(AIChatId id, LLMAgentId agentId, String systemPrompt, AIChatUserMessageModel retryMessage) {
    // 先把当前及其后面的message 删除, 然后重新chat
    final messages = _getChatMessage(id);
    int? index;
    for (var i = 0; i < messages.length; i++) {
      final item = messages[i];
      final found = item.maybeWhen(
        userMessage: (msg) => msg.content == retryMessage.content,
        orElse: () => false,
      );
      if (found) {
        index = i;
        break;
      }
    }
    if (index == null) {
      return;
    }
    // 更新 message
    ref.read(aiChatRepoProvider).updateMessages(id, messages.sublist(0, index));
    ref.invalidateSelf();
    // 重新 chat
    chat(id, agentId, systemPrompt);
  }

  void cleanMessages(AIChatId id) {
    final List<AIChatMessageItem> messages = [];
    ref.read(aiChatRepoProvider).updateMessages(id, messages);
    ref.invalidateSelf();
  }

  void delete(AIChatId id) {
    ref.read(aiChatRepoProvider).delete(id);
    ref.invalidateSelf();
  }
}
