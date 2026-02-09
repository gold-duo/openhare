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

  void _updateProgress(AIChatId id, AIChatProgressModel progress) {
    ref.read(aiChatRepoProvider).updateProgress(id, progress);
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

  /// 检查当前 chat 是否处于 cancel 状态
  bool _isCancelled(AIChatId id) {
    return ref.read(aiChatRepoProvider).isCancel(id);
  }

  /// 通过最后一条消息判断是否正在执行工具（query）
  bool _isExecutingTool(AIChatId id) {
    final model = ref.read(aiChatRepoProvider).getAIChatById(id);
    final messages = model?.messages ?? [];
    if (messages.isEmpty) return false;
    final last = messages.last;
    return last.maybeWhen(
      toolsResult: (toolsResult) => toolsResult.toolCall.result?.state == State.running,
      orElse: () => false,
    );
  }

  /// 用户主动取消当前 chat
  void cancelChat(AIChatId id) {
    _updateState(id, AIChatState.cancel);

    // 仅当正在执行工具（通过最后一条消息判断，目前只有 query）时，kill 掉正在执行的 query
    if (!_isExecutingTool(id)) {
      return;
    }
    final sessionId = SessionId(value: id.value);
    final sessionModel = ref.read(sessionsServicesProvider.notifier).getSession(sessionId);
    if (sessionModel != null && sessionModel.connId != null) {
      ref.read(sessionConnsServicesProvider.notifier).killQuery(sessionModel.connId!);
    }
  }

  Future<void> _executeQueryTool(AIChatId chatId, String query) async {
    if (query.isEmpty) {
      throw Exception('query 参数不能为空');
    }
    final sessionId = SessionId(value: chatId.value);
    final model = AIChatMessageToolCallsModel(
      id: AIChatMessageId.generate(),
      toolCall: AIChatMessageToolCallQueryModel(
        query: query,
        result: StateValue<BaseQueryResult>.running(),
      ),
    );

    // 先记录toolcall的执行状态
    ref.read(aiChatRepoProvider).addMessage(chatId, AIChatMessageItem.toolsResult(model));
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
      ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(completedModel));
      _invalidateSelf();
    } catch (e) {
      final errorModel = model.copyWith(
        toolCall: AIChatMessageToolCallQueryModel(
          query: query,
          result: StateValue<BaseQueryResult>.error(e.toString()),
        ),
      );
      ref.read(aiChatRepoProvider).updateMessageById(chatId, model.id, AIChatMessageItem.toolsResult(errorModel));
      _invalidateSelf();
    }
  }

  /// 进行AI对话，请求接口，存储消息并刷新使用 provider 来动态刷新页面
  Future<void> chat(AIChatId chatId, LLMAgentId agentId, String systemPrompt,
      {String? message, String? refText}) async {
    // 获取当前选中的 LLM Agent
    LLMAgentModel? lastUsedLLMAgent = ref.read(lLMAgentRepoProvider).getLastUsedLLMAgent();
    if (lastUsedLLMAgent == null) {
      return;
    }

    final repo = ref.read(aiChatRepoProvider);
    final model = repo.getAIChatById(chatId);
    if (model == null) {
      return;
    }

    // 0. 预算状态：上下文硬停后无法继续
    if (model.progress.contextHardStopped) {
      return;
    }
    // 每次主动对话设置50轮数，自动 loop 一次最大50.
    _updateProgress(chatId, model.progress.copyWith(loopLimit: 50, loopUsed: 0));

    // 1.如果有则更新用户提问的消息
    if (message != null) {
      final messageModel = AIChatUserMessageModel(id: AIChatMessageId.generate(), content: message, ref: refText);
      repo.addMessage(chatId, AIChatMessageItem.userMessage(messageModel));
    }

    _updateState(chatId, AIChatState.waiting);

    final llmSdk = LLMProvider.create(lastUsedLLMAgent.setting, systemPrompt, tools: [tool_lib.QueryTool()]);

    // 节流：限制 UI 刷新频率（跨 turn 持续生效）
    DateTime? lastUpdateTime;
    const throttleDuration = Duration(milliseconds: 200);

    while (true) {
      final model = repo.getAIChatById(chatId);
      if (model == null) {
        break; // 几乎不会发生
      }
      // 每次 loop 开始时检查是否被取消
      if (_isCancelled(chatId)) {
        break;
      }
      // loop 上限后无法继续
      if (model.progress.loopStopped) {
        break;
      }
      // 上下文硬停后无法继续
      if (model.progress.contextHardStopped) {
        break;
      }

      AIChatAssistantMessageModel lastMessage = AIChatAssistantMessageModel(
        id: AIChatMessageId.generate(),
        content: '',
        status: State.running,
      );

      ChatUsage? usage;

      List<AIChatMessageToolCall> toolCalls = [];
      try {
        final chatStream = llmSdk.stream(model.messages);

        DateTime? lastUpdate = lastUpdateTime;

        await for (final chunk in chatStream) {
          if (_isCancelled(chatId)) {
            break;
          }

          usage = chunk.usage;

          toolCalls = chunk.toolCalls ?? [];
          lastMessage = lastMessage.copyWith(
            content: chunk.content,
            thinking: chunk.thinking,
          );

          // 数据始终更新到 repo
          repo.updateMessageById(chatId, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));

          // 节流 UI 更新
          final now = DateTime.now();
          if (lastUpdate == null) {
            lastUpdate = now;
            _invalidateSelf();
          } else if (now.difference(lastUpdate) >= throttleDuration) {
            lastUpdate = now;
            _invalidateSelf();
          }
        }

        // 更新统计信息
        if (usage != null) {
          _updateProgress(
            chatId,
            model.progress.copyWith(
              totalTokens: usage.totalTokens ?? model.progress.totalTokens,
              loopUsed: model.progress.loopUsed + 1,
            ),
          );
        }
        // 更新对话状态
        lastMessage = lastMessage.copyWith(status: State.done);
        repo.updateMessageById(chatId, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));

        _invalidateSelf();
        lastUpdateTime = lastUpdate;
      } catch (e) {
        lastMessage = lastMessage.copyWith(
          error: e.toString(),
          status: State.failing,
        );
        repo.updateMessageById(chatId, lastMessage.id, AIChatMessageItem.assistantMessage(lastMessage));
        _invalidateSelf();
        break;
      }

      if (toolCalls.isNotEmpty) {
        for (final toolCall in toolCalls) {
          // 每个工具执行前检查是否被取消
          if (_isCancelled(chatId)) {
            break;
          }
          try {
            // 获取工具参数
            final arguments = toolCall.arguments;
            // 根据工具名称执行对应的工具
            if (toolCall.name == 'execute_query') {
              final queryValue = arguments['query'];
              final query = queryValue is String ? queryValue : (queryValue?.toString() ?? '');

              await _executeQueryTool(chatId, query);
            } else {
              debugPrint('❌ [Chat] 未知的工具: ${toolCall.name}');
            }
          } catch (e) {
            debugPrint('❌ [Chat] 工具执行失败: ${toolCall.name}');
            debugPrint('    - 错误: $e');
          }
        }
        continue;
      }
      // 正常完成（无工具调用时默认结束）
      break;
    }
    _updateState(chatId, AIChatState.idle);
  }

  void retryChat(AIChatId id, LLMAgentId agentId, String systemPrompt, AIChatUserMessageModel retryMessage) {
    final messages = _getChatMessage(id);
    final index = messages.indexWhere((item) => item.maybeWhen(
          userMessage: (msg) => msg.id.value == retryMessage.id.value,
          orElse: () => false,
        ));
    if (index == -1) return;
    // 保留当前及之前的 message（含当前这条用户消息），然后重新 chat
    ref.read(aiChatRepoProvider).updateMessages(id, messages.sublist(0, index + 1));
    _invalidateSelf();
    // 重新 chat
    chat(id, agentId, systemPrompt);
  }

  void cleanMessages(AIChatId id) {
    final List<AIChatMessageItem> messages = [];
    ref.read(aiChatRepoProvider).updateMessages(id, messages);
    ref.read(aiChatRepoProvider).updateProgress(id, const AIChatProgressModel());
    _invalidateSelf();
  }

  void delete(AIChatId id) {
    ref.read(aiChatRepoProvider).delete(id);
    _invalidateSelf();
  }
}
