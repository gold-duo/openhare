import 'package:client/models/ai.dart';
import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/services/ai/agent.dart';
import 'package:client/services/ai/chat.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/services/sessions/session_metadata.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'session_chat.g.dart';

@Riverpod(keepAlive: true)
class SessionAIChatNotifier extends _$SessionAIChatNotifier {
  @override
  SessionAIChatModel build() {
    SessionDetailModel? session = ref.watch(selectedSessionDetailProvider);
    if (session == null) {
      throw Exception("Session not found");
    }
    LLMAgentsModel llmAgents = ref.watch(lLMAgentProvider);

    ref.watch(aIChatServiceProvider);

    AIChatModel? aiChatModel = ref
        .read(aIChatServiceProvider.notifier)
        .getAIChatById(
          AIChatId(value: session.sessionId.value),
        );

    if (aiChatModel == null) {
      aiChatModel = AIChatModel(
        id: AIChatId(value: session.sessionId.value), // todo: 暂时用session id 替代chatId
        messages: [],
        state: AIChatState.idle,
      );

      ref.read(aIChatServiceProvider.notifier).create(aiChatModel);
    }

    AsyncValue<InstanceMetadataModel>? metadata; //todo: 如何优雅处理嵌套异步
    if (session.instanceId != null) {
      metadata = ref.watch(selectedSessionMetadataProvider);
    }

    return SessionAIChatModel(
      chatModel: aiChatModel,
      sessionId: session.sessionId,
      currentSchema: session.currentSchema,
      dbType: session.dbType,
      metadata: metadata?.value,
      connId: session.connId,
      state: session.connState,
      llmAgents: llmAgents,
    );
  }
}
