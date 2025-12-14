import 'package:client/models/ai.dart';
import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/repositories/instances/instances.dart';
import 'package:client/services/ai/agent.dart';
import 'package:client/services/ai/chat.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'session_chat.g.dart';

@Riverpod(keepAlive: true)
class SessionAIChatNotifier extends _$SessionAIChatNotifier {
  @override
  Future<SessionAIChatModel> build() async {
    SessionDetailModel? session = ref.watch(selectedSessionDetailProvider);
    if (session == null) {
      throw Exception("Session not found");
    }
    LLMAgentsModel llmAgents = ref.watch(lLMAgentServiceProvider);

    AIChatModel? aiChatModel = ref.watch(aIChatServiceProvider.select((m) {
      return m.chats[AIChatId(value: session.sessionId.value)];
    }));

    if (aiChatModel == null) {
      aiChatModel = AIChatModel(
        id: AIChatId(
            value: session.sessionId.value), // todo: 暂时用session id 替代chatId
        messages: [],
        state: AIChatState.idle,
        tables: {},
      );

      ref.read(aIChatServiceProvider.notifier).create(aiChatModel);
    }

    InstanceMetadataModel? metadata; //todo: 如何优雅处理嵌套异步
    if (session.instanceId != null) {
      metadata = await ref
          .watch(instanceRepoProvider)
          .getMetadata(session.instanceId!);
    }

    return SessionAIChatModel(
      chatModel: aiChatModel,
      sessionId: session.sessionId,
      currentSchema: session.currentSchema,
      dbType: session.dbType,
      metadata: metadata?.metadata,
      connId: session.connId,
      state: session.connState,
      llmAgents: llmAgents,
    );
  }
}
