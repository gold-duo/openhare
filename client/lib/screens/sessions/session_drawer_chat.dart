import 'package:client/models/sessions.dart';
import 'package:client/models/settings.dart';
import 'package:client/screens/sessions/session_operation_bar.dart';
import 'package:client/services/sessions/session_chat.dart';
import 'package:client/services/sessions/session_controller.dart';
import 'package:client/services/sessions/session_sql_result.dart';
import 'package:client/services/settings/settings.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/chat_list_view.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/empty.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:client/models/ai.dart';
import 'package:go_router/go_router.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/screens/sessions/ai_chat/message_tool.dart';
import 'package:client/screens/sessions/ai_chat/message_user.dart';
import 'package:client/screens/sessions/ai_chat/message_ai.dart';
import 'package:client/screens/sessions/ai_chat/input_user.dart';
import 'package:client/services/ai/chat.dart';
import 'package:client/services/ai/prompt.dart';
import 'package:sql_parser/parser.dart';

class SessionDrawerChat extends ConsumerWidget {
  const SessionDrawerChat({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    SessionAIChatModel model = ref.watch(sessionAIChatProvider);
    SessionController sessionController = ref.watch(selectedSessionControllerProvider);
    final chatScrollController = sessionController.chatScrollController;
    return Column(
      children: [
        // 聊天内容
        Expanded(
          child: (model.llmAgents.agents.isEmpty)
              ? const SessionChatGuide()
              : SessionChatMessages(
                  model: model,
                  chatScrollController: chatScrollController,
                ),
        ),
        // 下方的输入框区域
        SessionChatInputCard(
          model: model,
          controller: sessionController.chatInputController,
          modelSearchTextController: sessionController.aiChatModelSearchTextController,
          onSendMessage: chatScrollController.goToBottom,
        ),
        const SizedBox(height: kSpacingMedium),
      ],
    );
  }
}

class SessionChatMessages extends ConsumerStatefulWidget {
  final SessionAIChatModel model;
  final ChatScrollController chatScrollController;
  const SessionChatMessages({
    super.key,
    required this.model,
    required this.chatScrollController,
  });

  @override
  ConsumerState<SessionChatMessages> createState() => _SessionChatMessagesState();
}

class _SessionChatMessagesState extends ConsumerState<SessionChatMessages> {
  int _lastMessageCount = 0;

  @override
  void initState() {
    super.initState();
    _lastMessageCount = widget.model.chatModel.messages.length;
  }

  void _runSQL(BuildContext context, WidgetRef ref, SessionAIChatModel model, String code) {
    final SQLDefiner sd = parser(model.dbType?.dialectType ?? DialectType.mysql, code);
    if (sd.isDangerousSQL && model.config.enableQueryCheck) {
      queryDangerousSQLDialog(
        context,
        ref,
        model.sessionId,
        model.config,
        model.dbType?.dialectType ?? DialectType.mysql,
        code,
      );
    } else {
      ref.read(sQLResultsServicesProvider.notifier).queryAddResult(model.sessionId, code);
    }
  }

  Widget _buildMessage(
    BuildContext context,
    WidgetRef ref,
    SessionAIChatModel model,
    AIChatMessageItem message,
    int index,
  ) {
    return message.when(
      userMessage: (msg) => UserMessage(message: msg, sessionChatModel: model),
      assistantMessage: (msg) => AIMessage(
        message: msg,
        dbType: model.dbType ?? DatabaseType.mysql,
        onRunSQL: SQLConnectState.isIdle(model.state) ? (code) => _runSQL(context, ref, model, code) : null,
      ),
      toolsResult: (msg) => ToolCallWidget(
        chatId: model.chatModel.id,
        toolsMessageId: msg.id,
        dbType: model.dbType ?? DatabaseType.mysql,
        toolCall: msg.toolCall,
        onRun: SQLConnectState.isIdle(model.state) ? (query) => _runSQL(context, ref, model, query) : null,
        onResolveToolQuery: model.llmAgents.lastUsedLLMAgent != null
            ? (approved) => ref
                  .read(aIChatServiceProvider.notifier)
                  .resolveToolQueryExecution(
                    model.chatModel.id,
                    msg.id,
                    approved,
                    model.llmAgents.lastUsedLLMAgent!.id,
                    genChatSystemPrompt(model),
                  )
            : null,
      ),
    );
  }

  @override
  void didUpdateWidget(SessionChatMessages oldWidget) {
    super.didUpdateWidget(oldWidget);
    final messages = widget.model.chatModel.messages;

    if (messages.length != _lastMessageCount) {
      _lastMessageCount = messages.length;
    }

    // 新消息加入或流式输出更新时，若仍处于自动滚动状态则跟随底部。
    widget.chatScrollController.onContentChanged();
  }

  @override
  Widget build(BuildContext context) {
    final messages = widget.model.chatModel.messages;

    return ChatListView(
      chatScrollController: widget.chatScrollController,
      itemCount: messages.length,
      bottomAnchorHeight: 100,
      padding: const EdgeInsets.fromLTRB(kSpacingSmall, 0, kSpacingSmall + kSpacingTiny, 0),
      itemBuilder: (context, index) {
        return _buildMessage(context, ref, widget.model, messages[index], index);
      },
    );
  }
}

// 未配置模型的引导页面
class SessionChatGuide extends ConsumerWidget {
  const SessionChatGuide({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return EmptyPage(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Text(
            AppLocalizations.of(context)!.ai_chat_guide_tip,
            style: Theme.of(
              context,
            ).textTheme.bodySmall?.copyWith(color: Theme.of(context).colorScheme.onSurfaceVariant), // 没有模型时显示的文字颜色
          ),
          const SizedBox(height: kSpacingSmall),
          LinkButton(
            text: AppLocalizations.of(context)!.ai_chat_guide_tip_add_model,
            onPressed: () {
              // 切换到模型设置tab
              ref.read(settingTabServiceProvider.notifier).setSelectedSettingType(SettingType.llmApi);
              GoRouter.of(context).go('/settings');
            },
          ),
        ],
      ),
    );
  }
}
