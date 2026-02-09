import 'package:client/models/sessions.dart';
import 'package:client/models/settings.dart';
import 'package:client/services/sessions/session_chat.dart';
import 'package:client/services/sessions/session_controller.dart';
import 'package:client/services/sessions/session_sql_result.dart';
import 'package:client/services/settings/settings.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/empty.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:client/models/ai.dart';
import 'package:go_router/go_router.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/screens/sessions/ai_chat/message_tool.dart';
import 'package:client/screens/sessions/ai_chat/message_user.dart';
import 'package:client/screens/sessions/ai_chat/message_ai.dart';
import 'package:client/screens/sessions/ai_chat/input_user.dart';

class SessionDrawerChat extends ConsumerStatefulWidget {
  const SessionDrawerChat({super.key});

  @override
  ConsumerState<SessionDrawerChat> createState() => _SessionDrawerChatState();
}

class _SessionDrawerChatState extends ConsumerState<SessionDrawerChat> {
  @override
  Widget build(BuildContext context) {
    SessionAIChatModel model = ref.watch(sessionAIChatProvider);
    return Column(
      children: [
        // 聊天内容
        Expanded(
          child: (model.llmAgents.agents.isEmpty) ? const SessionChatGuide() : SessionChatMessages(model: model),
        ),
        // 下方的输入框区域
        SessionChatInputCard(model: model),
        const SizedBox(height: kSpacingMedium),
      ],
    );
  }
}

class SessionChatMessages extends ConsumerStatefulWidget {
  final SessionAIChatModel model;
  const SessionChatMessages({super.key, required this.model});

  @override
  ConsumerState<SessionChatMessages> createState() => _SessionChatMessagesState();
}

class _SessionChatMessagesState extends ConsumerState<SessionChatMessages> {
  int _lastMessageCount = 0;
  DateTime? _lastScrollTime;

  void _runSQL(BuildContext context, WidgetRef ref, SessionAIChatModel model, String code) {
    ref.read(sQLResultsServicesProvider.notifier).queryAddResult(model.sessionId, code);
  }

  Widget _buildMessage(
      BuildContext context, WidgetRef ref, SessionAIChatModel model, AIChatMessageItem message, int index) {
    return message.when(
      userMessage: (msg) => UserMessage(message: msg),
      assistantMessage: (msg) => AIMessage(
        message: msg,
        onRunSQL: SQLConnectState.isIdle(model.state) ? (code) => _runSQL(context, ref, model, code) : null,
      ),
      toolsResult: (msg) => ToolCallWidget(
        toolCall: msg.toolCall,
        onRun: SQLConnectState.isIdle(model.state) ? (query) => _runSQL(context, ref, model, query) : null,
      ),
    );
  }

  void _scrollToBottom({bool isWaiting = false}) {
    final scrollController = SessionController.sessionController(widget.model.sessionId).aiChatScrollController;
    if (!scrollController.hasClients) return;

    final position = scrollController.position;
    if (!position.hasContentDimensions) return;

    // 节流：waiting状态下更频繁，其他情况节流更严格
    final now = DateTime.now();
    final throttleDuration = isWaiting ? const Duration(milliseconds: 100) : const Duration(milliseconds: 200);
    if (_lastScrollTime != null && now.difference(_lastScrollTime!) < throttleDuration) {
      return;
    }
    _lastScrollTime = now;

    final target = position.maxScrollExtent;
    final distance = (position.pixels - target).abs();

    // 如果已经很接近底部，直接跳转
    if (distance < 30) {
      scrollController.jumpTo(target);
    } else {
      scrollController.animateTo(
        target,
        duration: const Duration(milliseconds: 300),
        curve: Curves.easeOutCubic,
      );
    }
  }

  @override
  void didUpdateWidget(SessionChatMessages oldWidget) {
    super.didUpdateWidget(oldWidget);
    final messages = widget.model.chatModel.messages;

    // 消息数量变化时滚动
    if (messages.length != _lastMessageCount) {
      _lastMessageCount = messages.length;
      WidgetsBinding.instance.addPostFrameCallback((_) {
        if (mounted) {
          _scrollToBottom();
        }
      });
    }
    _lastMessageCount = messages.length;
  }

  @override
  void initState() {
    super.initState();
    _lastMessageCount = widget.model.chatModel.messages.length;
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (mounted) {
        _scrollToBottom();
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    final messages = widget.model.chatModel.messages;
    final state = widget.model.chatModel.state;

    // 等待状态时（流式输出）滚动到底部
    if (state == AIChatState.waiting) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        if (mounted) {
          _scrollToBottom(isWaiting: true);
        }
      });
    }

    return ListView.builder(
      controller: SessionController.sessionController(widget.model.sessionId).aiChatScrollController,
      itemCount: messages.length,
      padding: const EdgeInsets.fromLTRB(kSpacingSmall, kSpacingMedium, kSpacingSmall + kSpacingTiny, 0),
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
        child: Column(mainAxisAlignment: MainAxisAlignment.center, children: [
      Text(AppLocalizations.of(context)!.ai_chat_guide_tip),
      const SizedBox(height: kSpacingSmall),
      LinkButton(
        text: AppLocalizations.of(context)!.ai_chat_guide_tip_add_model,
        onPressed: () {
          // 切换到模型设置tab
          ref.read(settingTabServiceProvider.notifier).setSelectedSettingType(SettingType.llmApi);
          GoRouter.of(context).go('/settings');
        },
      ),
    ]));
  }
}
