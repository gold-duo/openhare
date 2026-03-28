import 'package:client/l10n/app_localizations.dart';
import 'package:client/models/ai.dart';
import 'package:client/models/sessions.dart';
import 'package:client/services/ai/chat.dart';
import 'package:client/services/ai/prompt.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/mention_text.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

class UserMessage extends ConsumerStatefulWidget {
  final AIChatUserMessageModel message;
  final SessionAIChatModel? sessionChatModel;

  const UserMessage({
    super.key,
    required this.message,
    this.sessionChatModel,
  });

  @override
  ConsumerState<UserMessage> createState() => _UserMessageState();
}

class _UserMessageState extends ConsumerState<UserMessage> {
  bool _hovering = false;

  void _onRetry() {
    final model = widget.sessionChatModel;
    if (model == null || model.llmAgents.lastUsedLLMAgent == null) return;
    ref
        .read(aIChatServiceProvider.notifier)
        .retryChat(
          model.chatModel.id,
          model.llmAgents.lastUsedLLMAgent!.id,
          genChatSystemPrompt(model),
          widget.message,
        );
  }

  @override
  Widget build(BuildContext context) {
    final model = widget.sessionChatModel;
    final hasAgent = model?.llmAgents.lastUsedLLMAgent != null;
    final isIdle = model?.chatModel.state == AIChatState.idle;
    final canRetry = hasAgent && isIdle;
    final showRetry = _hovering && hasAgent;
    return Padding(
      padding: const EdgeInsets.only(bottom: kSpacingSmall, top: kSpacingSmall),
      child: Align(
        child: MouseRegion(
          onEnter: (_) => setState(() => _hovering = true),
          onExit: (_) => setState(() => _hovering = false),
          child: Stack(
            clipBehavior: Clip.none,
            children: [
              Container(
                padding: const EdgeInsets.all(kSpacingSmall),
                constraints: const BoxConstraints(
                  maxHeight: 300,
                ),
                decoration: BoxDecoration(
                  color: Theme.of(context).colorScheme.secondaryContainer, // 用户消息卡片背景颜色
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(
                    color: Theme.of(context).colorScheme.outline, // 用户消息卡片边框颜色
                  ),
                ),
                child: MentionTextField(
                  controller: MentionTextEditingController(text: widget.message.content),
                  style: Theme.of(context).textTheme.bodyMedium,
                  readOnly: true,
                ),
              ),
              if (showRetry)
                Positioned(
                  right: kSpacingSmall,
                  bottom: kSpacingTiny,
                  child: RectangleIconButton.small(
                    tooltip: AppLocalizations.of(context)!.button_tooltip_retry_message,
                    icon: Icons.send,
                    onPressed: canRetry ? _onRetry : null,
                  ),
                ),
            ],
          ),
        ),
      ),
    );
  }
}
