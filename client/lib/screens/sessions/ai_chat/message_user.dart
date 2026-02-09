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
    ref.read(aIChatServiceProvider.notifier).retryChat(
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
      padding: const EdgeInsets.only(bottom: kSpacingMedium),
      child: Align(
        // alignment: Alignment.centerLeft,
        child: MouseRegion(
          onEnter: (_) => setState(() => _hovering = true),
          onExit: (_) => setState(() => _hovering = false),
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
            height: 42,
            decoration: BoxDecoration(
              color: Theme.of(context).colorScheme.surfaceContainer,
              borderRadius: BorderRadius.circular(12),
              border: Border.all(
                color: Theme.of(context).colorScheme.surfaceContainerHighest,
                width: 0.5,
              ),
            ),
            child: Row(
              crossAxisAlignment: CrossAxisAlignment.center,
              mainAxisAlignment: MainAxisAlignment.center,
              mainAxisSize: MainAxisSize.min,
              children: [
                Expanded(
                  child: MentionTextField(
                    controller: MentionTextEditingController(text: widget.message.content),
                    style: Theme.of(context).textTheme.bodyMedium,
                    readOnly: true,
                    selectionColor: Theme.of(context).colorScheme.primaryContainer,
                  ),
                ),
                if (showRetry) ...[
                  const SizedBox(width: kSpacingTiny),
                  RectangleIconButton.small(
                    tooltip: AppLocalizations.of(context)!.button_tooltip_retry_message,
                    icon: Icons.send,
                    onPressed: canRetry ? _onRetry : null,
                  ),
                ],
              ],
            ),
          ),
        ),
      ),
    );
  }
}
