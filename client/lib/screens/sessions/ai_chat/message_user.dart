import 'package:client/models/ai.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/mention_text.dart';
import 'package:flutter/material.dart';

class UserMessage extends StatelessWidget {
  final AIChatUserMessageModel message;

  const UserMessage({
    super.key,
    required this.message,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: kSpacingMedium),
      child: Align(
        alignment: Alignment.centerLeft,
        child: Container(
          padding: const EdgeInsets.all(10),
          decoration: BoxDecoration(
            color: Theme.of(context).colorScheme.surfaceContainer,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(
              color: Theme.of(context).colorScheme.surfaceContainerHighest,
              width: 0.5,
            ),
          ),
          child: MentionTextField(
            controller: MentionTextEditingController(text: message.content),
            style: Theme.of(context).textTheme.bodyMedium,
            readOnly: true,
            selectionColor: Theme.of(context).colorScheme.primaryContainer,
          ),
        ),
      ),
    );
  }
}
