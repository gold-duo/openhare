import 'package:client/models/ai.dart';
import 'package:client/screens/sessions/ai_chat/block_sql.dart';
import 'package:client/widgets/const.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:gpt_markdown/gpt_markdown.dart';

class AIMessage extends StatefulWidget {
  final AIChatAssistantMessageModel message;
  final DatabaseType dbType;
  final Function(String)? onRunSQL;

  const AIMessage({
    super.key,
    required this.message,
    required this.dbType,
    this.onRunSQL,
  });

  @override
  State<AIMessage> createState() => _AIMessageState();
}

class _AIMessageState extends State<AIMessage> {
  bool _isThinkingExpanded = false;

  Widget _buildError(BuildContext context) {
    return Text(
      widget.message.error ?? "",
      style: Theme.of(context).textTheme.bodySmall?.copyWith(
        color: Theme.of(context).colorScheme.error,
      ),
    );
  }

  Widget _buildThinking(BuildContext context, bool isThinking) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        GestureDetector(
          onTap: () {
            setState(() {
              _isThinkingExpanded = !_isThinkingExpanded;
            });
          },
          child: Row(
            children: [
              Icon(
                _isThinkingExpanded ? Icons.keyboard_arrow_down : Icons.keyboard_arrow_right,
                size: 16,
                color: Theme.of(context).colorScheme.onSurface, // 思考过程下拉按钮的颜色
              ),
              const SizedBox(width: kSpacingTiny),
              if (isThinking && !_isThinkingExpanded)
                SizedBox(
                  width: 12,
                  height: 12,
                  child: CircularProgressIndicator(
                    strokeWidth: 2,
                    valueColor: AlwaysStoppedAnimation<Color>(
                      Theme.of(context).colorScheme.onSurfaceVariant, // 思考过程 loading 颜色
                    ),
                  ),
                ),
              if (isThinking && !_isThinkingExpanded) const SizedBox(width: kSpacingTiny),
              Text(
                isThinking
                    ? AppLocalizations.of(context)!.ai_chat_thinking
                    : AppLocalizations.of(context)!.ai_chat_thinking_process,
                style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Theme.of(context).colorScheme.onSurfaceVariant, // 思考过程文字颜色
                  fontStyle: FontStyle.italic,
                ),
              ),
            ],
          ),
        ),
        if (_isThinkingExpanded) ...[
          SizedBox(height: kSpacingTiny),
          RichText(
            text: TextSpan(
              text: widget.message.thinking?.trim() ?? "",
              style: Theme.of(context).textTheme.bodySmall?.copyWith(
                color: Theme.of(context).colorScheme.onSurfaceVariant, // 思考过程文字颜色
                fontStyle: FontStyle.italic,
              ),
            ),
          ),
        ],
      ],
    );
  }

  Widget _buildContent(BuildContext context, String content) {
    return SelectionArea(
      child: GptMarkdownTheme(
        gptThemeData: GptMarkdownThemeData(
          brightness: Theme.of(context).brightness,
          h1: Theme.of(context).textTheme.titleLarge,
          h2: Theme.of(context).textTheme.titleMedium,
          h3: Theme.of(context).textTheme.titleSmall,
          h4: Theme.of(context).textTheme.bodyLarge,
          h5: Theme.of(context).textTheme.bodyMedium,
          h6: Theme.of(context).textTheme.bodySmall,
          hrLineThickness: 0.2,
          highlightColor: Theme.of(context).colorScheme.surfaceContainerLowest, // 高亮色设置成和背景色一致，等于取消高亮
        ),
        child: GptMarkdown(
          key: ValueKey(widget.message.id.value),
          content,
          style: TextStyle(
            color: Theme.of(context).colorScheme.onSurface, // 内容文字颜色
          ),
          codeBuilder: (context, name, code, closed) {
            return SqlChatField(
              name: name,
              codes: code,
              dbType: widget.dbType,
              onRun: (name == "sql" && widget.onRunSQL != null) ? widget.onRunSQL : null,
            );
          },
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final content = widget.message.content;
    final hasThinking = widget.message.thinking != null && widget.message.thinking!.isNotEmpty;
    // 当 content 有值时，思考结束
    final isThinking = !widget.message.isThinkingCompleted;

    return Padding(
      padding: const EdgeInsets.only(bottom: kSpacingMedium),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          if (widget.message.error != null) _buildError(context),
          if (hasThinking) _buildThinking(context, isThinking),
          if (content.isNotEmpty && content.trim() != "") ...[
            SizedBox(height: kSpacingSmall),
            _buildContent(context, content),
          ],
        ],
      ),
    );
  }
}
