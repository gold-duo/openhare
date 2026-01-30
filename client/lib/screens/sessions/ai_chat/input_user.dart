import 'package:client/l10n/app_localizations.dart';
import 'package:client/models/ai.dart';
import 'package:client/models/sessions.dart';
import 'package:client/services/ai/agent.dart';
import 'package:client/services/ai/chat.dart';
import 'package:client/services/ai/prompt.dart';
import 'package:client/services/sessions/session_controller.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/code_auto_complete.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/loading.dart';
import 'package:client/widgets/menu.dart';
import 'package:client/widgets/mention_text.dart';
import 'package:client/widgets/sql_highlight.dart';
import 'package:client/utils/fuzzy_match.dart';
import 'package:client/widgets/tooltip.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:hugeicons/hugeicons.dart';

class SessionChatInputCard extends ConsumerStatefulWidget {
  final SessionAIChatModel model;

  const SessionChatInputCard({super.key, required this.model});

  @override
  ConsumerState<SessionChatInputCard> createState() => _SessionChatInputCardState();
}

class _SessionChatInputCardState extends ConsumerState<SessionChatInputCard> {
  Future<void> _sendMessage(AIChatId chatId, SessionAIChatModel chatModel) async {
    final chatInputController = SessionController.sessionController(chatModel.sessionId).chatInputController;
    final encoded = chatInputController.text.trim();
    final text = MentionSegmentSerializer.decode(encoded).map((s) => s.toDisplayText()).join().trim();
    chatInputController.clear();

    // 调用AIChatService的chat方法
    await ref.read(aIChatServiceProvider.notifier).chat(
          chatId,
          chatModel.llmAgents.lastUsedLLMAgent!.id,
          genChatSystemPrompt(chatModel),
          message: text,
        );

    final scrollController = SessionController.sessionController(chatModel.sessionId).aiChatScrollController;

    // 滚动到底部
    Future.delayed(const Duration(milliseconds: 100), () {
      if (scrollController.hasClients) {
        scrollController.animateTo(
          scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 200),
          curve: Curves.easeOut,
        );
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    final services = ref.read(aIChatServiceProvider.notifier);

    return Padding(
      padding: const EdgeInsets.fromLTRB(kSpacingSmall - 5, 0, kSpacingSmall, 0),
      child: Container(
        padding: const EdgeInsets.fromLTRB(kSpacingSmall, kSpacingSmall, kSpacingSmall, kSpacingTiny),
        // 设置一个圆角
        decoration: BoxDecoration(
          color: Theme.of(context).colorScheme.surfaceContainer,
          borderRadius: BorderRadius.circular(12),
          border: Border.all(
            color: Theme.of(context).colorScheme.surfaceContainerHighest,
            width: 0.5,
          ),
        ),
        child: Column(
          children: [
            // 输入框
            ChatInputFieldWidget(
              model: widget.model,
              onSubmitted: widget.model.canSendMessage()
                  ? () => _sendMessage(widget.model.chatModel.id, widget.model)
                  : null,
            ),

            const SizedBox(height: kSpacingSmall),

            // 工具栏
            Row(
              children: [
                const SizedBox(width: kSpacingTiny),
                // 模型选择
                ModelSelectorWidget(model: widget.model),
                const Spacer(),

                // 清空聊天记录
                RectangleIconButton.small(
                  tooltip: AppLocalizations.of(context)!.button_tooltip_clear_chat,
                  icon: Icons.cleaning_services,
                  onPressed:
                      widget.model.canClearMessage() ? () => services.cleanMessages(widget.model.chatModel.id) : null,
                ),

                // 发送消息
                (widget.model.chatModel.state == AIChatState.waiting)
                    ? const Loading.small()
                    : RectangleIconButton.small(
                        tooltip: AppLocalizations.of(context)!.button_tooltip_send_message,
                        icon: Icons.send,
                        onPressed: widget.model.canSendMessage()
                            ? () => _sendMessage(widget.model.chatModel.id, widget.model)
                            : null,
                      ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

/// 模型选择器组件
class ModelSelectorWidget extends ConsumerStatefulWidget {
  final SessionAIChatModel model;

  const ModelSelectorWidget({super.key, required this.model});

  @override
  ConsumerState<ModelSelectorWidget> createState() => _ModelSelectorWidgetState();
}

class _ModelSelectorWidgetState extends ConsumerState<ModelSelectorWidget> {
  void _onModelSearchChanged() {
    setState(() {});
  }

  List<LLMAgentModel> _filteredAgents(SessionAIChatModel model, String searchText) {
    if (searchText.isEmpty) {
      return model.llmAgents.agents.values.toList();
    }
    return model.llmAgents.agents.values
        .where((agent) => agent.setting.name.toLowerCase().contains(searchText.toLowerCase()))
        .toList();
  }

  bool _isModelSelected(SessionAIChatModel model, LLMAgentModel agent) {
    return model.llmAgents.lastUsedLLMAgent?.id == agent.id;
  }

  @override
  Widget build(BuildContext context) {
    final modelSearchTextController =
        SessionController.sessionController(widget.model.sessionId).aiChatModelSearchTextController;

    // 模型选择工具栏
    final modelToolWidget = Container(
      constraints: const BoxConstraints(
        maxWidth: 120,
      ),
      padding: const EdgeInsets.fromLTRB(kSpacingSmall, kSpacingTiny, kSpacingSmall, kSpacingTiny),
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerLowest,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(
          width: 1,
          color: Theme.of(context).colorScheme.surfaceContainerHigh,
        ),
      ),
      child: Text(
        widget.model.llmAgents.lastUsedLLMAgent?.setting.name ?? "-",
        overflow: TextOverflow.ellipsis,
        style: Theme.of(context).textTheme.bodySmall,
      ),
    );

    return OverlayMenu(
      isAbove: true,
      spacing: kSpacingTiny,
      tabs: [
        for (var agent in _filteredAgents(widget.model, modelSearchTextController.text))
          OverlayMenuItem(
            height: 24,
            child: Padding(
              padding: const EdgeInsets.fromLTRB(kSpacingSmall, 0, kSpacingSmall, 0),
              child: Align(
                alignment: Alignment.centerLeft,
                child: Row(
                  children: [
                    // 如果agent是当前选中的模型，则显示选中状态
                    _isModelSelected(widget.model, agent)
                        ? const Icon(
                            Icons.check_circle,
                            size: kIconSizeSmall,
                            color: Colors.green,
                          )
                        : const Icon(
                            Icons.circle_outlined,
                            size: kIconSizeSmall,
                          ),
                    const SizedBox(width: kSpacingTiny),
                    Expanded(
                      child: TooltipText(text: agent.setting.name, style: Theme.of(context).textTheme.bodySmall),
                    ),
                  ],
                ),
              ),
            ),
            onTabSelected: () {
              ref.read(lLMAgentServiceProvider.notifier).updateLastUsedLLMAgent(agent.id);
            },
          ),
      ],
      footer: OverlayMenuFooter(
        height: 36,
        child: Padding(
          padding: const EdgeInsets.fromLTRB(kSpacingSmall, kSpacingTiny, kSpacingSmall, kSpacingTiny),
          child: SearchBarTheme(
            data: SearchBarThemeData(
                textStyle: WidgetStatePropertyAll(Theme.of(context).textTheme.bodySmall),
                backgroundColor: WidgetStatePropertyAll(Theme.of(context).colorScheme.surfaceContainer),
                elevation: const WidgetStatePropertyAll(0),
                constraints: const BoxConstraints(
                  minHeight: 24,
                )),
            child: SearchBar(
                controller: modelSearchTextController,
                onChanged: (value) {
                  _onModelSearchChanged();
                },
                trailing: const [
                  Icon(
                    Icons.search,
                    size: kIconSizeSmall,
                  ),
                ]),
          ),
        ),
      ),
      child: modelToolWidget,
    );
  }
}

/// 表提示类，复用 FuzzyMatchCodePrompt 的逻辑
class _TablePrompt extends FuzzyMatchCodePrompt {
  const _TablePrompt({required super.word});
}

/// 聊天输入框组件
class ChatInputFieldWidget extends ConsumerStatefulWidget {
  final SessionAIChatModel model;
  final VoidCallback? onSubmitted;

  const ChatInputFieldWidget({
    super.key,
    required this.model,
    this.onSubmitted,
  });

  @override
  ConsumerState<ChatInputFieldWidget> createState() => _ChatInputFieldWidgetState();
}

class _ChatInputFieldWidgetState extends ConsumerState<ChatInputFieldWidget> {
  late final MentionTextEditingController _controller;
  late final SessionController _sessionController;
  late final VoidCallback _syncFromMention;
  late final VoidCallback _syncFromSession;
  bool _syncing = false;

  @override
  void initState() {
    super.initState();
    _sessionController = SessionController.sessionController(widget.model.sessionId);
    final stored = _sessionController.chatInputController.text;
    final looksEncoded = stored.contains(MentionSegmentSerializer.mentionEndChar);
    _controller = MentionTextEditingController(
      text: looksEncoded ? null : stored,
      encodedString: looksEncoded ? stored : null,
      mentionBuilderV2: _buildMentionWidget,
    );

    _syncFromMention = () {
      if (_syncing) return;
      _syncing = true;
      _sessionController.chatInputController.text = _controller.toEncodedString();
      _syncing = false;
    };
    _syncFromSession = () {
      if (_syncing) return;
      _syncing = true;
      final encoded = _sessionController.chatInputController.text;
      if (encoded != _controller.toEncodedString()) {
        _controller.loadFromEncodedString(encoded);
      }
      _syncing = false;
    };

    _controller.addListener(_syncFromMention);
    _sessionController.chatInputController.addListener(_syncFromSession);

    // 初始化时确保 session 保存的是 encodedString
    _syncFromMention();
  }

  @override
  void dispose() {
    _controller.removeListener(_syncFromMention);
    _sessionController.chatInputController.removeListener(_syncFromSession);
    _controller.dispose();
    super.dispose();
  }

  Widget _buildMentionWidget(
    BuildContext context,
    MentionSegment segment,
    TextStyle baseStyle,
    bool hovering,
    VoidCallback onDelete,
  ) {
    final colorScheme = Theme.of(context).colorScheme;
    final cardBg = colorScheme.primaryContainer;
    final cardFg = colorScheme.onSurface;
    final fontSize = baseStyle.fontSize ?? 14;
    // 让 token 的高度尽量贴近 TextField 的行高（selection 背景高度也会更一致）。
    final heightFactor = baseStyle.height ?? 1.0;
    final tokenHeight = fontSize * heightFactor;
    final labelStyle = baseStyle.copyWith(color: cardFg, height: heightFactor);
    return Container(
      key: ValueKey('table_mention_${segment.label}'),
      padding: const EdgeInsets.symmetric(horizontal: 6),
      decoration: BoxDecoration(
        color: cardBg,
        borderRadius: BorderRadius.circular(5),
      ),
      child: SizedBox(
        height: tokenHeight,
        child: Row(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            if (hovering)
              GestureDetector(
                behavior: HitTestBehavior.opaque,
                onTap: onDelete,
                child: Icon(
                  Icons.close_rounded,
                  size: fontSize,
                  color: cardFg,
                ),
              )
            else
              HugeIcon(
                icon: HugeIcons.strokeRoundedTable,
                size: fontSize,
                color: cardFg,
              ),
            const SizedBox(width: 4),
            Center(
              child: Text(
                segment.label,
                style: labelStyle,
              ),
            ),
          ],
        ),
      ),
    );
  }

  List<String> _getTableNames() {
    if (widget.model.metadata == null || widget.model.currentSchema == null) {
      return [];
    }
    final schema = MetaDataNode(MetaType.instance, "", items: widget.model.metadata!);
    final schemaNodes = schema.getChildren(MetaType.schema, widget.model.currentSchema!);
    return schemaNodes
        .where((e) => e.type == MetaType.table)
        .map((e) => e.value)
        .toList();
  }

  List<String> _filterAndSortTables(List<String> allTables, String query) {
    if (query.isEmpty) {
      return allTables;
    }

    // 使用模糊匹配进行过滤和排序
    final List<(String, double)> scoredTables = [];
    for (final table in allTables) {
      final result = FuzzyMatch.matchWithResult(query, table);
      if (result.matched) {
        scoredTables.add((table, result.score));
      }
    }

    // 按分数排序（降序）
    scoredTables.sort((a, b) => b.$2.compareTo(a.$2));
    return scoredTables.map((item) => item.$1).toList();
  }

  static TextStyle _tableTextBaseStyle(BuildContext context) => GoogleFonts.robotoMono(
        textStyle: Theme.of(context).textTheme.bodySmall,
        color: Theme.of(context).colorScheme.onSurface,
      );

  Widget _buildTableText(BuildContext context, String table, String query) {
    final baseStyle = _tableTextBaseStyle(context);
    if (query.isEmpty) {
      return Text(table, style: baseStyle, maxLines: 1, overflow: TextOverflow.ellipsis);
    }
    final textSpan = _TablePrompt(word: table).getTextSpan(context, query);
    final highlightStyle = baseStyle.copyWith(
      color: Theme.of(context).colorScheme.primary,
      fontWeight: FontWeight.bold,
    );
    return Text.rich(
      _adjustTextSpanStyle(textSpan, baseStyle, highlightStyle),
      maxLines: 1,
      overflow: TextOverflow.ellipsis,
    );
  }

  /// 调整 TextSpan 样式以匹配表搜索的样式（bodySmall + primary 颜色）
  TextSpan _adjustTextSpanStyle(TextSpan textSpan, TextStyle baseStyle, TextStyle highlightStyle) {
    if (textSpan.children == null) {
      final isHighlight = textSpan.style?.color == SQLHighlightColor.keyword;
      return TextSpan(
        text: textSpan.text,
        style: isHighlight ? highlightStyle : (textSpan.style ?? baseStyle).copyWith(fontSize: baseStyle.fontSize),
      );
    }
    final adjustedChildren = textSpan.children!
        .map((span) => span is TextSpan ? _adjustTextSpanStyle(span, baseStyle, highlightStyle) : span)
        .toList();
    return TextSpan(
      children: adjustedChildren,
      style: textSpan.style?.copyWith(fontSize: baseStyle.fontSize) ?? baseStyle,
    );
  }

  List<MentionCandidate> _mentionCandidates(String query) {
    final names = _filterAndSortTables(_getTableNames(), query);
    return names.map((n) => MentionCandidate(label: n)).toList();
  }

  Widget _mentionItemBuilder(BuildContext context, MentionCandidate c, String query) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
      child: Row(
        children: [
          HugeIcon(
            icon: HugeIcons.strokeRoundedTable,
            size: 16,
            color: Theme.of(context).colorScheme.onSurface,
          ),
          const SizedBox(width: kSpacingTiny),
          Expanded(
            child: _buildTableText(context, c.label, query),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final tokenBg = Theme.of(context).colorScheme.primaryContainer;
    return MentionTextField(
      controller: _controller,
      style: Theme.of(context).textTheme.bodyMedium,
      textAlignVertical: TextAlignVertical.center,
      minLines: 1,
      maxLines: 5,
      enabled: widget.model.chatModel.state != AIChatState.waiting,
      textInputAction: TextInputAction.done,
      selectionColor: tokenBg,
      decoration: InputDecoration(
        hintText: AppLocalizations.of(context)!.ai_chat_input_tip,
        border: InputBorder.none,
        isDense: true,
        contentPadding: const EdgeInsets.symmetric(vertical: kSpacingSmall, horizontal: kSpacingTiny),
      ),
      mentionCandidatesBuilder: _mentionCandidates,
      mentionItemBuilder: _mentionItemBuilder,
      onSubmitted: (_) {
        widget.onSubmitted?.call();
        _controller.loadFromEncodedString('');
      },
    );
  }
}
