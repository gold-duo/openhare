import 'package:client/widgets/sql_highlight.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/divider.dart';
import 'package:client/widgets/data_grid.dart';
import 'package:client/widgets/loading.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:client/models/ai.dart';
import 'package:flutter/services.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/utils/time_format.dart';

/// 工具调用结果展示 Widget
///
/// 参考 SqlChatField 设计，用于展示工具调用结果
/// 针对不同的工具类型使用不同的展示形态
class ToolCallWidget extends ConsumerStatefulWidget {
  final AIChatId chatId;
  final AIChatMessageId toolsMessageId;
  final AIChatMessageToolCallQueryModel toolCall;
  final Function(String)? onRun;

  /// 待确认 SQL 时：拒绝为 `false`，确认并执行后为 `true`（由上层执行 SQL 并再次发起 [chat]）
  final Future<void> Function(bool approved)? onResolveToolQuery;
  final DatabaseType dbType;

  const ToolCallWidget({
    super.key,
    required this.chatId,
    required this.toolsMessageId,
    required this.dbType,
    required this.toolCall,
    this.onRun,
    this.onResolveToolQuery,
  });

  @override
  ConsumerState<ToolCallWidget> createState() => _ToolCallWidgetState();
}

class _ToolCallWidgetState extends ConsumerState<ToolCallWidget> {
  static const _animationDuration = Duration(milliseconds: 200);
  static const _copyFeedbackDuration = Duration(seconds: 2);
  static const _expandedTableHeight = 300.0;

  bool _copied = false;
  bool _expanded = false;
  bool _footerHovering = false;

  /// 判断是否有查询内容
  bool get _hasQuery => widget.toolCall.query.isNotEmpty;

  /// 判断是否有结果需要显示（包括成功结果和错误）
  bool get _hasResultToShow {
    final tc = widget.toolCall;
    return switch (tc.execState) {
      AIChatToolQueryState.initializing => false,
      AIChatToolQueryState.awaitingUserConfirm => false,
      AIChatToolQueryState.approved => false,
      AIChatToolQueryState.running => false,
      AIChatToolQueryState.rejected => false,
      AIChatToolQueryState.finished => tc.queryResult != null,
      AIChatToolQueryState.failed => tc.errorMessage?.isNotEmpty ?? false,
    };
  }

  /// 将SQL转换为单行显示
  String _sqlToSingleLine(String sql) {
    return sql.replaceAll(RegExp(r'\s+'), ' ').trim(); // todo: 使用sql parser 处理
  }

  List<DataGridColumn> _buildDataGridColumns(
    BuildContext context,
    BaseQueryResult result, {
    int? maxRows,
  }) {
    final rowsToShow = maxRows != null && maxRows < result.rows.length ? result.rows.sublist(0, maxRows) : result.rows;

    return [
      for (int i = 0; i < result.columns.length; i++)
        DataGridColumn.autoSize(
          context: context,
          name: result.columns[i].name,
          dataType: result.columns[i].dataType(),
          cells: [
            for (int j = 0; j < rowsToShow.length; j++)
              DataGridCell(
                data: rowsToShow[j].values[i].getSummary() ?? '',
              ),
          ],
        ),
    ];
  }

  Widget _buildResultStatistics(BuildContext context, BaseQueryResult result) {
    return Padding(
      padding: const EdgeInsets.all(kSpacingSmall),
      child: Text(
        [
          AppLocalizations.of(context)!.ai_chat_result_rows_returned(result.rows.length),
          AppLocalizations.of(context)!.ai_chat_result_rows_affected(result.affectedRows.toInt()),
          AppLocalizations.of(context)!.ai_chat_execution_time(widget.toolCall.executeTime?.format() ?? '-'),
        ].join(' · '),
        style: Theme.of(context).textTheme.bodySmall?.copyWith(
          color: Theme.of(context).colorScheme.onSurface, // tool 里执行结果统计字体颜色
        ),
        overflow: TextOverflow.ellipsis,
      ),
    );
  }

  Widget _buildResultTable(BuildContext context, BaseQueryResult result) {
    if (result.columns.isEmpty || result.rows.isEmpty) {
      return const SizedBox(height: _expandedTableHeight);
    }

    final columns = _buildDataGridColumns(
      context,
      result,
      maxRows: null,
    );
    final controller = DataGridController(columns: columns);

    return SizedBox(
      height: _expandedTableHeight,
      child: DataGrid(controller: controller),
    );
  }

  Widget _buildErrorDisplay(BuildContext context, String error) {
    return Padding(
      padding: const EdgeInsets.all(kSpacingSmall),
      child: SelectableText(
        error,
        style: Theme.of(context).textTheme.bodySmall?.copyWith(
          color: Theme.of(context).colorScheme.onErrorContainer, // tool 错误消息颜色
        ),
      ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    return Container(
      height: bottomBarHeight,
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerHigh, // tool 头部背景颜色
        borderRadius: const BorderRadius.only(
          topLeft: Radius.circular(10),
          topRight: Radius.circular(10),
        ),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
        child: SizedBox(
          height: kIconButtonSizeSmall,
          child: Row(
            children: [
              Icon(
                Icons.polyline,
                size: kIconSizeSmall,
                color: Theme.of(context).colorScheme.primary,
              ),
              const SizedBox(width: kSpacingTiny),
              Text(
                AppLocalizations.of(context)!.ai_chat_tool_execute_query,
                style: Theme.of(context).textTheme.bodySmall,
              ),
              const Spacer(),
              if (widget.onRun != null)
                RectangleIconButton.small(
                  tooltip: AppLocalizations.of(context)!.button_tooltip_run_sql_new_tab,
                  icon: Icons.not_started_outlined,
                  iconColor: Colors.green,
                  onPressed: () {
                    final query = widget.toolCall.query;
                    if (query.isNotEmpty) {
                      widget.onRun?.call(query);
                    }
                  },
                ),
              RectangleIconButton.small(
                tooltip: AppLocalizations.of(context)!.button_tooltip_copy_sql,
                icon: _copied ? Icons.done : Icons.content_paste,
                onPressed: () async {
                  final query = widget.toolCall.query;
                  if (query.isEmpty) return;
                  await Clipboard.setData(ClipboardData(text: query));
                  if (!mounted) return;
                  setState(() => _copied = true);
                  await Future.delayed(_copyFeedbackDuration);
                  if (mounted) {
                    setState(() => _copied = false);
                  }
                },
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildFooter(BuildContext context) {
    final baseColor = Theme.of(context).colorScheme.surfaceContainerHigh; // tool 底部背景颜色
    final hoverColor = Theme.of(context).colorScheme.surfaceContainerHighest; // tool 底部 hover 背景颜色
    final statusStyle = Theme.of(context).textTheme.bodySmall?.copyWith(
      color: Theme.of(context).colorScheme.onSurfaceVariant, // tool 底部状态文字颜色
    );
    final l10n = AppLocalizations.of(context)!;
    final tc = widget.toolCall;

    return MouseRegion(
      cursor: SystemMouseCursors.click,
      onEnter: (_) => setState(() => _footerHovering = true),
      onExit: (_) => setState(() => _footerHovering = false),
      child: GestureDetector(
        onTap: () {
          setState(() => _expanded = !_expanded);
        },
        child: Container(
          decoration: BoxDecoration(
            color: _footerHovering ? hoverColor : baseColor,
            borderRadius: const BorderRadius.only(
              bottomLeft: Radius.circular(10),
              bottomRight: Radius.circular(10),
            ),
          ),
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
            child: SizedBox(
              height: kIconButtonSizeSmall,
              child: Row(
                children: [
                  if (_footerHovering)
                    Icon(
                      _expanded ? Icons.expand_less : Icons.expand_more,
                      size: kIconSizeSmall,
                      color: Theme.of(context).colorScheme.onSurface, // tool 底部 icon 颜色
                    ),
                  const Spacer(),
                  switch (tc.execState) {
                    AIChatToolQueryState.initializing => const SizedBox.shrink(),
                    AIChatToolQueryState.awaitingUserConfirm => _buildFooterConfirmActions(context),
                    AIChatToolQueryState.approved => const SizedBox.shrink(),
                    AIChatToolQueryState.running => const Loading.small(),
                    AIChatToolQueryState.rejected => Text(l10n.ai_chat_execution_cancelled, style: statusStyle),
                    AIChatToolQueryState.finished => Text(l10n.ai_chat_execution_success, style: statusStyle),
                    AIChatToolQueryState.failed => Text(l10n.ai_chat_execution_failed, style: statusStyle),
                  },
                  const SizedBox(width: kSpacingTiny),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildFooterConfirmActions(BuildContext context) {
    const double footerConfirmButtonWidth = 64;
    const double footerConfirmButtonHeight = 28;
    if (widget.onResolveToolQuery == null) {
      return const SizedBox.shrink();
    }
    final theme = Theme.of(context);

    final baseStyle = ButtonStyle(
      minimumSize: WidgetStateProperty.all(
        const Size(footerConfirmButtonWidth, footerConfirmButtonHeight),
      ),
      padding: WidgetStateProperty.all(
        const EdgeInsets.symmetric(horizontal: kSpacingSmall),
      ),
      textStyle: WidgetStateProperty.all(theme.textTheme.labelSmall),
      shape: WidgetStateProperty.all(RoundedRectangleBorder(borderRadius: BorderRadius.circular(6))),
      alignment: Alignment.center,
    );

    final cancelStyle = baseStyle.copyWith(
      foregroundColor: WidgetStateProperty.all(theme.colorScheme.onSurfaceVariant),
      side: WidgetStateProperty.all(BorderSide(color: theme.colorScheme.outline)),
      backgroundColor: WidgetStateProperty.resolveWith((states) {
        if (states.contains(WidgetState.hovered)) {
          return theme.colorScheme.surfaceContainerHighest;
        }
        return theme.colorScheme.surface;
      }),
    );

    final runStyle = baseStyle.copyWith(
      textStyle: WidgetStateProperty.all(
        theme.textTheme.labelSmall?.copyWith(
          color: theme.colorScheme.onPrimary,
        ),
      ),
      backgroundColor: WidgetStateProperty.resolveWith((states) {
        if (states.contains(WidgetState.disabled)) {
          return theme.colorScheme.onSurface;
        }
        if (states.contains(WidgetState.pressed)) {
          return theme.colorScheme.primary;
        }
        if (states.contains(WidgetState.hovered)) {
          return theme.colorScheme.primary;
        }
        return theme.colorScheme.primary;
      }),
    );

    return Row(
      children: [
        OutlinedButton(
          style: cancelStyle,
          onPressed: () async {
            await widget.onResolveToolQuery!.call(false);
          },
          child: Text(AppLocalizations.of(context)!.ai_chat_tool_confirm_decline),
        ),
        const SizedBox(width: kSpacingTiny),
        FilledButton(
          style: runStyle,
          onPressed: () async {
            await widget.onResolveToolQuery!.call(true);
          },
          child: Text(AppLocalizations.of(context)!.ai_chat_tool_confirm_run),
        ),
      ],
    );
  }

  Widget _buildResultContent(BuildContext context) {
    final tc = widget.toolCall;
    final qr = tc.queryResult;
    if (qr != null) {
      return _expanded ? _buildResultTable(context, qr) : _buildResultStatistics(context, qr);
    }
    final err = tc.errorMessage;
    if (err != null && err.isNotEmpty) {
      return _buildErrorDisplay(context, err);
    }
    return const SizedBox.shrink();
  }

  Widget _buildQuerySection(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(kSpacingSmall),
      child: AnimatedSize(
        duration: _animationDuration,
        curve: Curves.easeInOut,
        child: ClipRect(
          child: SingleChildScrollView(
            scrollDirection: Axis.horizontal,
            child: RichText(
              text: getSQLHighlightTextSpan(
                widget.dbType.dialectType,
                _expanded ? widget.toolCall.query : _sqlToSingleLine(widget.toolCall.query),
                defalutStyle: TextStyle(
                  color: Theme.of(context).colorScheme.onSurface, // tool 查询文字颜色默认色
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: kSpacingMedium),
      child: Container(
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(10),
          border: Border.all(
            color: Theme.of(context).colorScheme.outline, // tool 边框颜色
          ),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Header 部分
            _buildHeader(context),
            const PixelDivider(),
            // Query 部分
            if (_hasQuery) ...[
              _buildQuerySection(context),
            ],
            // 结果部分
            if (_hasResultToShow) ...[
              const PixelDivider(),
              AnimatedSize(
                duration: _animationDuration,
                curve: Curves.easeInOut,
                child: ClipRect(
                  child: _buildResultContent(context),
                ),
              ),
            ],
            // Footer 部分
            const PixelDivider(),
            _buildFooter(context),
          ],
        ),
      ),
    );
  }
}
