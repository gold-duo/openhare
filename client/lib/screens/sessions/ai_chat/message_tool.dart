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
import 'package:client/utils/state_value.dart';
import 'package:client/utils/time_format.dart';

/// 工具调用结果展示 Widget
///
/// 参考 SqlChatField 设计，用于展示工具调用结果
/// 针对不同的工具类型使用不同的展示形态
class ToolCallWidget extends ConsumerStatefulWidget {
  final AIChatMessageToolCallQueryModel toolCall;
  final Function(String)? onRun;

  const ToolCallWidget({
    super.key,
    required this.toolCall,
    this.onRun,
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

  /// 获取结果状态（避免重复访问）
  StateValue<BaseQueryResult>? get _resultState => widget.toolCall.result;

  /// 判断是否有查询内容
  bool get _hasQuery => widget.toolCall.query.isNotEmpty;

  /// 判断是否有结果需要显示（包括成功结果和错误）
  bool get _hasResultToShow {
    if (_resultState == null) return false;
    return _resultState!.match(
      (result) => result.columns.isNotEmpty && result.rows.isNotEmpty,
      (error) => true,
      () => false,
    );
  }

  /// 将SQL转换为单行显示
  String _sqlToSingleLine(String sql) {
    return sql.replaceAll(RegExp(r'\s+'), ' ').trim();
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
      child: Row(
        children: [
          Text(
            AppLocalizations.of(context)!.ai_chat_result_rows_returned(result.rows.length),
            style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Theme.of(context).colorScheme.onSurfaceVariant,
                ),
          ),
          const SizedBox(width: kSpacingMedium),
          Text(
            AppLocalizations.of(context)!.ai_chat_result_rows_affected(result.affectedRows.toInt()),
            style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Theme.of(context).colorScheme.onSurfaceVariant,
                ),
          ),
          const SizedBox(width: kSpacingMedium),
          Text(
            AppLocalizations.of(context)!.ai_chat_execution_time(widget.toolCall.executeTime?.format() ?? '-'),
            style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: Theme.of(context).colorScheme.onSurfaceVariant,
                ),
          ),
        ],
      ),
    );
  }

  Widget _buildResultTable(BuildContext context, BaseQueryResult result) {
    if (result.columns.isEmpty || result.rows.isEmpty) {
      return const SizedBox.shrink();
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
              color: Theme.of(context).colorScheme.onErrorContainer,
            ),
      ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    return Container(
      height: bottomBarHeight,
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerLow,
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
    final baseColor = Theme.of(context).colorScheme.surfaceContainerLow;
    final hoverColor = Theme.of(context).colorScheme.surfaceContainer;
    final statusStyle = Theme.of(context).textTheme.bodySmall?.copyWith(
          color: Theme.of(context).colorScheme.onSurfaceVariant,
        );

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
                      color: Theme.of(context).colorScheme.onSurfaceVariant,
                    ),
                  const Spacer(),
                  _resultState == null
                      ? const SizedBox.shrink()
                      : _resultState!.match(
                          (result) => Text(
                            AppLocalizations.of(context)!.ai_chat_execution_success,
                            style: statusStyle,
                          ),
                          (error) => Text(
                            AppLocalizations.of(context)!.ai_chat_execution_failed,
                            style: statusStyle,
                          ),
                          () => const Loading.small(),
                        ),
                  const SizedBox(width: kSpacingTiny),
                ],
              ),
            ),
          ),
        ),
      ),
    );
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
                _expanded ? widget.toolCall.query : _sqlToSingleLine(widget.toolCall.query),
                defalutStyle: TextStyle(
                  color: Theme.of(context).colorScheme.onSurface,
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
    final dpr = MediaQuery.of(context).devicePixelRatio;

    return Padding(
      padding: const EdgeInsets.only(bottom: kSpacingMedium),
      child: Container(
        decoration: BoxDecoration(
          color: Theme.of(context).colorScheme.surfaceContainerLowest,
          borderRadius: BorderRadius.circular(10),
          border: Border.all(
            color: Theme.of(context).dividerColor,
            width: 1 / dpr,
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
                  child: _resultState == null
                      ? const SizedBox.shrink()
                      : _resultState!.match(
                          (result) =>
                              _expanded ? _buildResultTable(context, result) : _buildResultStatistics(context, result),
                          (error) => _buildErrorDisplay(context, error),
                          () => const SizedBox.shrink(),
                        ),
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
