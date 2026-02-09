import 'package:client/widgets/sql_highlight.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/divider.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:client/l10n/app_localizations.dart';

class SqlChatField extends StatefulWidget {
  final String name;
  final String codes;
  final Function(String)? onRun;
  const SqlChatField({
    super.key,
    required this.codes,
    required this.onRun,
    required this.name,
  });

  @override
  State<SqlChatField> createState() => _SqlChatFieldState();
}

class _SqlChatFieldState extends State<SqlChatField> {
  static const _copyFeedbackDuration = Duration(seconds: 2);

  bool _copied = false;

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
              Text(widget.name, style: Theme.of(context).textTheme.bodySmall),
              const Spacer(),
              if (widget.name == "sql")
                RectangleIconButton.small(
                  tooltip: AppLocalizations.of(context)!.button_tooltip_run_sql_new_tab,
                  icon: Icons.not_started_outlined,
                  iconColor: (widget.onRun != null) ? Colors.green : Colors.grey,
                  onPressed: () {
                    widget.onRun?.call(widget.codes);
                  },
                ),
              RectangleIconButton.small(
                tooltip: AppLocalizations.of(context)!.button_tooltip_copy_sql,
                icon: _copied ? Icons.done : Icons.content_paste,
                onPressed: () async {
                  await Clipboard.setData(ClipboardData(text: widget.codes));
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

  Widget _buildQuerySection(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: RichText(
          text: getSQLHighlightTextSpan(
            widget.codes,
            defalutStyle: TextStyle(
              color: Theme.of(context).colorScheme.onSurface,
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
      padding: const EdgeInsets.only(top: kSpacingSmall),
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
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            // Header 部分
            _buildHeader(context),
            const PixelDivider(),
            // Query 部分
            if (widget.codes.isNotEmpty) ...[
              const SizedBox(height: kSpacingSmall),
              _buildQuerySection(context),
            ],
          ],
        ),
      ),
    );
  }
}
