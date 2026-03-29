import 'package:client/widgets/sql_highlight.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:client/l10n/app_localizations.dart';

class SqlChatField extends StatefulWidget {
  final String name;
  final String codes;
  final DatabaseType dbType;
  final Function(String)? onRun;
  const SqlChatField({
    super.key,
    required this.dbType,
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

  Widget _buildQuerySection(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: SelectableText.rich(
          getSQLHighlightTextSpan(
            widget.dbType.dialectType,
            widget.codes,
            defalutStyle: TextStyle(
              color: Theme.of(context).colorScheme.onSurface,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildActionButtons(BuildContext context) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
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
    );
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(top: kSpacingSmall),
      child: Container(
        width: double.infinity,
        decoration: BoxDecoration(
          color: Theme.of(context).colorScheme.surfaceContainerLowest,
          borderRadius: BorderRadius.circular(10),
          border: Border.all(
            color: Theme.of(context).colorScheme.outlineVariant,
          ),
        ),
        child: Stack(
          children: [
            if (widget.codes.isNotEmpty)
              Padding(
                padding: const EdgeInsets.only(
                  top: kIconButtonSizeSmall,
                  bottom: kSpacingTiny,
                ),
                child: _buildQuerySection(context),
              ),
            Positioned(
              top: kSpacingSmall,
              right: kSpacingSmall,
              child: _buildActionButtons(context),
            ),
          ],
        ),
      ),
    );
  }
}
