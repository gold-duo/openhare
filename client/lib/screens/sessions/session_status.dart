import 'package:client/models/sessions.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/widgets/const.dart';
import 'package:flutter/material.dart';
import 'package:client/utils/time_format.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/divider.dart';

class SessionStatusTab extends ConsumerWidget {
  const SessionStatusTab({super.key});

  Widget divider(BuildContext context) {
    return const Padding(
      padding: EdgeInsets.symmetric(horizontal: kSpacingSmall),
      child: PixelVerticalDivider(indent: 10, endIndent: 10),
    );
  }

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    SessionStatusModel? model = ref.watch(selectedSessionStatusProvider);
    if (model == null) {
      return Container(
        height: 30,
      );
    }
    String affectedRowsDisplay = "${model.affectedRows ?? "-"}";
    String executeTimeDisplay = model.executeTime?.format() ?? "-";
    String shortQueryDisplay = model.query?.trimLeft().split("\n")[0] ?? "-";

    return Container(
      padding: const EdgeInsets.only(left: kSpacingSmall),
      child: Row(
        children: [
          if (model.connState != null)
            Tooltip(
              message: model.connErrorMsg ?? '-',
              child: MouseRegion(
                cursor: SystemMouseCursors.click,
                child: IntrinsicWidth(
                  child: Container(
                    constraints: const BoxConstraints(maxWidth: 100),
                    child: Row(
                      mainAxisAlignment: MainAxisAlignment.start,
                      children: [
                        Text(
                          "${AppLocalizations.of(context)!.short_conn}:",
                          style: Theme.of(
                            context,
                          ).textTheme.bodySmall?.copyWith(color: Theme.of(context).colorScheme.onSurfaceVariant),
                        ),
                        const SizedBox(width: kSpacingSmall),
                        Expanded(
                          child: switch (model.connState) {
                            SQLConnectState.connected || SQLConnectState.executing => const Align(
                              alignment: Alignment.centerLeft,
                              child: SizedBox(
                                width: 6,
                                height: 6,
                                child: DecoratedBox(
                                  decoration: BoxDecoration(color: Colors.green, shape: BoxShape.circle),
                                ),
                              ),
                            ),
                            SQLConnectState.failed || SQLConnectState.unHealth => const Align(
                              alignment: Alignment.centerLeft,
                              child: SizedBox(
                                width: 6,
                                height: 6,
                                child: DecoratedBox(
                                  decoration: BoxDecoration(color: Colors.red, shape: BoxShape.circle),
                                ),
                              ),
                            ),
                            _ => const Text("-"),
                          }, // 根据model.state展示不同的图标
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
          if (model.resultId != null) ...[
            // affected rows
            divider(context),
            ValueStatusWidget(
              label: AppLocalizations.of(context)!.effect_rows,
              value: affectedRowsDisplay,
              tooltip: affectedRowsDisplay,
            ),

            // duration
            divider(context),
            ValueStatusWidget(
              label: AppLocalizations.of(context)!.duration,
              value: executeTimeDisplay,
              tooltip: executeTimeDisplay,
            ),

            // sql query
            divider(context),
            ValueStatusWidget(
              width: 300,
              label: AppLocalizations.of(context)!.query,
              value: shortQueryDisplay,
              tooltip: model.query ?? AppLocalizations.of(context)!.no_query_executed,
            ),
          ],
          const Spacer(),
        ],
      ),
    );
  }
}

class ValueStatusWidget extends StatelessWidget {
  final String label;
  final String value;
  final String? tooltip;
  final double width;

  const ValueStatusWidget({
    super.key,
    required this.label,
    required this.value,
    this.tooltip,
    this.width = 150,
  });

  @override
  Widget build(BuildContext context) {
    final textStyle =
        Theme.of(
          context,
        ).textTheme.bodySmall?.copyWith(
          color: Theme.of(context).colorScheme.onSurfaceVariant,
        );
    return Tooltip(
      message: tooltip ?? value,
      child: MouseRegion(
        cursor: SystemMouseCursors.click,
        child: IntrinsicWidth(
          child: Container(
            constraints: BoxConstraints(maxWidth: width),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.start,
              children: [
                Text('$label:', style: textStyle),
                const SizedBox(width: kSpacingTiny),
                Expanded(
                  child: Text(
                    value,
                    style: textStyle,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
