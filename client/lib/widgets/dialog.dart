import 'dart:math';
import 'package:flutter/material.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';

// 二次确认的对话框，确认后执行onConfirm, 点击取消或关闭对话框则不执行
void doActionDialog(
  BuildContext context,
  String title,
  String message,
  Function onConfirm, {
  Icon? icon,
}) {
  showDialog(
    context: context,
    builder: (context) {
      return AlertDialog(
        backgroundColor: Theme.of(context).colorScheme.surfaceContainerLowest, // 对话框默认背景色
        title: Row(
          children: [
            if (icon != null) ...[
              icon,
              const SizedBox(width: 8),
            ],
            Text(title, style: Theme.of(context).textTheme.titleLarge),
          ],
        ),
        content: SizedBox(
          width: 300,
          child: Padding(
            padding: const EdgeInsets.symmetric(vertical: 8.0),
            child: Text(
              message,
              style: Theme.of(context).textTheme.bodyMedium,
            ),
          ),
        ),
        actionsPadding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: Text(
              AppLocalizations.of(context)!.cancel,
              style: TextStyle(color: Theme.of(context).colorScheme.secondary), // 取消按钮文字颜色
            ),
          ),
          TextButton(
            onPressed: () {
              onConfirm();
              Navigator.of(context).pop();
            },
            child: Text(
              AppLocalizations.of(context)!.confirm,
              style: TextStyle(color: icon?.color ?? Theme.of(context).colorScheme.primary), // 确认按钮文字颜色
            ),
          ),
        ],
      );
    },
  );
}

// 通用的自定义对话框组件
class CustomDialog extends StatelessWidget {
  final String title;
  final String? subtitle;
  final Widget? titleIcon;
  final Widget? footerLeading;
  final Widget content;
  final List<Widget> actions;
  final double? maxWidth;
  final double? maxHeight;

  const CustomDialog({
    super.key,
    required this.title,
    this.subtitle,
    this.titleIcon,
    this.footerLeading,
    required this.content,
    required this.actions,
    this.maxWidth = 640,
    this.maxHeight,
  });

  @override
  Widget build(BuildContext context) {
    return Dialog(
      child: CustomDialogWidget(
        title: title,
        subtitle: subtitle,
        titleIcon: titleIcon,
        footerLeading: footerLeading,
        body: content,
        actions: actions,
        maxWidth: maxWidth ?? 640,
        maxHeight: maxHeight,
      ),
    );
  }
}

class CustomDialogWidget extends StatelessWidget {
  final String title;
  final String? subtitle;
  final Widget? titleIcon;
  final Widget? footerLeading;
  final Widget body;

  final List<Widget> actions;
  final double maxWidth;

  /// 为 null 时：不超出视口并避开顶底栏，且不超过 800。
  final double? maxHeight;

  const CustomDialogWidget({
    super.key,
    required this.title,
    this.subtitle,
    this.titleIcon,
    this.footerLeading,
    required this.body,
    required this.actions,
    required this.maxWidth,
    this.maxHeight,
  });

  @override
  Widget build(BuildContext context) {
    final resolvedMaxHeight =
        maxHeight ??
        min(
          MediaQuery.of(context).size.height - tabbarHeight - bottomBarHeight - 10,
          800,
        ); // 高度不能超出屏幕高度，且不能覆盖顶部和底部状态栏
    return ConstrainedBox(
      constraints: BoxConstraints(maxWidth: maxWidth, maxHeight: resolvedMaxHeight),
      child: Container(
        decoration: BoxDecoration(
          color: Theme.of(context).colorScheme.surfaceContainerLowest, // 对话框默认背景色
          borderRadius: BorderRadius.circular(12),
        ),
        padding: const EdgeInsets.fromLTRB(
          kSpacingMedium,
          kSpacingMedium,
          kSpacingMedium,
          kSpacingMedium,
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Builder(
              builder: (context) {
                final theme = Theme.of(context);
                final cs = theme.colorScheme;
                final textTheme = theme.textTheme;
                return Padding(
                  padding: const EdgeInsets.only(bottom: kSpacingSmall, right: 0),
                  child: Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      if (titleIcon != null) ...[
                        DecoratedBox(
                          decoration: BoxDecoration(
                            color: cs.primaryContainer,
                            borderRadius: BorderRadius.circular(10),
                          ),
                          child: Padding(
                            padding: const EdgeInsets.all(kSpacingSmall),
                            child: titleIcon!,
                          ),
                        ),
                        const SizedBox(width: kSpacingSmall),
                      ],
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(title, style: textTheme.titleMedium),
                            if (subtitle != null && subtitle!.isNotEmpty)
                              Text(
                                subtitle!,
                                style: textTheme.bodySmall?.copyWith(color: cs.onSurfaceVariant),
                              ),
                          ],
                        ),
                      ),
                      RectangleIconButton.medium(
                        tooltip: AppLocalizations.of(context)!.close,
                        icon: Icons.close,
                        iconColor: cs.onSurfaceVariant,
                        onPressed: () => Navigator.of(context).pop(),
                      ),
                    ],
                  ),
                );
              },
            ),
            const SizedBox(height: kSpacingMedium),
            Expanded(child: body),
            const SizedBox(height: kSpacingMedium),
            Row(
              crossAxisAlignment: CrossAxisAlignment.center,
              mainAxisAlignment: footerLeading == null ? MainAxisAlignment.end : MainAxisAlignment.start,
              children: [
                if (footerLeading != null)
                  Expanded(
                    child: Align(
                      alignment: Alignment.centerLeft,
                      child: footerLeading!,
                    ),
                  ),
                Row(
                  mainAxisSize: MainAxisSize.min,
                  children: actions,
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
