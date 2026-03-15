import 'package:client/widgets/const.dart';
import 'package:flutter/material.dart';

class EmptyPage extends StatelessWidget {
  final Widget child;
  final Color? color;

  const EmptyPage({super.key, required this.child, this.color});

  @override
  Widget build(BuildContext context) {
    return Container(
      color: color,
      child: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(
              Icons.inbox,
              size: 64,
              color: Theme.of(context).colorScheme.surfaceContainerHighest, // 空页面图标颜色
            ),
            const SizedBox(height: kSpacingSmall),
            child,
          ],
        ),
      ),
    );
  }
}
