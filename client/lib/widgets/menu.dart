import 'dart:async';

import 'package:client/widgets/const.dart';
import 'package:flutter/material.dart';

class OverlayMenu extends StatefulWidget {
  final double maxHeight;
  final double maxWidth;
  final List<OverlayMenuItem> tabs;
  final OverlayMenuHeader? header;
  final OverlayMenuFooter? footer;
  final Widget child;

  /// 支持设置弹窗的位置。在上方或者下方。默认在下方
  final bool isAbove;

  /// 支持设置弹窗的间距。默认0
  final double spacing;

  /// 支持设置点击菜单项后是否关闭菜单。默认关闭
  final bool closeOnSelectItem;

  /// 把弹出菜单对齐到比 [child] 更大一圈的矩形时常用（例如与带 [InputDecoration.contentPadding] 的输入框外框对齐）。
  final EdgeInsetsGeometry anchorAlignmentInset;

  const OverlayMenu({
    super.key,
    this.maxHeight = 400,
    this.maxWidth = 220,
    required this.tabs,
    required this.child,
    this.header,
    this.footer,
    this.isAbove = false,
    this.spacing = 0,
    this.closeOnSelectItem = true,
    this.anchorAlignmentInset = EdgeInsets.zero,
  });

  @override
  State<OverlayMenu> createState() => _OverlayMenuState();
}

class _OverlayMenuState extends State<OverlayMenu> {
  bool _showingMenu = false;
  Offset? _childPosition;
  Size? _childSize;

  final LayerLink _layerLink = LayerLink();
  final OverlayPortalController _portalController = OverlayPortalController();

  void _toggleMenu(BuildContext context) {
    if (_showingMenu) {
      setState(() {
        _showingMenu = false;
      });
      _portalController.hide();
    } else {
      // 这里需要获取宿主widget的全局位置和大小
      final RenderBox? child = context.findRenderObject() as RenderBox?;
      if (child != null) {
        final Offset position = child.localToGlobal(Offset.zero);
        final Size size = child.size;
        setState(() {
          _childPosition = position;
          _childSize = size;
          _showingMenu = true;
        });
        _portalController.show();
      }
    }
  }

  Widget _buildMenu(BuildContext context, double maxHeight, {double? fixedWidth}) {
    final box = Container(
      constraints: BoxConstraints(maxWidth: fixedWidth ?? widget.maxWidth, maxHeight: maxHeight),
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerLowest, // 菜单库默认背景色
        border: Border.all(color: Theme.of(context).colorScheme.outline),
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            color: Theme.of(context).colorScheme.outline, // 菜单阴影颜色
            blurRadius: 10,
          ),
        ],
      ),
      // 上面设置的圆角没作用，被下面的widget覆盖了
      child: Column(
        children: [
          if (widget.header != null) widget.header!,
          Expanded(
            child: ListView(
              padding: EdgeInsets.zero,
              shrinkWrap: true,
              children: [
                for (int i = 0; i < widget.tabs.length; i++)
                  widget.tabs[i].onTabSelected != null
                      ? InkWell(
                          onTap: () {
                            widget.tabs[i].onTabSelected?.call();
                            if (widget.closeOnSelectItem) {
                              setState(() {
                                _showingMenu = false;
                              });
                              _portalController.hide();
                            }
                          },
                          child: widget.tabs[i],
                        )
                      : widget.tabs[i],
              ],
            ),
          ),
          if (widget.footer != null) widget.footer!,
        ],
      ),
    );
    if (fixedWidth != null) {
      return SizedBox(width: fixedWidth, child: box);
    }
    return box;
  }

  @override
  Widget build(BuildContext context) {
    return CompositedTransformTarget(
      link: _layerLink,
      child: Stack(
        children: [
          Builder(
            builder: (iconContext) => GestureDetector(onTap: () => _toggleMenu(iconContext), child: widget.child),
          ),
          OverlayPortal(
            controller: _portalController,
            overlayChildBuilder: (context) {
              // 计算弹出菜单的位置
              final Size screenSize = MediaQuery.of(context).size; // 屏幕大小
              final Offset position = _childPosition ?? Offset.zero; // 按钮位置
              final Size childSize = _childSize ?? const Size(40, 40);
              final outset = widget.anchorAlignmentInset.resolve(Directionality.of(context));

              // 计算弹窗的总height
              double menuHeight = 0;
              if (widget.header != null) {
                menuHeight += widget.header!.height;
              }
              for (int i = 0; i < widget.tabs.length; i++) {
                menuHeight += widget.tabs[i].height;
              }
              if (widget.footer != null) {
                menuHeight += widget.footer!.height;
              }
              // 限制菜单高度
              menuHeight = (menuHeight > widget.maxHeight) ? widget.maxHeight : menuHeight;

              var left = position.dx - outset.left;
              final double top;
              if (widget.isAbove) {
                top = position.dy - outset.top - menuHeight - widget.spacing;
              } else {
                top = position.dy + childSize.height + outset.bottom + widget.spacing;
              }

              double? menuFixedWidth;
              var menuWidth = widget.maxWidth;
              if (outset.horizontal > 0 && childSize.width > 0) {
                menuFixedWidth = childSize.width + outset.left + outset.right;
                menuWidth = menuFixedWidth;
              }
              const screenPad = 8.0;
              final maxPanel = screenSize.width - screenPad * 2;
              if (menuWidth > maxPanel) {
                menuWidth = maxPanel;
                if (menuFixedWidth != null) {
                  menuFixedWidth = menuWidth;
                }
              }

              if (left + menuWidth > screenSize.width) {
                left = screenSize.width - menuWidth - screenPad;
              }
              if (left < screenPad) {
                left = screenPad;
              }

              return Stack(
                children: [
                  // 点击遮罩关闭菜单
                  Positioned.fill(
                    child: GestureDetector(
                      behavior: HitTestBehavior.translucent,
                      onTap: () {
                        setState(() {
                          _showingMenu = false;
                        });
                        _portalController.hide();
                      },
                    ),
                  ),
                  Positioned(
                    left: left,
                    top: top,
                    child: Material(
                      color: Colors.transparent,
                      child: _buildMenu(context, menuHeight, fixedWidth: menuFixedWidth),
                    ),
                  ),
                ],
              );
            },
          ),
        ],
      ),
    );
  }
}

class OverlayMenuItem extends StatefulWidget {
  final double height;
  final Widget child;
  final void Function()? onTabSelected;

  const OverlayMenuItem({super.key, required this.height, required this.child, this.onTabSelected});

  @override
  State<OverlayMenuItem> createState() => _OverlayMenuItemState();
}

class _OverlayMenuItemState extends State<OverlayMenuItem> {
  bool _hovering = false;

  @override
  Widget build(BuildContext context) {
    final hoverColor = Theme.of(context).colorScheme.surfaceContainerLow; // 菜单列鼠标移入的颜色

    return MouseRegion(
      onEnter: (_) => setState(() => _hovering = true),
      onExit: (_) => setState(() => _hovering = false),
      child: SizedBox(
        height: widget.height,
        child: Container(color: _hovering ? hoverColor : null, child: widget.child),
      ),
    );
  }
}

class _OverlayNumberTextField extends StatefulWidget {
  const _OverlayNumberTextField({
    required this.value,
    required this.onChanged,
  });

  /// 当前应展示的数字（与外部状态一致；外部变更时通过 [didUpdateWidget] 同步到输入框）。
  final int value;
  final ValueChanged<int?> onChanged;

  @override
  State<_OverlayNumberTextField> createState() => _OverlayNumberTextFieldState();
}

class _OverlayNumberTextFieldState extends State<_OverlayNumberTextField> {
  late final TextEditingController _controller;
  bool _showSuccess = false;
  bool _hovering = false;
  Timer? _successTimer;

  @override
  void initState() {
    super.initState();
    _controller = TextEditingController(text: widget.value.toString());
  }

  @override
  void didUpdateWidget(_OverlayNumberTextField oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (widget.value != oldWidget.value) {
      _controller.text = widget.value.toString();
    }
  }

  @override
  void dispose() {
    _successTimer?.cancel();
    _controller.dispose();
    super.dispose();
  }

  void _submit() {
    FocusScope.of(context).unfocus();
    widget.onChanged(int.tryParse(_controller.text.trim()));
    setState(() => _showSuccess = true);
    _successTimer?.cancel();
    _successTimer = Timer(const Duration(seconds: 2), () {
      if (mounted) {
        setState(() => _showSuccess = false);
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final cs = theme.colorScheme;

    return MouseRegion(
      onEnter: (_) => setState(() => _hovering = true),
      onExit: (_) => setState(() => _hovering = false),
      child: SizedBox(
        width: 100,
        child: TextField(
          controller: _controller,
          keyboardType: TextInputType.number,
          textInputAction: TextInputAction.done,
          style: theme.textTheme.bodyMedium?.copyWith(
            fontFeatures: const [FontFeature.tabularFigures()],
          ),
          onSubmitted: (_) => _submit(),
          decoration: InputDecoration(
            filled: true,
            fillColor: cs.surfaceContainerHigh,
            isDense: true,
            hintStyle: theme.textTheme.bodySmall?.copyWith(
              color: cs.onSurfaceVariant,
            ),
            contentPadding: const EdgeInsets.symmetric(horizontal: kSpacingSmall, vertical: kSpacingSmall),
            border: OutlineInputBorder(
              borderRadius: BorderRadius.circular(8),
              borderSide: BorderSide.none,
            ),
            enabledBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(8),
              borderSide: BorderSide(
                color: cs.outlineVariant,
              ),
            ),
            focusedBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(8),
              borderSide: BorderSide(color: cs.primary),
            ),
            suffixIconConstraints: const BoxConstraints.tightFor(width: 32, height: 32),
            suffixIcon: SizedBox(
              width: 32,
              height: 32,
              child: AnimatedSwitcher(
                duration: const Duration(milliseconds: 200),
                switchInCurve: Curves.easeOut,
                switchOutCurve: Curves.easeIn,
                child: _showSuccess
                    ? Center(
                        key: const ValueKey('ok'),
                        child: Icon(
                          Icons.check_rounded,
                          size: kIconSizeMedium,
                          color: Colors.green.shade700,
                        ),
                      )
                    : _hovering
                    ? IconButton(
                        key: const ValueKey('go'),
                        padding: EdgeInsets.zero,
                        tooltip: '提交',
                        style: IconButton.styleFrom(
                          minimumSize: Size.zero,
                          tapTargetSize: MaterialTapTargetSize.shrinkWrap,
                          visualDensity: VisualDensity.compact,
                        ),
                        constraints: const BoxConstraints.tightFor(width: 32, height: 32),
                        onPressed: _submit,
                        icon: Icon(
                          Icons.task_alt_rounded,
                          size: kIconSizeMedium,
                          color: cs.onSurfaceVariant,
                        ),
                      )
                    : const SizedBox(key: ValueKey('empty')),
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class OverlayConfigItem extends OverlayMenuItem {
  OverlayConfigItem({
    super.key,
    required super.height,
    super.onTabSelected,
    required String title,
    String? description,
    required Widget trailing,
  }) : super(
         child: _OverlayConfigItemContent(
           title: title,
           description: description,
           trailing: trailing,
         ),
       );

  factory OverlayConfigItem.number({
    Key? key,
    required double height,
    required String title,
    String? description,
    required int value,
    required ValueChanged<int?> onChanged,
  }) {
    return OverlayConfigItem(
      key: key,
      height: height,
      title: title,
      description: description,
      trailing: _OverlayNumberTextField(
        value: value,
        onChanged: onChanged,
      ),
    );
  }

  factory OverlayConfigItem.checkbox({
    Key? key,
    required double height,
    required String title,
    String? description,
    required bool value,
    required ValueChanged<bool> onChanged,
  }) {
    return OverlayConfigItem(
      key: key,
      height: height,
      title: title,
      description: description,
      trailing: SizedBox(
        width: 100,
        child: Row(
          mainAxisAlignment: MainAxisAlignment.end,
          children: [
            SizedBox(
              width: 32,
              height: 32,
              child: Checkbox(
                value: value,
                onChanged: (v) {
                  if (v != null) onChanged(v);
                },
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _OverlayConfigItemContent extends StatelessWidget {
  const _OverlayConfigItemContent({
    required this.title,
    this.description,
    required this.trailing,
  });

  final String title;
  final String? description;
  final Widget trailing;

  bool get _hasDescription => description != null && description!.isNotEmpty;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: kSpacingSmall, horizontal: kSpacingMedium),
      child: Row(
        crossAxisAlignment: _hasDescription ? CrossAxisAlignment.start : CrossAxisAlignment.center,
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(title),
                if (_hasDescription) ...[
                  const SizedBox(height: kSpacingTiny),
                  Text(
                    description!,
                    style: Theme.of(context).textTheme.bodySmall?.copyWith(
                      color: Theme.of(context).colorScheme.onSurfaceVariant,
                    ),
                  ),
                ],
              ],
            ),
          ),
          const SizedBox(width: kSpacingTiny),
          trailing,
        ],
      ),
    );
  }
}

class OverlayMenuHeader extends StatelessWidget {
  final double height;
  final Widget child;

  const OverlayMenuHeader({super.key, required this.height, required this.child});

  factory OverlayMenuHeader.tile({
    Key? key,
    double height = 74,
    required IconData icon,
    required String title,
    String? subtitle,
  }) {
    return OverlayMenuHeader(
      key: key,
      height: height,
      child: Builder(
        builder: (context) {
          final theme = Theme.of(context);
          final cs = theme.colorScheme;
          final textTheme = theme.textTheme;
          return Padding(
            padding: const EdgeInsets.fromLTRB(kSpacingMedium, kSpacingMedium, kSpacingSmall, kSpacingSmall),
            child: Row(
              crossAxisAlignment: CrossAxisAlignment.center,
              children: [
                DecoratedBox(
                  decoration: BoxDecoration(
                    color: cs.primaryContainer,
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Padding(
                    padding: const EdgeInsets.all(kSpacingSmall),
                    child: Icon(
                      icon,
                      size: 20,
                      color: cs.onPrimaryContainer,
                    ),
                  ),
                ),
                const SizedBox(width: kSpacingSmall),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(title, style: textTheme.titleMedium),
                      if (subtitle != null && subtitle.isNotEmpty)
                        Text(
                          subtitle,
                          style: textTheme.bodySmall?.copyWith(color: cs.onSurfaceVariant),
                        ),
                    ],
                  ),
                ),
              ],
            ),
          );
        },
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return SizedBox(height: height, child: child);
  }
}

class OverlayMenuFooter extends StatefulWidget {
  final double height;
  final Widget child;
  final VoidCallback? onTap;

  const OverlayMenuFooter({super.key, required this.height, required this.child, this.onTap});

  @override
  State<OverlayMenuFooter> createState() => _OverlayMenuFooterState();
}

class _OverlayMenuFooterState extends State<OverlayMenuFooter> {
  bool _hovering = false;

  @override
  Widget build(BuildContext context) {
    final base = SizedBox(height: widget.height, child: widget.child);

    const radius = BorderRadius.only(bottomLeft: Radius.circular(12), bottomRight: Radius.circular(12));
    final hoverColor = Theme.of(context).colorScheme.surfaceContainerLow; // 菜单footer鼠标移入的颜色

    return MouseRegion(
      onEnter: (_) => setState(() => _hovering = true),
      onExit: (_) => setState(() => _hovering = false),
      child: GestureDetector(
        onTap: widget.onTap,
        child: Container(
          decoration: BoxDecoration(color: _hovering ? hoverColor : null, borderRadius: radius),
          child: base,
        ),
      ),
    );
  }
}
