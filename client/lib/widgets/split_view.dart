import 'package:flutter/material.dart';

/// 分割视图控制器
/// first 区域总是自适应（Expanded），second 区域固定大小（可拖拽调整）
class SplitViewController extends ChangeNotifier {
  final double firstMinSize; // first 区域的最小尺寸
  final double secondMinSize; // second 区域的最小尺寸

  double _secondSize; // 当前 second 区域的大小（拖拽时可能改变）

  SplitViewController({
    required double secondSize,
    this.firstMinSize = 0,
    this.secondMinSize = 0,
  }) : _secondSize = secondSize;

  /// 获取第二个区域的当前大小
  double get secondSize => _secondSize;

  /// 设置第二个区域的大小（带边界检查）
  /// [size] 要设置的大小值
  /// [maxSize] 最大允许的大小（通常是 totalSize - dividerThickness - firstMinSize）
  void setSecondSize(double size, double? maxSize) {
    final double clampedSize = maxSize != null
        ? size.clamp(secondMinSize, maxSize)
        : size.clamp(secondMinSize, double.infinity);
    if (_secondSize == clampedSize) return;
    _secondSize = clampedSize;
    notifyListeners();
  }
}

/// 分割视图组件
class SplitView extends StatefulWidget {
  final SplitViewController controller;
  final Widget first;
  final Widget second;
  final Axis axis;

  const SplitView({
    super.key,
    required this.controller,
    required this.first,
    required this.second,
    this.axis = Axis.horizontal,
  });

  @override
  State<SplitView> createState() => _SplitViewState();
}

class _SplitViewState extends State<SplitView> {
  static const double _dividerThickness = 5.0;
  bool _isDragging = false; // 是否正在拖拽，用于控制 IgnorePointer
  double? _currentTotalSize; // 保存当前的总尺寸，用于拖拽时计算

  // 重复计算的抽取
  bool get _isHorizontal => widget.axis == Axis.horizontal;

  /// 计算总尺寸（根据轴方向从约束中提取）
  double _calculateTotalSize(BoxConstraints constraints) {
    return _isHorizontal ? constraints.maxWidth : constraints.maxHeight;
  }

  /// 计算第二个区域的最大尺寸
  double _calculateMaxSecondSize(double totalSize) {
    return totalSize - _dividerThickness - widget.controller.firstMinSize;
  }

  void _onPanStart(DragStartDetails details) {
    setState(() {
      _isDragging = true;
    });
  }

  void _onPanUpdate(DragUpdateDetails details) {
    if (_currentTotalSize == null || _currentTotalSize! <= 0) return;

    final double delta = _isHorizontal ? details.delta.dx : details.delta.dy;

    // 拖拽 second：向右拖 second 变小（delta 为正，size 要减）
    double newSecondSize = widget.controller.secondSize - delta;
    final double maxSecondSize = _calculateMaxSecondSize(_currentTotalSize!);
    // 使用 setSecondSize，会自动触发通知，_onControllerChanged 会调用 setState 更新 UI
    widget.controller.setSecondSize(newSecondSize, maxSecondSize);
  }

  void _onPanEnd(DragEndDetails details) {
    setState(() {
      _isDragging = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: widget.controller,
      builder: (context, _) {
        return LayoutBuilder(
          builder: (context, constraints) {
            final double totalSize = _calculateTotalSize(constraints);
            if (totalSize <= 0) return const SizedBox.shrink();

            // 保存当前的总尺寸，供拖拽时使用
            _currentTotalSize = totalSize;

            final children = [
              Expanded(
                child: RepaintBoundary(
                  child: IgnorePointer(
                    ignoring: _isDragging,
                    child: widget.first,
                  ),
                ),
              ),
              _buildDivider(),
              SizedBox(
                width: _isHorizontal ? widget.controller.secondSize : null,
                height: _isHorizontal ? null : widget.controller.secondSize,
                child: RepaintBoundary(
                  child: IgnorePointer(
                    ignoring: _isDragging,
                    child: widget.second,
                  ),
                ),
              ),
            ];

            return _isHorizontal ? Row(children: children) : Column(children: children);
          },
        );
      },
    );
  }

  Widget _buildDivider() {
    return RepaintBoundary(
      child: GestureDetector(
        behavior: HitTestBehavior.opaque, // 阻止事件穿透到底层
        onPanStart: _onPanStart,
        onPanUpdate: _onPanUpdate,
        onPanEnd: _onPanEnd,
        child: SizedBox(
          width: _isHorizontal ? _dividerThickness : double.infinity,
          height: _isHorizontal ? double.infinity : _dividerThickness,
          child: MouseRegion(
            cursor: _isHorizontal ? SystemMouseCursors.resizeColumn : SystemMouseCursors.resizeRow,
            child: const SizedBox.expand(),
          ),
        ),
      ),
    );
  }
}
