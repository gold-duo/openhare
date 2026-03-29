import 'dart:async';

import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/scroll.dart';
import 'package:flutter/material.dart';
import 'package:flutter/rendering.dart';
import 'package:flutter/scheduler.dart';

/// 聊天列表自动滚动控制器
class ChatScrollController extends ChangeNotifier {
  final KeepOffestScrollController _scrollController = KeepOffestScrollController();
  final GlobalKey bottomAnchorKey = GlobalKey();
  static const int _maxEnsureVisibleAttempts = 4;
  static const Duration _resumeAutoScrollDelay = Duration(milliseconds: 200);
  static const double _showGoToBottomButtonThreshold = 120;

  // 是否允许后续消息更新继续自动吸底。
  bool _autoScrollEnabled = true;

  // 回到底部后，如果一段时间未再收到新的滚动请求，则恢复自动吸底。
  Timer? _resumeAutoScrollTimer;

  // 是否展示"回到底部"按钮。
  bool showGoToBottomButton = false;

  ScrollController get listScrollController => _scrollController;

  @override
  void dispose() {
    _resumeAutoScrollTimer?.cancel();
    _scrollController.dispose();
    super.dispose();
  }

  bool onScrollNotification(ScrollNotification notification) {
    if (notification is ScrollStartNotification && notification.dragDetails != null) {
      _onUserScrollRequest();
    } else if (notification is ScrollUpdateNotification && notification.dragDetails != null) {
      _onUserScrollRequest();
    } else if (notification is UserScrollNotification && notification.direction != ScrollDirection.idle) {
      _onUserScrollRequest();
    } else if (notification is ScrollEndNotification) {
      _scheduleResumeAutoScrollIfNeeded();
    }
    _syncGoToBottomButtonVisibility();
    return false;
  }

  /// 聊天内容变化时调用，仅在自动滚动开启时继续吸底。
  void onContentChanged() {
    if (!_autoScrollEnabled) return;
    _scheduleJumpToBottom();
  }

  /// 回到底部，并恢复自动滚动。
  void goToBottom() {
    _resumeAutoScrollTimer?.cancel();
    _setAutoScrollEnabled(true);
    _scheduleJumpToBottom();
  }

  void _setAutoScrollEnabled(bool value) {
    final autoScrollChanged = value != _autoScrollEnabled;
    _autoScrollEnabled = value;
    final buttonChanged = _syncGoToBottomButtonVisibility(notify: false);
    if (!autoScrollChanged && !buttonChanged) return;
    notifyListeners();
  }

  void _scheduleJumpToBottom() {
    SchedulerBinding.instance.addPostFrameCallback((_) {
      _jumpToBottom();
    });
  }

  /// 将列表滚到底部（锚点或 [maxScrollExtent]）。
  /// 仅在自动滚动开启时执行。
  ///
  /// [attempt]：布局未稳定时多帧重试 [Scrollable.ensureVisible] / 对齐，避免仍停在非底部。
  void _jumpToBottom({int attempt = 0}) {
    if (!_autoScrollEnabled) return;
    if (!_scrollController.hasClients || !_scrollController.position.hasContentDimensions) return;

    final anchorContext = bottomAnchorKey.currentContext;
    if (anchorContext != null) {
      Scrollable.ensureVisible(
        anchorContext,
        duration: Duration.zero,
        alignment: 1,
      );
    } else {
      _scrollController.jumpTo(_scrollController.position.maxScrollExtent);
    }

    if (attempt >= _maxEnsureVisibleAttempts) return;
    SchedulerBinding.instance.addPostFrameCallback((_) {
      if (!_autoScrollEnabled) return;
      if (!_isBottomAnchorVisible()) {
        _jumpToBottom(attempt: attempt + 1);
      }
    });
  }

  void _onUserScrollRequest() {
    _resumeAutoScrollTimer?.cancel();
    _setAutoScrollEnabled(false);
  }

  void _scheduleResumeAutoScrollIfNeeded() {
    _resumeAutoScrollTimer?.cancel();

    _resumeAutoScrollTimer = Timer(_resumeAutoScrollDelay, () {
      SchedulerBinding.instance.addPostFrameCallback((_) {
        if (!_isBottomAnchorVisible()) return;
        _setAutoScrollEnabled(true);
      });
    });
  }

  bool _isBottomAnchorVisible() {
    if (!_scrollController.hasClients || !_scrollController.position.hasContentDimensions) {
      return false;
    }

    final anchorContext = bottomAnchorKey.currentContext;
    if (anchorContext == null) return false;

    final renderObject = anchorContext.findRenderObject();
    if (renderObject is! RenderBox || !renderObject.attached) return false;

    final viewport = RenderAbstractViewport.maybeOf(renderObject);
    if (viewport == null) return false;

    final position = _scrollController.position;
    final viewportStart = position.pixels;
    final viewportEnd = viewportStart + position.viewportDimension;
    final anchorTop = viewport.getOffsetToReveal(renderObject, 0).offset;
    final anchorBottom = viewport.getOffsetToReveal(renderObject, 1).offset;

    return anchorBottom >= viewportStart && anchorTop <= viewportEnd;
  }

  double _distanceToBottom() {
    if (!_scrollController.hasClients || !_scrollController.position.hasContentDimensions) {
      return 0;
    }
    final position = _scrollController.position;
    return position.maxScrollExtent - position.pixels;
  }

  bool _syncGoToBottomButtonVisibility({bool notify = true}) {
    final shouldShow = !_autoScrollEnabled && _distanceToBottom() > _showGoToBottomButtonThreshold;
    if (shouldShow == showGoToBottomButton) return false;
    showGoToBottomButton = shouldShow;
    if (notify) {
      notifyListeners();
    }
    return true;
  }
}

class ChatListView extends StatelessWidget {
  final ChatScrollController chatScrollController;
  final int itemCount;
  final IndexedWidgetBuilder itemBuilder;
  final EdgeInsetsGeometry padding;
  final double bottomAnchorHeight;

  const ChatListView({
    super.key,
    required this.chatScrollController,
    required this.itemCount,
    required this.itemBuilder,
    this.padding = EdgeInsets.zero,
    this.bottomAnchorHeight = 100,
  });

  @override
  Widget build(BuildContext context) {
    return ListenableBuilder(
      listenable: chatScrollController,
      builder: (context, _) => Stack(
        children: [
          NotificationListener<ScrollNotification>(
            onNotification: chatScrollController.onScrollNotification,
            child: ListView.builder(
              controller: chatScrollController.listScrollController,
              physics: const ClampingScrollPhysics(),
              itemCount: itemCount + 1,
              padding: padding,
              itemBuilder: (context, index) {
                if (index == itemCount) {
                  return SizedBox(
                    key: chatScrollController.bottomAnchorKey,
                    height: bottomAnchorHeight,
                  );
                }
                return itemBuilder(context, index);
              },
            ),
          ),
          if (chatScrollController.showGoToBottomButton)
            Positioned(
              right: kSpacingMedium + 2, // +2 是为了与chat message 里的其他icon对齐
              bottom: kSpacingMedium,
              child: FloatingActionButton.small(
                heroTag: null,
                onPressed: chatScrollController.goToBottom,
                tooltip: AppLocalizations.of(context)!.button_tooltip_go_to_bottom,
                backgroundColor: Theme.of(context).colorScheme.surfaceContainer,
                child: Icon(Icons.keyboard_arrow_down, color: Theme.of(context).colorScheme.onSurface),
              ),
            ),
        ],
      ),
    );
  }
}
