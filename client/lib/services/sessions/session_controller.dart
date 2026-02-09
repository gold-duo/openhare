// 报存与session 有关的所有controller, 没想好怎么处理他们，感觉用riverpod 不太好。先自己处理吧

import 'package:client/models/sessions.dart';
import 'package:client/widgets/data_grid.dart';
import 'package:client/widgets/split_view.dart';
import 'package:flutter/material.dart';
import 'package:client/widgets/scroll.dart';
import 'package:sql_editor/re_editor.dart';
import 'package:client/widgets/mention_text.dart';

class SessionController {
  // split
  final SplitViewController multiSplitViewCtrl;
  final SplitViewController metaDataSplitViewCtrl;

  // sql editor
  final CodeScrollController sqlEditorScrollController;

  // ai chat
  final MentionTextEditingController chatInputController;
  final TextEditingController aiChatSearchTextController;
  final TextEditingController aiChatModelSearchTextController;
  final KeepOffestScrollController aiChatScrollController;

  // drawer
  final KeepOffestScrollController metadataTreeScrollController;

  SessionController({
    required this.multiSplitViewCtrl,
    required this.metaDataSplitViewCtrl,
    required this.aiChatSearchTextController,
    required this.aiChatModelSearchTextController,
    required this.chatInputController,
    required this.aiChatScrollController,
    required this.sqlEditorScrollController,
    required this.metadataTreeScrollController,
  });

  static Map<SessionId, SessionController> cache = {};

  static SessionController sessionController(SessionId sessionId) {
    if (cache.containsKey(sessionId)) {
      return cache[sessionId]!;
    }
    final controller = SessionController(
      multiSplitViewCtrl: SplitViewController(secondSize: 500, firstMinSize: 100, secondMinSize: 140),
      metaDataSplitViewCtrl: SplitViewController(secondSize: 400, firstMinSize: 140, secondMinSize: 360),
      // sql editor
      sqlEditorScrollController: CodeScrollController(
        verticalScroller: KeepOffestScrollController(),
        horizontalScroller: KeepOffestScrollController(),
      ),
      // ai chat
      aiChatSearchTextController: TextEditingController(),
      aiChatModelSearchTextController: TextEditingController(),
      chatInputController: MentionTextEditingController(),
      aiChatScrollController: KeepOffestScrollController(),

      // drawer
      metadataTreeScrollController: KeepOffestScrollController(),
    );
    cache[sessionId] = controller;
    return controller;
  }

  static void removeSessionController(SessionId sessionId) {
    if (cache.containsKey(sessionId)) {
      cache[sessionId]!.multiSplitViewCtrl.dispose();
      cache[sessionId]!.metaDataSplitViewCtrl.dispose();
      // sql editor
      cache[sessionId]!.sqlEditorScrollController.verticalScroller.dispose();
      cache[sessionId]!.sqlEditorScrollController.horizontalScroller.dispose();
      // ai chat
      cache[sessionId]!.aiChatSearchTextController.dispose();
      cache[sessionId]!.aiChatModelSearchTextController.dispose();
      cache[sessionId]!.chatInputController.dispose();
      cache[sessionId]!.aiChatScrollController.dispose();
      // drawer
      cache[sessionId]!.metadataTreeScrollController.dispose();
      // remove cache
      cache.remove(sessionId);
    }
  }
}

class SQLResultController {
  final DataGridController controller;

  /// 表格滚动控制器
  final KeepOffestLinkedScrollControllerGroup horizontalScrollGroup;
  final KeepOffestLinkedScrollControllerGroup verticalScrollGroup;

  SQLResultController({
    required this.controller,
    required this.horizontalScrollGroup,
    required this.verticalScrollGroup,
  });

  static Map<ResultId, SQLResultController> cache = {};

  // 使用init回调，如果存在则跳过初始化
  static SQLResultController sqlResultController(ResultId resultId, DataGridController Function() init) {
    if (cache.containsKey(resultId)) {
      return cache[resultId]!;
    }
    final controller = SQLResultController(
      controller: init(),
      horizontalScrollGroup: KeepOffestLinkedScrollControllerGroup(),
      verticalScrollGroup: KeepOffestLinkedScrollControllerGroup(),
    );
    cache[resultId] = controller;
    return controller;
  }

  static void removeSQLResultController(ResultId resultId) {
    if (cache.containsKey(resultId)) {
      cache[resultId]!.controller.dispose();
      cache.remove(resultId);
    }
  }
}
