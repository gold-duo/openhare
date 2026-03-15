import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/widgets/data_tree.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter_fancy_tree_view/flutter_fancy_tree_view.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'session_metadata.g.dart';

@Riverpod(keepAlive: true)
class SelectedSessionMetadataNotifier extends _$SelectedSessionMetadataNotifier {
  @override
  Future<InstanceMetadataModel> build() async {
      SessionModel? sessionModel = ref.watch(selectedSessionProvider);
    if (sessionModel == null || sessionModel.instanceId == null) {
      throw Exception("Session not found");
    }
    return await ref.read(instancesServicesProvider.notifier).getMetadata(sessionModel.instanceId!);
  }

  Future<void> refreshMetadata() async {
    SessionModel? sessionModel = ref.watch(selectedSessionProvider);
    if (sessionModel == null || sessionModel.instanceId == null) {
      throw Exception("Session not found");
    }
    state = const AsyncValue.loading();
    await ref.read(instancesServicesProvider.notifier).refreshMetadata(sessionModel.instanceId!);
    ref.invalidateSelf();
  }
}

@Riverpod(keepAlive: true)
class SelectedSessionMetadataTreeNotifier extends _$SelectedSessionMetadataTreeNotifier {
  @override
  Future<SessionMetadataTreeModel> build() async {
    SessionModel? sessionModel = ref.watch(selectedSessionProvider);
    if (sessionModel == null || sessionModel.instanceId == null) {
      throw Exception("Session not found");
    }
    
    final metadataModel = await ref.watch(selectedSessionMetadataProvider.future);

    List<MetaDataNode> items = metadataModel.metadata;
    RootNode root = RootNode();
    final metadataController = TreeController<DataNode>(
      roots: buildMetadataTree(root, items).children,
      childrenProvider: (DataNode node) => node.children,
    );

    root.visitor((node) {
      // 默认打开 SchemaNode
      if (node is SchemaNode) {
        metadataController.setExpansionState(node, true);
      }
      // 默认打开 currentSchema 对应的节点
      if (node is SchemaValueNode && node.name == sessionModel.currentSchema) {
        metadataController.setExpansionState(node, true);
      }
      // 默认打开所有table 节点
      if (node is TableNode) {
        metadataController.setExpansionState(node, true);
      }
      // 默认打开所有column 节点
      if (node is ColumnNode) {
        metadataController.setExpansionState(node, true);
      }
      return true;
    });
    return SessionMetadataTreeModel(
      sessionId: sessionModel.sessionId,
      metadataTreeCtrl: metadataController,
    );
  }
}

// schema 
@Riverpod(keepAlive: true)
class SelectedSessionSchemaNotifier extends _$SelectedSessionSchemaNotifier {
  @override
  Future<List<String>> build() async {
    SessionModel? sessionModel = ref.watch(selectedSessionProvider);
    if (sessionModel == null || sessionModel.instanceId == null) {
      throw Exception("Session not found");
    }
    final metadataModel = await ref.watch(selectedSessionMetadataProvider.future);
    return metadataModel.schemas;
  }
}