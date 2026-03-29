import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/services/sessions/session_metadata.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'session_sql_editor.g.dart';

@Riverpod(keepAlive: true)
class SelectedSessionSQLEditorNotifier extends _$SelectedSessionSQLEditorNotifier {
  @override
  SessionSQLEditorModel build() {
    SessionDetailModel? sessionDetailModel = ref.watch(selectedSessionDetailProvider);
    if (sessionDetailModel == null) {
      return const SessionSQLEditorModel(sessionId: SessionId(value: 0));
    }
    if (sessionDetailModel.instanceId != null) {
      AsyncValue<InstanceMetadataModel>? sessionMeta = ref.watch(
        selectedSessionMetadataProvider,
      );
      return SessionSQLEditorModel(
        sessionId: sessionDetailModel.sessionId,
        currentSchema: sessionDetailModel.currentSchema,
        dbType: sessionDetailModel.dbType,
        metadata: sessionMeta?.when(
          data: (data) => data.metadata,
          error: (error, trace) => null,
          loading: () => null,
        ),
      );
    }

    return SessionSQLEditorModel(
      sessionId: sessionDetailModel.sessionId,
      currentSchema: sessionDetailModel.currentSchema,
      dbType: sessionDetailModel.dbType,
      metadata: null,
    );
  }
}
