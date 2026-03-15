import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/repositories/instances/instances.dart';
// ignore: unnecessary_import
import 'package:objectbox/objectbox.dart'; // 必须引入, 不然objectbox不能正常使用
import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'package:client/repositories/repo.dart';
import 'package:client/utils/reorder_list.dart';

part 'sessions.g.dart';

@Entity()
class SessionStorage {
  @Id()
  int id;

  final instance = ToOne<InstanceStorage>();

  final code = ToOne<SessionCodeStorage>();

  String? currentSchema;

  SessionStorage({
    this.id = 0,
    this.currentSchema,
  });
}

@Entity()
class SessionCodeStorage {
  @Id()
  int id;
  String? text;

  SessionCodeStorage({
    this.id = 0,
    this.text,
  });
}

class SessionRepoImpl extends SessionRepo {
  final ObjectBox ob;
  final Box<SessionStorage> _sessionBox;
  final Box<SessionCodeStorage> _sessionCodeBox;
  ReorderSelectedList<SessionStorage>? _sessionCache;

  final Map<int, ConnId> _connIdMap = {};

  SessionRepoImpl(this.ob) : _sessionBox = ob.store.box(), _sessionCodeBox = ob.store.box();

  void _initSessionCache() {
    _sessionCache = ReorderSelectedList(data: _sessionBox.getAll());
  }

  ReorderSelectedList<SessionStorage> get _sessions {
    if (_sessionCache == null) {
      _initSessionCache();
    }
    return _sessionCache!;
  }

  SessionModel _toModel(SessionStorage session) {
    return SessionModel(
      sessionId: SessionId(value: session.id),
      instanceId: session.instance.hasValue ? InstanceId(value: session.instance.targetId) : null,
      currentSchema: session.currentSchema,
      connId: _connIdMap[session.id],
    );
  }

  @override
  SessionId newSession() {
    final sessionId = _sessionBox.put(SessionStorage());
    final session = _sessionBox.get(sessionId);
    // 更新缓存, 理论上不会为空
    if (session != null) {
      _sessions.add(session);
    }
    return SessionId(value: sessionId);
  }

  @override
  void updateSession(SessionId sessionId, {InstanceModel? instance, String? currentSchema}) {
    final session = _sessionBox.get(sessionId.value);
    if (session == null) {
      return;
    }
    if (instance != null) {
      session.instance.target = InstanceStorage.fromModel(instance);
    }
    if (currentSchema != null) {
      session.currentSchema = currentSchema;
    }

    _sessionBox.put(session);

    // 更新缓存
    final sessionCache = _getSession(sessionId);
    // 理论上不存在空的情况
    if (sessionCache != null) {
      _sessions.replace(sessionCache, session);
    }
  }

  @override
  void setConnId(SessionId sessionId, ConnId connId) {
    _connIdMap[sessionId.value] = connId;
  }

  @override
  void unsetConnId(SessionId sessionId) {
    _connIdMap.remove(sessionId.value);
  }

  @override
  void deleteSession(SessionId sessionId) {
    final session = _sessionBox.get(sessionId.value);
    if (session == null) {
      return;
    }
    if (session.code.hasValue) {
      _sessionCodeBox.remove(sessionId.value);
    }
    _sessionBox.remove(sessionId.value);

    // 从缓存中移除
    final sessionCache = _getSession(sessionId);
    if (sessionCache != null) {
      _sessions.removeAt(_sessions.indexOf(sessionCache));
    }
  }

  SessionStorage? _getSession(SessionId sessionId) {
    final session = _sessions.firstWhere(
      (s) => s.id == sessionId.value,
      orElse: () => SessionStorage(id: 0),
    );
    return session.id == 0 ? null : session;
  }

  @override
  SessionModel? getSession(SessionId sessionId) {
    final session = _getSession(sessionId);
    return session != null ? _toModel(session) : null;
  }

  @override
  SessionModel? seletedSession() {
    final session = _sessions.selected();
    return session != null ? _toModel(session) : null;
  }

  @override
  SessionListModel getSessions() {
    return SessionListModel(
      sessions: _sessions.map((s) {
        return _toModel(s);
      }).toList(),
    );
  }

  @override
  void selectSessionByIndex(int index) {
    _sessions.select(index);
  }

  @override
  void reorderSession(int oldIndex, int newIndex) {
    _sessions.reorder(oldIndex, newIndex);
  }

  SessionCodeStorage? _getCode(SessionId sessionId) {
    final session = _sessionBox.get(sessionId.value);
    if (session == null) {
      return null;
    }
    if (!session.code.hasValue) {
      final codeStorage = SessionCodeStorage();
      _sessionCodeBox.put(codeStorage);
      session.code.target = codeStorage;
      _sessionBox.put(session);
    }
    return session.code.target;
  }

  @override
  String? getCode(SessionId sessionId) {
    final codeStorage = _getCode(sessionId);
    if (codeStorage == null) {
      return null;
    }
    return codeStorage.text;
  }

  @override
  void saveCode(SessionId sessionId, String code) {
    final codeStorage = _getCode(sessionId);
    if (codeStorage == null) {
      return;
    }
    codeStorage.text = code;
    _sessionCodeBox.put(codeStorage);
  }
}

@Riverpod(keepAlive: true)
SessionRepo sessionRepo(Ref ref) {
  ObjectBox ob = ref.watch(objectboxProvider);
  return SessionRepoImpl(ob);
}
