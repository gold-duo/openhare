import 'package:client/models/instances.dart';
import 'package:client/repositories/instances/session_conn.dart';
import 'package:objectbox/objectbox.dart';
import 'package:client/repositories/objectbox.g.dart';
import 'package:client/repositories/repo.dart';
import 'package:client/utils/active_set.dart';
import 'package:db_driver/db_driver.dart';
import 'dart:convert';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'instances.g.dart';

@Entity()
class InstanceStorage {
  @Id()
  int id;

  @Transient()
  DatabaseType dbType;

  int get stDbType => dbType.index;

  set stDbType(int value) {
    dbType = DatabaseType.values[value];
  }

  String name;
  String host;
  int? port;
  String user;
  String password;
  String desc;

  /// 使用字符串类型存储 custom 的 json 字符串， 使用时进行 json 解析。 原因：ObjectBox 不支持 json 类型。
  /// 本来使用的 @Transient 注解加自定义类型，但是更新后好像失效，弃用了。
  String customJson;
  
  @Transient()
  Map<String, String> get custom {
    if (customJson.isEmpty) {
      return {};
    }
    return Map<String, String>.from(jsonDecode(customJson));
  }

  List<String> initQuerys;

  @Property(type: PropertyType.dateNano)
  DateTime createdAt;

  @Property(type: PropertyType.dateNano)
  DateTime latestOpenAt;

  @Transient()
  ConnectValue get connectValue => ConnectValue(
      name: name,
      host: host,
      port: port,
      user: user,
      password: password,
      desc: desc,
      custom: custom,
      initQuerys: initQuerys);

  @Transient()
  ActiveSet<String> activeSchemas;
  List<String> get stActiveSchemas => activeSchemas.toList();

  set stActiveSchemas(List<String> value) {
    activeSchemas = ActiveSet<String>(value);
  }

  InstanceStorage({
    this.id = 0,
    required int stDbType,
    required this.name,
    required this.host,
    this.port,
    required this.user,
    required this.password,
    required this.desc,
    required this.customJson,
    required this.initQuerys,
    ActiveSet<String>? activeSchemas,
    DateTime? createdAt,
    DateTime? latestOpenAt,
  })  : activeSchemas = activeSchemas ?? ActiveSet<String>(List.empty()),
        dbType = DatabaseType.values[stDbType],
        createdAt = createdAt ?? DateTime.now(),
        latestOpenAt = latestOpenAt ?? DateTime(1970, 1, 1); //latestOpenAt 默认值未很早之前的时间

  InstanceStorage.fromModel(InstanceModel model)
      : id = model.id.value,
        dbType = model.dbType,
        name = model.name,
        host = model.host,
        port = model.port,
        user = model.user,
        password = model.password,
        desc = model.desc,
        customJson = jsonEncode(model.custom),
        initQuerys = model.initQuerys,
        activeSchemas = ActiveSet<String>(model.activeSchemas),
        createdAt = model.createdAt,
        latestOpenAt = model.latestOpenAt; 

  InstanceModel toModel() => InstanceModel(
        id: InstanceId(value: id),
        dbType: dbType,
        name: name,
        host: host,
        port: port,
        user: user,
        password: password,
        desc: desc,
        custom: custom,
        initQuerys: initQuerys,
        activeSchemas: activeSchemas.toList(),
        createdAt: createdAt,
        latestOpenAt: latestOpenAt,
      );
}

class InstanceRepoImpl extends InstanceRepo {
  final ObjectBox ob;
  final Box<InstanceStorage> _instanceBox;

  Map<InstanceId, InstanceMetadataModel> metadataCache = {};

  InstanceRepoImpl(this.ob) : _instanceBox = ob.store.box();

  @override
  void add(InstanceModel instance) {
    _instanceBox.put(InstanceStorage.fromModel(instance));
  }

  @override
  void update(InstanceModel instance) {
    _instanceBox.put(InstanceStorage.fromModel(instance));
    metadataCache.remove(instance.id);
  }

  @override
  void delete(InstanceId id) {
    _instanceBox.remove(id.value);
    metadataCache.remove(id);
  }

  @override
// todo: aync
  bool isInstanceExist(String name) {
    final instance = getInstanceByName(name);
    return instance != null;
  }

  @override
// todo: aync
  InstanceModel? getInstanceByName(String name) {
    final build = _instanceBox.query(InstanceStorage_.name.equals(name)).build();
    return build.findFirst()?.toModel();
  }

  @override
// todo: 替换 getInstance
  InstanceModel? getInstanceById(InstanceId id) {
    return _instanceBox.get(id.value)?.toModel();
  }

  @override
  InstanceListModel isntances(String key, {int? pageNumber, int? pageSize}) {
    final sanitizedKey = key.trim();
    Condition<InstanceStorage>? condition;

    if (sanitizedKey.isNotEmpty) {
      condition = InstanceStorage_.name.contains(sanitizedKey, caseSensitive: false);
    }

    // 统计总数
    final countQuery = _instanceBox.query(condition).build();
    final totalCount = countQuery.count();
    countQuery.close();

    // 分页参数
    final currentPage = (pageNumber != null && pageNumber > 0) ? pageNumber : 1;
    final size = (pageSize != null && pageSize > 0) ? pageSize : 10;
    final offset = (currentPage - 1) * size;

    // 获取分页数据（按创建时间倒序）
    final dataQuery = _instanceBox.query(condition).order(InstanceStorage_.createdAt, flags: Order.descending).build();
    dataQuery.limit = size;
    dataQuery.offset = offset;

    final instanceList = dataQuery.find();
    dataQuery.close();

    return InstanceListModel(
      instances: instanceList.map((e) => e.toModel()).toList(),
      count: totalCount,
    );
  }

  @override
  List<InstanceModel> getActiveInstances(int top) {
    final build = _instanceBox
        .query(InstanceStorage_.latestOpenAt.notNull())
        .order(InstanceStorage_.latestOpenAt, flags: Order.descending)
        .build();
    build.limit = top;
    return build.find().map((e) => e.toModel()).toList();
  }

  @override
  void addActiveInstance(InstanceId id) {
    final instance = _instanceBox.get(id.value);
    if (instance == null) {
      return;
    }
    instance.latestOpenAt = DateTime.now();
    _instanceBox.put(instance);
    return;
  }

  @override
  void addInstanceActiveSchema(InstanceId id, String schema) {
    final instance = _instanceBox.get(id.value);
    if (instance == null) {
      return;
    }
    instance.activeSchemas.add(schema);
    _instanceBox.put(instance);
    return;
  }

  @override
  Future<List<String>> getSchemas(InstanceId instanceId) async {
    final instance = _instanceBox.get(instanceId.value);
    if (instance == null) {
      return List.empty();
    }
    final model = metadataCache[instanceId];

    if (model == null) {
      return List.empty();
    }
    final schemas = List<String>.empty(growable: true);
    for (final meta in model.metadata) {
      meta.visitor((node, parent) {
        if (node.type == MetaType.schema) {
          schemas.add(node.value);
        }
        return true;
      });
    }
    return schemas;
  }

  Future<InstanceMetadataModel> _getMetadata(InstanceModel instance) async {
    SessionConn? conn;
    try {
      conn = SessionConn(model: instance);
      await conn.connect();
      final metadataNode = await conn.metadata();
      final version = await conn.version();
      return InstanceMetadataModel(metadata: metadataNode, version: version);
    } catch (e) {
      rethrow;
    } finally {
      if (conn != null) {
        await conn.close();
      }
    }
  }

  @override
  Future<InstanceMetadataModel> getMetadata(InstanceId instanceId) async {
    final metadata = metadataCache[instanceId];
    if (metadata != null) {
      return metadata;
    }
    final instance = _instanceBox.get(instanceId.value);
    if (instance == null) {
      throw Exception("Instance not found");
    }
    final newMetadata = await _getMetadata(instance.toModel());
    metadataCache[instanceId] = newMetadata;
    return newMetadata;
  }

  @override
  Future<void> refreshMetadata(InstanceId instanceId) async {
    final instance = _instanceBox.get(instanceId.value);
    if (instance == null) {
      throw Exception("Instance not found");
    }
    final newMetadata = await _getMetadata(instance.toModel());
    metadataCache[instanceId] = newMetadata;
  }
}

@Riverpod(keepAlive: true)
InstanceRepo instanceRepo(Ref ref) {
  ObjectBox ob = ref.watch(objectboxProvider);
  return InstanceRepoImpl(ob);
}
