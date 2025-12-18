import 'package:client/models/instances.dart';
import 'package:client/repositories/instances/instances.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'instances.g.dart';

@Riverpod(keepAlive: true)
class InstancesServices extends _$InstancesServices {
  @override
  int build() {
    return 1;
  }

  void _invalidateSelf() {
    state++;
  }

  void addInstance(InstanceModel instance) {
    final repo = ref.read(instanceRepoProvider);
    repo.add(instance);
    _invalidateSelf();
  }

  InstanceModel? getInstanceById(InstanceId instanceId) {
    final repo = ref.read(instanceRepoProvider);
    return repo.getInstanceById(instanceId);
  }

  void updateInstance(InstanceModel instance) {
    final repo = ref.read(instanceRepoProvider);
    repo.update(instance);
    _invalidateSelf();
  }

  void deleteInstance(InstanceId id) {
    final repo = ref.read(instanceRepoProvider);
    repo.delete(id);
    _invalidateSelf();
  }

  bool isInstanceExist(String name) {
    final repo = ref.read(instanceRepoProvider);
    return repo.isInstanceExist(name);
  }

  InstanceListModel instances(String key, {int? pageNumber, int? pageSize}) {
    final repo = ref.read(instanceRepoProvider);
    return repo.isntances(key, pageNumber: pageNumber, pageSize: pageSize);
  }

  Future<void> addActiveInstance(InstanceId instanceId, {String? schema}) async {
    await ref.read(instanceRepoProvider).addActiveInstance(instanceId);
    if (schema != null) {
      await ref.read(instanceRepoProvider).addInstanceActiveSchema(instanceId, schema);
      _invalidateSelf();
    }
  }

  List<InstanceModel> activeInstances() {
    final repo = ref.read(instanceRepoProvider);
    return repo.getActiveInstances(5);
  }

  Future<List<String>> getSchemas(InstanceId instanceId) async {
    final repo = ref.read(instanceRepoProvider);
    return await repo.getSchemas(instanceId);
  }
}

@Riverpod(keepAlive: true)
class InstancesNotifier extends _$InstancesNotifier {
  @override
  PaginationInstanceListModel build() {
    ref.watch(instancesServicesProvider);
    return instances("", pageNumber: 1, pageSize: 10);
  }

  PaginationInstanceListModel instances(String key, {int pageNumber = 1, int pageSize = 10}) {
    final instances = ref.read(instancesServicesProvider.notifier).instances(
          key,
          pageNumber: pageNumber,
          pageSize: pageSize,
        );
    return PaginationInstanceListModel(
      instances: instances,
      key: key,
      currentPage: pageNumber,
      pageSize: pageSize,
    );
  }

  void changePage(String key, {int pageNumber = 1, int pageSize = 10}) {
    state = instances(key, pageNumber: pageNumber, pageSize: pageSize);
  }
}
