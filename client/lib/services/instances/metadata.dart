import 'package:client/models/instances.dart';
import 'package:client/repositories/instances/instances.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'metadata.g.dart';

@Riverpod()
class InstanceMetadataServices extends _$InstanceMetadataServices {
  @override
  Future<InstanceMetadataModel> build(InstanceId instanceId) {
    final metadataModel =
        ref.watch(instanceRepoProvider).getMetadata(instanceId);
    return metadataModel;
  }

  Future<void> refreshMetadata() async {
    await ref.read(instanceRepoProvider).refreshMetadata(instanceId);
    ref.invalidateSelf();
  }
}
