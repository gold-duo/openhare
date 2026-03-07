import 'package:freezed_annotation/freezed_annotation.dart';

part 'version_info.freezed.dart';

enum UpdateCheckStatus {
  none,
  available,
  alreadyLatest,
  failed,
}

@freezed
abstract class VersionInfoModel with _$VersionInfoModel {
  const factory VersionInfoModel({
    @Default('') String version,
    @Default('-') String latestVersion,
    @Default(UpdateCheckStatus.none) UpdateCheckStatus updateStatus,
    @Default('') String updateError,
  }) = _VersionInfoModel;
}
