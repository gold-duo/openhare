import 'package:client/models/version_info.dart';
import 'package:client/utils/version.dart';
import 'package:package_info_plus/package_info_plus.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';

part 'about.g.dart';

@Riverpod(keepAlive: true)
class VersionInfoService extends _$VersionInfoService {
  @override
  Future<VersionInfoModel> build() async {
    final packageInfo = await PackageInfo.fromPlatform();
    try {
      final latestRelease = await fetchLatestReleaseFromGitHub();
      final latestVersion = latestRelease.version;
      final hasNew =
          latestVersion.isNotEmpty &&
          packageInfo.version.isNotEmpty &&
          compareVersion(latestVersion, packageInfo.version) > 0;
      return VersionInfoModel(
        version: packageInfo.version,
        latestVersion: latestVersion,
        updateStatus: hasNew ? UpdateCheckStatus.available : UpdateCheckStatus.alreadyLatest,
      );
    } catch (e) {
      return VersionInfoModel(
        version: packageInfo.version,
        latestVersion: '-',
        updateStatus: UpdateCheckStatus.failed,
        updateError: e.toString(),
      );
    }
  }
}
