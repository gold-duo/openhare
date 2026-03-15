import 'dart:convert';

import 'package:http/http.dart' as http;

class GitHubLatestRelease {
  final String version;

  const GitHubLatestRelease({
    required this.version,
  });
}

final Uri githubLatestReleaseUrl = Uri.parse('https://github.com/sjjian/openhare/releases');
final Uri giteeLatestReleaseUrl = Uri.parse('https://gitee.com/sjjian/openhare/releases');

final Uri _githubLatestReleaseApi = Uri.parse('https://api.github.com/repos/sjjian/openhare/releases/latest');
final Uri _giteeLatestReleaseApi = Uri.parse('https://gitee.com/api/v5/repos/sjjian/openhare/releases/latest');

const Map<String, String> _githubHeaders = {
  'Accept': 'application/vnd.github+json',
};

Future<GitHubLatestRelease> _fetchLatestRelease(Uri api, {Map<String, String>? headers}) async {
  final response = await http.get(api, headers: headers);
  if (response.statusCode != 200) {
    throw Exception('HTTP ${response.statusCode} for $api');
  }
  final Map<String, dynamic> data = jsonDecode(response.body) as Map<String, dynamic>;
  final version = (data['tag_name'] ?? '').toString();
  if (version.isEmpty) {
    throw Exception('Invalid release payload from $api');
  }
  return GitHubLatestRelease(version: version);
}

Future<GitHubLatestRelease> fetchLatestReleaseFromGitHub() async {
  Object? lastError;
  for (int i = 0; i < 2; i++) {
    try {
      return await _fetchLatestRelease(_githubLatestReleaseApi, headers: _githubHeaders);
    } catch (e) {
      lastError = e;
    }
    try {
      return await _fetchLatestRelease(_giteeLatestReleaseApi);
    } catch (e) {
      lastError = e;
    }
  }
  throw Exception('Failed to fetch latest release from GitHub and Gitee: $lastError');
}

List<int> parseVersion(String raw) {
  final normalized = raw.trim().replaceFirst(RegExp(r'^[vV]'), '').split('+').first;
  final semverMatch = RegExp(r'(\d+)\.(\d+)\.(\d+)').firstMatch(normalized);
  if (semverMatch != null) {
    return [
      int.parse(semverMatch.group(1)!),
      int.parse(semverMatch.group(2)!),
      int.parse(semverMatch.group(3)!),
    ];
  }

  final numericParts = RegExp(r'\d+').allMatches(normalized).map((m) => int.parse(m.group(0)!)).toList(growable: false);
  if (numericParts.isEmpty) {
    return const [0];
  }
  return numericParts;
}

int compareVersion(String a, String b) {
  final pa = parseVersion(a);
  final pb = parseVersion(b);
  final maxLen = pa.length > pb.length ? pa.length : pb.length;
  for (int i = 0; i < maxLen; i++) {
    final va = i < pa.length ? pa[i] : 0;
    final vb = i < pb.length ? pb[i] : 0;
    if (va != vb) {
      return va.compareTo(vb);
    }
  }
  return 0;
}
