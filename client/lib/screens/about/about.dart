import 'package:client/l10n/app_localizations.dart';
import 'package:client/models/version_info.dart';
import 'package:client/screens/page_skeleton.dart';
import 'package:client/services/about/about.dart';
import 'package:client/utils/version.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/divider.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:url_launcher/url_launcher.dart';

class AboutPage extends ConsumerWidget {
  const AboutPage({super.key});

  static final Uri _githubRepoUrl = Uri.parse('https://github.com/sjjian/openhare');
  static final Uri _giteeRepoUrl = Uri.parse('https://gitee.com/sjjian/openhare');
  static final Uri _websiteUrl = Uri.parse('https://sjjian.github.io/openhare/');

  Future<void> _launchUrl(Uri url) async {
    if (!await launchUrl(url)) {
      throw Exception('Could not launch $url');
    }
  }

  void _checkUpdate(WidgetRef ref) {
    ref.invalidate(versionInfoServiceProvider);
  }

  String _updateMessage(AppLocalizations l10n, VersionInfoModel model) {
    if (model.updateStatus == UpdateCheckStatus.failed) {
      return l10n.about_update_check_failed;
    }

    if (model.latestVersion == '-' || model.version.isEmpty) {
      return '';
    }

    return compareVersion(model.latestVersion, model.version) > 0
        ? l10n.about_update_available
        : l10n.about_update_already_latest;
  }

  Widget _buildSectionHeader(BuildContext context, String title) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          title,
          style: Theme.of(context).textTheme.titleMedium,
        ),
        const SizedBox(height: kSpacingSmall),
        PixelDivider(),
      ],
    );
  }

  Widget _buildUpdateMessageChip(
    BuildContext context,
    AppLocalizations l10n,
    String message, {
    String? errorDetail,
  }) {
    Color? backgroundColor;
    if (message == l10n.about_update_available) {
      backgroundColor = Colors.green.withValues(alpha: 0.15);
    } else if (message == l10n.about_update_check_failed) {
      backgroundColor = Colors.red.withValues(alpha: 0.15);
    }
    final chip = Container(
      padding: const EdgeInsets.symmetric(
        horizontal: kSpacingSmall,
        vertical: 2,
      ),
      decoration: BoxDecoration(
        color: backgroundColor,
        border: Border.all(
          color: Theme.of(context).colorScheme.outlineVariant,
        ),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Text(
        message,
        style: Theme.of(context).textTheme.bodySmall,
      ),
    );
    if (message == l10n.about_update_check_failed &&
        errorDetail != null &&
        errorDetail.isNotEmpty) {
      return Tooltip(
        message: errorDetail,
        preferBelow: false,
        child: chip,
      );
    }
    return chip;
  }

  Widget _buildExternalLinksSection(BuildContext context, AppLocalizations l10n) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildSectionHeader(context, l10n.about_more_info),
        const SizedBox(height: kSpacingMedium),
        Row(
          children: [
            LinkButton(
              text: "GitHub",
              padding: EdgeInsets.only(right: kSpacingSmall),
              onPressed: () => _launchUrl(_githubRepoUrl),
            ),
            LinkButton(
              text: "Gitee",
              onPressed: () => _launchUrl(_giteeRepoUrl),
            ),
            LinkButton(
              text: "Website",
              onPressed: () => _launchUrl(_websiteUrl),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildVersionActions(
    WidgetRef ref,
    AppLocalizations l10n,
    bool isLoading,
  ) {
    return Row(
      children: [
        OutlinedButton.icon(
          onPressed: isLoading ? null : () => _checkUpdate(ref),
          icon: SizedBox(
            width: 18,
            height: 18,
            child: isLoading
                ? const CircularProgressIndicator(strokeWidth: 2)
                : const Icon(Icons.refresh, size: 18),
          ),
          label: Text(l10n.about_check_update),
        ),
        const SizedBox(width: kSpacingSmall),
        OutlinedButton.icon(
          onPressed: () => _launchUrl(githubLatestReleaseUrl),
          icon: const Icon(Icons.download),
          label: const Text('GitHub'),
        ),
        const SizedBox(width: kSpacingSmall),
        OutlinedButton.icon(
          onPressed: () => _launchUrl(giteeLatestReleaseUrl),
          icon: const Icon(Icons.download),
          label: const Text('Gitee'),
        ),
      ],
    );
  }

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final l10n = AppLocalizations.of(context)!;
    final asyncVersion = ref.watch(versionInfoServiceProvider);
    final isRefreshing = asyncVersion.isRefreshing || asyncVersion.isReloading;

    return PageSkeleton(
      key: const Key("about"),
      child: BodyPageSkeleton(
        header: Row(
          children: [
            Text(
              l10n.about,
              style: Theme.of(context).textTheme.titleLarge,
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              l10n.app_desc,
              style: Theme.of(context).textTheme.bodyLarge,
            ),
            const SizedBox(height: kSpacingMedium),
            asyncVersion.when(
              data: (model) => _buildVersionSection(
                context,
                ref,
                l10n,
                model,
                isLoading: isRefreshing,
              ),
              loading: () => _buildVersionSectionLoading(context, ref, l10n),
              error: (e, _) => _buildVersionSectionError(context, ref, l10n, e),
            ),
            const SizedBox(height: kSpacingMedium),
            _buildExternalLinksSection(context, l10n),
          ],
        ),
      ),
    );
  }

  Widget _buildVersionSection(
    BuildContext context,
    WidgetRef ref,
    AppLocalizations l10n,
    VersionInfoModel model, {
    bool isLoading = false,
  }) {
    final updateMessage = isLoading ? '' : _updateMessage(l10n, model);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildSectionHeader(context, l10n.about_version_info),
        const SizedBox(height: kSpacingMedium),
        Row(
          crossAxisAlignment: CrossAxisAlignment.baseline,
          textBaseline: TextBaseline.alphabetic,
          children: [
            Text(
              l10n.about_current_version(model.version),
              style: Theme.of(context).textTheme.bodyLarge,
            ),
            if (updateMessage.isNotEmpty) ...[
              const SizedBox(width: kSpacingMedium),
              _buildUpdateMessageChip(
                context,
                l10n,
                updateMessage,
                errorDetail: model.updateError,
              ),
            ],
          ],
        ),
        const SizedBox(height: kSpacingSmall),
        Text(
          l10n.about_latest_version(model.latestVersion),
          style: Theme.of(context).textTheme.bodyLarge,
        ),
        const SizedBox(height: kSpacingMedium),
        _buildVersionActions(ref, l10n, isLoading),
      ],
    );
  }

  Widget _buildVersionSectionLoading(
    BuildContext context,
    WidgetRef ref,
    AppLocalizations l10n,
  ) {
    const placeholder = VersionInfoModel(version: '-', latestVersion: '-');
    return _buildVersionSection(
      context,
      ref,
      l10n,
      placeholder,
      isLoading: true,
    );
  }

  Widget _buildVersionSectionError(
    BuildContext context,
    WidgetRef ref,
    AppLocalizations l10n,
    Object error,
  ) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _buildSectionHeader(context, l10n.about_version_info),
        const SizedBox(height: kSpacingMedium),
        Text(
          error.toString(),
          style: Theme.of(context).textTheme.bodySmall?.copyWith(
                color: Theme.of(context).colorScheme.error,
              ),
        ),
        const SizedBox(height: kSpacingSmall),
        OutlinedButton(
          onPressed: () => _checkUpdate(ref),
          child: Text(l10n.about_check_update),
        ),
      ],
    );
  }
}
