import 'package:client/models/instances.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/widgets/dialog.dart';
import 'package:flutter/material.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:db_driver/db_driver.dart';
import 'package:client/screens/instances/instance_add.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:hugeicons/hugeicons.dart';

class UpdateInstanceController extends AddInstanceController {
  InstanceModel? instance;

  @override
  DatabaseType get selectedDatabaseType => instance?.dbType ?? DatabaseType.mysql;

  @override
  void onDatabaseTypeChange(DatabaseType type) {
    return;
  }

  UpdateInstanceController() : super();

  void loadFromMeta(ConnectValue connectValue) {
    final db = selectedDatabaseType;
    for (final meta in connectionMetaMap[db]?.connMeta ?? const <SettingMeta>[]) {
      switch (meta) {
        case NameMeta():
          fieldTextController(db, meta.name).text = connectValue.name;
        case TargetNetworkMeta():
          fieldTextController(db, settingMetaNameTargetNetworkHost).text = connectValue.getHost();
          fieldTextController(db, settingMetaNameTargetNetworkPort).text = connectValue.getPort()?.toString() ?? "";
        case TargetDBFileMeta():
          fieldTextController(db, meta.name).text = connectValue.getDbFile();
        case UserMeta():
          fieldTextController(db, meta.name).text = connectValue.user;
        case PasswordMeta():
          fieldTextController(db, meta.name).text = connectValue.password;
        case DescMeta():
          fieldTextController(db, meta.name).text = connectValue.desc;
        case CustomMeta():
          fieldTextController(db, meta.name).text = connectValue.getValue(meta.name);
      }
    }
    initQueryCodeController.text = connectValue.initQueryText();
  }

  void tryUpdateInstance(InstanceModel instance) {
    isDatabaseConnectable = null;
    databaseConnectError = null;
    isDatabasePingDoing = false;
    this.instance = instance;
    syncConnectFormDatabaseType();
    loadFromMeta(instance.connectValue);
    notifyListeners();
  }

  @override
  InstanceModel getInstanceModel() {
    final connectValue = getConnectValue();
    return InstanceModel(
      id: instance!.id,
      dbType: selectedDatabaseType,
      name: connectValue.name,
      target: connectValue.target,
      user: connectValue.user,
      password: connectValue.password,
      desc: connectValue.desc,
      custom: connectValue.custom,
      initQuerys: connectValue.initQuerys,
      activeSchemas: instance!.activeSchemas,
      createdAt: instance!.createdAt,
      latestOpenAt: instance!.latestOpenAt,
    );
  }
}

UpdateInstanceController updateInstanceController = UpdateInstanceController();

/// 编辑数据源：仅连接配置（与新建向导第二步一致的 Tab + 表单）。
Future<void> showUpdateInstanceDialog(
  BuildContext context,
  WidgetRef ref,
  InstanceModel instance,
) async {
  updateInstanceController.tryUpdateInstance(instance);
  await showDialog<void>(
    context: context,
    barrierDismissible: false,
    builder: (dialogContext) => const _UpdateInstanceDialog(),
  );
}

class _UpdateInstanceDialog extends ConsumerStatefulWidget {
  const _UpdateInstanceDialog();

  @override
  ConsumerState<_UpdateInstanceDialog> createState() => _UpdateInstanceDialogState();
}

class _UpdateInstanceDialogState extends ConsumerState<_UpdateInstanceDialog> {
  @override
  void initState() {
    super.initState();
    updateInstanceController.addListener(_onCtrl);
  }

  @override
  void dispose() {
    updateInstanceController.removeListener(_onCtrl);
    super.dispose();
  }

  void _onCtrl() {
    if (mounted) {
      setState(() {});
    }
  }

  void _submit() {
    if (!updateInstanceController.validateForm()) {
      return;
    }
    ref.read(instancesServicesProvider.notifier).updateInstance(updateInstanceController.getInstanceModel());
    updateInstanceController.clear();
    ref.read(instancesProvider.notifier).changePage("");
    if (!mounted) {
      return;
    }
    Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    return CustomDialog(
      title: l10n.update_db_instance,
      titleIcon: HugeIcon(
        icon: HugeIcons.strokeRoundedDatabase,
        color: Theme.of(context).colorScheme.onSurfaceVariant,
      ),
      subtitle: '连接配置',
      maxWidth: 960,
      maxHeight: 720,
      footerLeading: DbInstanceConnectionTestWidget(
        isDatabasePingDoing: updateInstanceController.isDatabasePingDoing,
        isDatabaseConnectable: updateInstanceController.isDatabaseConnectable,
        databaseConnectError: updateInstanceController.databaseConnectError,
        onTestConnection: () => updateInstanceController.databasePing(),
      ),
      content: ListenableBuilder(
        listenable: updateInstanceController,
        builder: (context, _) => ListenableBuilder(
          listenable: updateInstanceController.connectForm,
          builder: (context, _) => InstanceFormWidget(
            controller: updateInstanceController,
            codeController: updateInstanceController.initQueryCodeController,
            nameReadOnly: true,
          ),
        ),
      ),
      actions: [
        FilledButton(
          onPressed: _submit,
          child: Text(l10n.submit),
        ),
      ],
    );
  }
}
