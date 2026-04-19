import 'package:client/models/instances.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/button.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:db_driver/db_driver.dart';
import 'package:collection/collection.dart';
import 'package:go_router/go_router.dart';
import 'package:sql_editor/re_editor.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:sql_parser/parser.dart';
import 'package:client/widgets/sql_highlight.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/dialog.dart';
import 'package:client/widgets/loading.dart';
import 'package:hugeicons/hugeicons.dart';
import 'package:client/widgets/form.dart';

const String initQueryTabId = 'initize';

class AddInstanceController extends ChangeNotifier {
  final Map<DatabaseType, Map<String, TextEditingController>> _fieldControllers = {};

  DatabaseType selectedDatabaseType = DatabaseType.mysql;

  /// 每种 [DatabaseType] 一套 [GlobalKey] 与校验态；Tab/分组由各 [Tracked*] 的 [groupId] 声明，与 controller 无业务耦合。
  final Map<DatabaseType, TrackedFormController> _connectForms = {
    for (final t in allDatabaseType) t: TrackedFormController(),
  };

  /// 当前 [selectedDatabaseType] 对应的表单控制器（与 [TrackedForm] / [Tracked*] 共用）。
  TrackedFormController get connectForm => _connectForms[selectedDatabaseType]!;

  /// 编辑页在载入实例后曾用于对齐表单与 [selectedDatabaseType]；现为每类型独立 controller，无需再同步。
  void syncConnectFormDatabaseType() {}

  /// 提交前校验当前库类型下全部连接字段（与 [Tracked*] 共用同一套 [GlobalKey]）。
  bool validateForm() {
    return connectForm.validate();
  }

  @override
  void dispose() {
    for (final c in _connectForms.values) {
      c.dispose();
    }
    super.dispose();
  }

  final Map<DatabaseType, CodeLineEditingController> initQueryCodeControllers = {};

  bool? isDatabaseConnectable;
  bool isDatabasePingDoing = false;
  String? databaseConnectError;

  int _wizardStep = 0;

  int get wizardStep => _wizardStep;

  void setWizardStep(int step) {
    final s = step.clamp(0, 1);
    if (_wizardStep == s) {
      return;
    }
    _wizardStep = s;
    notifyListeners();
  }

  CodeLineEditingController get initQueryCodeController {
    return initQueryCodeControllers[selectedDatabaseType]!;
  }

  List<SettingMeta> get _connMetaList {
    return connectionMetaMap[selectedDatabaseType]?.connMeta ?? const <SettingMeta>[];
  }

  List<SettingMeta> get connectionMetasSelected => List<SettingMeta>.unmodifiable(_connMetaList);

  AddInstanceController() {
    for (final connMeta in connectionMetas) {
      final ctrls = _fieldControllers.putIfAbsent(connMeta.type, () => {});
      for (final meta in connMeta.connMeta) {
        if (meta is TargetNetworkMeta) {
          ctrls[settingMetaNameTargetNetworkHost] = TextEditingController(text: meta.defaultValue ?? "");
          ctrls[settingMetaNameTargetNetworkPort] = TextEditingController(text: meta.defaultPort ?? "");
        } else {
          ctrls[meta.name] = TextEditingController(text: meta.defaultValue ?? "");
        }
      }
    }
    for (final connMeta in connectionMetas) {
      final codeController = CodeLineEditingController(
        spanBuilder: ({required codeLines, required context, required style}) {
          return getSQLHighlightTextSpan(
            connMeta.type.dialectType,
            codeLines.asString(TextLineBreak.lf),
            defalutStyle: style,
          );
        },
      );
      codeController.text = connectionMetaMap[connMeta.type]!.initQueryText();
      initQueryCodeControllers[connMeta.type] = codeController;
    }
  }

  SettingMeta? _metaFor(DatabaseType db, String fieldName) {
    for (final m in connectionMetaMap[db]?.connMeta ?? const <SettingMeta>[]) {
      if (m.name == fieldName) {
        return m;
      }
      if (m is TargetNetworkMeta && fieldName == settingMetaNameTargetNetworkPort) {
        return m;
      }
    }
    return null;
  }

  TextEditingController fieldTextController(DatabaseType db, String fieldName) {
    return _fieldControllers[db]![fieldName]!;
  }

  TextEditingController fieldText(String fieldName) => fieldTextController(selectedDatabaseType, fieldName);

  SettingMeta fieldMeta(String fieldName) {
    final m = _metaFor(selectedDatabaseType, fieldName);
    if (m == null) {
      throw StateError('Unknown field $fieldName for $selectedDatabaseType');
    }
    return m;
  }

  String _fieldText(DatabaseType dbType, String fieldName) {
    return _fieldControllers[dbType]?[fieldName]?.text ?? "";
  }

  String _addressFieldText(DatabaseType dbType) {
    return _fieldControllers[dbType]?[settingMetaNameTargetNetworkHost]?.text ??
        _fieldControllers[dbType]?[settingMetaNameTargetDBFile]?.text ??
        "";
  }

  void _setFieldText(DatabaseType dbType, String fieldName, String value) {
    _fieldControllers[dbType]?[fieldName]?.text = value;
  }

  void onDatabaseTypeChange(DatabaseType type) {
    final sourceType = selectedDatabaseType;
    final sourcePortCtrl = _fieldControllers[sourceType]?[settingMetaNameTargetNetworkPort];
    final sourceNetMeta = _metaFor(sourceType, settingMetaNameTargetNetworkHost);
    final sourceDefaultPort = sourceNetMeta is TargetNetworkMeta ? (sourceNetMeta.defaultPort ?? "") : "";
    final isPortChanged = sourcePortCtrl != null && sourcePortCtrl.text != sourceDefaultPort;
    final name = _fieldText(sourceType, settingMetaNameName);
    final desc = _fieldText(sourceType, settingMetaNameDesc);
    final addr = _addressFieldText(sourceType);
    final user = _fieldText(sourceType, settingMetaNameUser);
    final password = _fieldText(sourceType, settingMetaNamePassword);

    selectedDatabaseType = type;
    isDatabasePingDoing = false;
    isDatabaseConnectable = null;
    databaseConnectError = null;

    _setFieldText(type, settingMetaNameName, name);
    _setFieldText(type, settingMetaNameDesc, desc);
    _setFieldText(type, settingMetaNameTargetNetworkHost, addr);
    _setFieldText(type, settingMetaNameTargetDBFile, addr);
    _setFieldText(type, settingMetaNameUser, user);
    _setFieldText(type, settingMetaNamePassword, password);
    if (!isPortChanged && _fieldControllers[type]?.containsKey(settingMetaNameTargetNetworkPort) == true) {
      port = defaultPort;
    }
    notifyListeners();
  }

  void clear() {
    for (final e in _fieldControllers.entries) {
      final db = e.key;
      for (final meta in connectionMetaMap[db]?.connMeta ?? const <SettingMeta>[]) {
        if (meta is TargetNetworkMeta) {
          e.value[settingMetaNameTargetNetworkHost]?.text = meta.defaultValue ?? "";
          e.value[settingMetaNameTargetNetworkPort]?.text = meta.defaultPort ?? "";
        } else {
          e.value[meta.name]?.text = meta.defaultValue ?? "";
        }
      }
    }
    _wizardStep = 0;
    isDatabasePingDoing = false;
    isDatabaseConnectable = null;
    databaseConnectError = null;
    for (final c in _connectForms.values) {
      c.resetInvalidGroups();
    }
    notifyListeners();
  }

  String get defaultPort {
    final m = _metaFor(selectedDatabaseType, settingMetaNameTargetNetworkHost);
    return m is TargetNetworkMeta ? (m.defaultPort ?? "") : "";
  }

  set port(String port) {
    _fieldControllers[selectedDatabaseType]?[settingMetaNameTargetNetworkPort]?.text = port;
  }

  ConnectValue getConnectValue() {
    String name = "";
    String addr = "";
    String dbFile = "";
    int? port;
    String user = "";
    String password = "";
    String desc = "";
    Map<String, String> custom = {};

    final db = selectedDatabaseType;
    for (final meta in _connMetaList) {
      switch (meta) {
        case NameMeta():
          name = _fieldControllers[db]![meta.name]!.text;
        case TargetNetworkMeta():
          addr = _fieldControllers[db]![settingMetaNameTargetNetworkHost]!.text;
          port = int.tryParse(_fieldControllers[db]![settingMetaNameTargetNetworkPort]!.text.trim());
        case TargetDBFileMeta():
          final text = _fieldControllers[db]![meta.name]!.text;
          dbFile = text;
          addr = dbFile;
        case UserMeta():
          user = _fieldControllers[db]![meta.name]!.text;
        case PasswordMeta():
          password = _fieldControllers[db]![meta.name]!.text;
        case DescMeta():
          desc = _fieldControllers[db]![meta.name]!.text;
        case CustomMeta():
          custom[meta.name] = _fieldControllers[db]![meta.name]!.text;
      }
    }
    List<String> querys = splitSQL(
      selectedDatabaseType.dialectType,
      initQueryCodeControllers[selectedDatabaseType]!.text.trim(),
    ).map((e) => e.content.trim()).whereNot((e) => e.trim() == "").toList();
    final target = dbFile.isNotEmpty
        ? ConnectTarget.dbFile(dbFile: dbFile)
        : ConnectTarget.network(host: addr, port: port ?? 0);
    return ConnectValue(
      name: name,
      target: target,
      user: user,
      password: password,
      desc: desc,
      custom: custom,
      initQuerys: querys,
    );
  }

  Future<void> databasePing() async {
    final connectValue = getConnectValue();
    BaseConnection? conn;
    try {
      isDatabasePingDoing = true;
      notifyListeners();
      conn = await ConnectionFactory.open(type: selectedDatabaseType, meta: connectValue);
      isDatabaseConnectable = true;
      databaseConnectError = null;
      conn.close();
    } catch (e) {
      isDatabaseConnectable = false;
      databaseConnectError = e.toString();
      print(e);
    } finally {
      isDatabasePingDoing = false;
      notifyListeners();
    }
  }

  InstanceModel getInstanceModel() {
    final connectValue = getConnectValue();
    return InstanceModel(
      id: const InstanceId(value: 0),
      dbType: selectedDatabaseType,
      name: connectValue.name,
      target: connectValue.target,
      user: connectValue.user,
      password: connectValue.password,
      desc: connectValue.desc,
      custom: connectValue.custom,
      initQuerys: connectValue.initQuerys,
      activeSchemas: [],
      createdAt: DateTime.now(),
      latestOpenAt: DateTime.now(),
    );
  }

  Color? get pingIndicatorColor {
    if (isDatabasePingDoing) {
      return null;
    }
    if (isDatabaseConnectable == null) {
      return null;
    }
    if (isDatabaseConnectable == true) {
      return Colors.green;
    }
    return Colors.red;
  }
}

AddInstanceController addInstanceController = AddInstanceController();

Future<void> showAddInstanceDialog(BuildContext context) async {
  addInstanceController.clear();
  await showDialog<void>(
    context: context,
    barrierDismissible: false,
    builder: (dialogContext) => _AddInstanceWizardDialog(),
  );
}

void _addInstanceWizardSubmitAndClose(WidgetRef ref, BuildContext context, {required bool closeToList}) {
  if (!addInstanceController.validateForm()) {
    return;
  }
  ref.read(instancesServicesProvider.notifier).addInstance(addInstanceController.getInstanceModel());
  addInstanceController.clear();
  ref.read(instancesProvider.notifier).changePage("");
  if (!context.mounted) {
    return;
  }
  Navigator.of(context).pop();
  if (closeToList && context.mounted) {
    GoRouter.of(context).go('/instances/list');
  }
}

class _AddInstanceWizardDialog extends ConsumerStatefulWidget {
  const _AddInstanceWizardDialog();

  @override
  ConsumerState<_AddInstanceWizardDialog> createState() => _AddInstanceWizardDialogState();
}

class _AddInstanceWizardDialogState extends ConsumerState<_AddInstanceWizardDialog> {
  @override
  void initState() {
    super.initState();
    addInstanceController.addListener(_onCtrl);
  }

  @override
  void dispose() {
    addInstanceController.removeListener(_onCtrl);
    super.dispose();
  }

  void _onCtrl() {
    if (mounted) {
      setState(() {});
    }
  }

  @override
  Widget build(BuildContext context) {
    final step = addInstanceController.wizardStep;
    return Dialog(
      child: step == 0 ? _AddInstanceWizardStep1() : _AddInstanceWizardStep2(),
    );
  }
}

/// 向导弹窗步骤 1：选择数据源
class _AddInstanceWizardStep1 extends StatelessWidget {
  static const double _tileWidth = 104;
  static const double _tileHeight = 92;

  const _AddInstanceWizardStep1();

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final selected = addInstanceController.selectedDatabaseType;
    final selectedColor = addInstanceController.pingIndicatorColor;
    return CustomDialogWidget(
      title: l10n.add_db_instance,
      titleIcon: HugeIcon(
        icon: HugeIcons.strokeRoundedDatabase,
        color: Theme.of(context).colorScheme.onSurfaceVariant, // navigation rail 默认icon颜色
      ),
      subtitle: l10n.add_instance_wizard_step1_subtitle,
      maxWidth: 960,
      maxHeight: 720,
      body: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          const SizedBox(height: kSpacingSmall),
          Expanded(
            child: SingleChildScrollView(
              padding: EdgeInsets.zero,
              child: Wrap(
                spacing: kSpacingTiny,
                runSpacing: kSpacingTiny,
                children: [
                  for (final meta in connectionMetas)
                    SizedBox(
                      width: _tileWidth,
                      height: _tileHeight,
                      child: DatabaseTypeCard(
                        name: meta.displayName,
                        type: meta.type,
                        logoPath: meta.logoAssertPath,
                        selected: meta.type == selected,
                        selectedColor: selectedColor,
                        onTap: addInstanceController.onDatabaseTypeChange,
                        onDoubleTap: () {
                          addInstanceController.onDatabaseTypeChange(meta.type);
                          addInstanceController.setWizardStep(1);
                        },
                      ),
                    ),
                ],
              ),
            ),
          ),
        ],
      ),
      actions: [
        FilledButton(
          onPressed: () => addInstanceController.setWizardStep(1),
          child: Text(l10n.wizard_next),
        ),
      ],
    );
  }
}

class _AddInstanceWizardStep2 extends ConsumerWidget {
  const _AddInstanceWizardStep2();

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final l10n = AppLocalizations.of(context)!;
    return CustomDialogWidget(
      title: l10n.add_db_instance,
      titleIcon: Image.asset(
        connectionMetaMap[addInstanceController.selectedDatabaseType]!.logoAssertPath,
        width: kIconSizeMedium,
        height: kIconSizeMedium,
      ),
      subtitle: l10n.add_instance_wizard_step2_subtitle,
      maxWidth: 960,
      maxHeight: 720,
      footerLeading: DbInstanceConnectionTestWidget(
        isDatabasePingDoing: addInstanceController.isDatabasePingDoing,
        isDatabaseConnectable: addInstanceController.isDatabaseConnectable,
        databaseConnectError: addInstanceController.databaseConnectError,
        onTestConnection: () => addInstanceController.databasePing(),
      ),
      body: ListenableBuilder(
        listenable: addInstanceController,
        builder: (context, _) => ListenableBuilder(
          listenable: addInstanceController.connectForm,
          builder: (context, _) => InstanceFormWidget.forAddInstanceWizard(),
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => addInstanceController.setWizardStep(0),
          child: Text(l10n.wizard_previous),
        ),
        SizedBox(width: kSpacingSmall),
        FilledButton(
          onPressed: () => _addInstanceWizardSubmitAndClose(ref, context, closeToList: true),
          child: Text(l10n.submit),
        ),
      ],
    );
  }
}

class DatabaseTypeCard extends StatelessWidget {
  final DatabaseType type;
  final String name;
  final String logoPath;
  final bool selected;
  final Color? selectedColor;
  final Function(DatabaseType type)? onTap;
  final VoidCallback? onDoubleTap;

  const DatabaseTypeCard({
    super.key,
    required this.type,
    required this.name,
    required this.logoPath,
    this.selected = false,
    this.selectedColor,
    this.onTap,
    this.onDoubleTap,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      constraints: const BoxConstraints(minHeight: 84, minWidth: 100),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(10),
        color: selected
            ? Theme.of(context)
                  .colorScheme
                  .primaryContainer // db type card selected color
            : null,
      ),
      child: InkWell(
        borderRadius: BorderRadius.circular(10),
        onTap: () {
          if (!selected && onTap != null) {
            onTap!(type);
          }
        },
        onDoubleTap: onDoubleTap,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Padding(
              padding: const EdgeInsets.fromLTRB(kSpacingTiny, kSpacingSmall, kSpacingTiny, kSpacingTiny),
              child: Image.asset(logoPath),
            ),
            Padding(
              padding: const EdgeInsets.fromLTRB(kSpacingTiny, kSpacingTiny, kSpacingTiny, kSpacingSmall),
              child: Text(name),
            ),
          ],
        ),
      ),
    );
  }
}

class InstanceFormWidget extends StatelessWidget {
  final AddInstanceController controller;
  final CodeLineEditingController codeController;

  final bool nameReadOnly;

  const InstanceFormWidget({
    super.key,
    required this.controller,
    required this.codeController,
    this.nameReadOnly = false,
  });

  factory InstanceFormWidget.forAddInstanceWizard() {
    final c = addInstanceController;
    return InstanceFormWidget(
      controller: c,
      codeController: c.initQueryCodeController,
    );
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final c = controller;
    // Tab 顺序由 [groupId] 首次出现顺序决定；每个 meta 一行；初始化 SQL 固定追加在末尾。
    final children = <Widget>[];
    for (final meta in c.connectionMetasSelected) {
      switch (meta) {
        case NameMeta():
          children.add(
            TrackedTextFormField(
              fieldName: settingMetaNameName,
              groupId: meta.group,
              label: l10n.db_instance_name,
              isRequired: true,
              controller: c.fieldText(settingMetaNameName),
              readOnly: nameReadOnly,
            ),
          );
        case TargetNetworkMeta():
          children.add(
            TrackedHostPortFields(
              groupId: meta.group,
              hostFieldName: settingMetaNameTargetNetworkHost,
              hostController: c.fieldText(settingMetaNameTargetNetworkHost),
              hostLabel: l10n.db_instance_host,
              hostRequired: true,
              portFieldName: settingMetaNameTargetNetworkPort,
              portController: c.fieldText(settingMetaNameTargetNetworkPort),
              portLabel: l10n.db_instance_port,
              portRequired: true,
            ),
          );
        case TargetDBFileMeta():
          children.add(
            TrackedFilePathFormField(
              fieldName: settingMetaNameTargetDBFile,
              groupId: meta.group,
              isRequired: true,
              label: 'Path',
              controller: c.fieldText(settingMetaNameTargetDBFile),
              pickTooltip: l10n.tooltip_select_directory,
            ),
          );
        case UserMeta():
          children.add(
            TrackedTextFormField(
              fieldName: settingMetaNameUser,
              groupId: meta.group,
              label: l10n.db_instance_user,
              controller: c.fieldText(settingMetaNameUser),
            ),
          );
        case PasswordMeta():
          children.add(
            TrackedPasswordFormField(
              fieldName: settingMetaNamePassword,
              groupId: meta.group,
              label: l10n.db_instance_password,
              controller: c.fieldText(settingMetaNamePassword),
            ),
          );
        case DescMeta():
          children.add(
            TrackedDescFormField(
              fieldName: settingMetaNameDesc,
              groupId: meta.group,
              label: l10n.db_instance_desc,
              controller: c.fieldText(settingMetaNameDesc),
            ),
          );
        case CustomMeta():
          final useEnum =
              meta.type == SettingMetaType.enumValue && meta.enumValues != null && meta.enumValues!.isNotEmpty;
          children.add(
            useEnum
                ? TrackedEnumFormField(
                    fieldName: meta.name,
                    groupId: meta.group,
                    isRequired: meta.isRequired,
                    label: meta.name,
                    controller: c.fieldText(meta.name),
                    enumValues: meta.enumValues!,
                    defaultValue: meta.defaultValue,
                    helperText: meta.comment,
                  )
                : TrackedTextFormField(
                    fieldName: meta.name,
                    groupId: meta.group,
                    isRequired: meta.isRequired,
                    label: meta.name,
                    controller: c.fieldText(meta.name),
                  ),
          );
      }
    }
    children.add(
      TrackedCodeEditorFormField(
        fieldName: initQueryTabId,
        groupId: initQueryTabId,
        codeController: codeController,
        child: Padding(
          padding: const EdgeInsets.all(kSpacingSmall),
          child: SizedBox(
            height: 420,
            child: Container(
              constraints: const BoxConstraints(maxHeight: 430),
              child: CodeEditor(
                borderRadius: BorderRadius.circular(10),
                style: CodeEditorStyle(
                  backgroundColor: Theme.of(context).colorScheme.surfaceContainerLow,
                  textStyle: GoogleFonts.robotoMono(
                    color: Theme.of(context).colorScheme.onSurface,
                  ),
                ),
                indicatorBuilder: (context, editingController, chunkController, notifier) {
                  return Row(
                    children: [
                      DefaultCodeLineNumber(
                        controller: editingController,
                        notifier: notifier,
                      ),
                      DefaultCodeChunkIndicator(width: 20, controller: chunkController, notifier: notifier),
                    ],
                  );
                },
                controller: codeController,
                wordWrap: false,
              ),
            ),
          ),
        ),
      ),
    );
    return TrackedForm.tabbed(
      controller: c.connectForm,
      tabLabels: {settingMetaGroupBase: l10n.db_base_config},
      children: children,
    );
  }
}

/// 数据源对话框底部：「测试连接」按钮与右侧状态（loading / 成功图标 / 失败图标+文案）。
class DbInstanceConnectionTestWidget extends StatelessWidget {
  final bool isDatabasePingDoing;
  final bool? isDatabaseConnectable;
  final String? databaseConnectError;
  final VoidCallback onTestConnection;

  const DbInstanceConnectionTestWidget({
    super.key,
    required this.isDatabasePingDoing,
    required this.isDatabaseConnectable,
    this.databaseConnectError,
    required this.onTestConnection,
  });

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final cs = Theme.of(context).colorScheme;

    return Row(
      crossAxisAlignment: CrossAxisAlignment.center,
      children: [
        LinkButton(
          text: l10n.db_instance_test,
          onPressed: isDatabasePingDoing ? null : onTestConnection,
        ),
        const SizedBox(width: kSpacingSmall),
        if (isDatabasePingDoing)
          const Loading.medium()
        else if (isDatabaseConnectable == true)
          Icon(
            Icons.check_circle,
            size: kIconSizeSmall,
            color: Colors.green,
          )
        else if (isDatabaseConnectable == false) ...[
          Icon(
            Icons.error,
            size: kIconSizeSmall,
            color: cs.error,
          ),
          const SizedBox(width: kSpacingTiny),
          Expanded(
            child: Text(
              databaseConnectError ?? '',
              style: Theme.of(context).textTheme.bodySmall?.copyWith(color: cs.error),
              maxLines: 3,
              overflow: TextOverflow.ellipsis,
            ),
          ),
        ],
      ],
    );
  }
}
