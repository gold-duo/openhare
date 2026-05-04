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

class DatabaseFormController {
  final DatabaseType databaseType;
  final Map<String, TextEditingController> fieldControllers = {};
  final TrackedFormController connectForm = TrackedFormController();
  late final CodeLineEditingController initQueryCodeController;

  void dispose() {
    for (final c in fieldControllers.values) {
      c.dispose();
    }
    initQueryCodeController.dispose();
    connectForm.dispose();
  }

  void reset() {
    for (final meta in connectionMetaMap[databaseType]!.connMeta) {
      if (meta is TargetNetworkMeta) {
        fieldControllers[settingMetaNameTargetNetworkHost]?.text = meta.defaultValue ?? "";
        fieldControllers[settingMetaNameTargetNetworkPort]?.text = meta.defaultPort ?? "";
      } else {
        fieldControllers[meta.name]?.text = meta.defaultValue ?? "";
      }
    }
    initQueryCodeController.text = connectionMetaMap[databaseType]!.initQueryText();
    connectForm.resetInvalidGroups();
  }

  DatabaseFormController(this.databaseType) {
    for (final meta in connectionMetaMap[databaseType]!.connMeta) {
      if (meta is TargetNetworkMeta) {
        fieldControllers[settingMetaNameTargetNetworkHost] = TextEditingController(text: meta.defaultValue ?? "");
        fieldControllers[settingMetaNameTargetNetworkPort] = TextEditingController(text: meta.defaultPort ?? "");
      } else {
        fieldControllers[meta.name] = TextEditingController(text: meta.defaultValue ?? "");
      }
    }
    initQueryCodeController = CodeLineEditingController(
      spanBuilder: ({required codeLines, required context, required style}) {
        return getSQLHighlightTextSpan(
          databaseType.dialectType,
          codeLines.asString(TextLineBreak.lf),
          defalutStyle: style,
        );
      },
    );
    initQueryCodeController.text = connectionMetaMap[databaseType]!.initQueryText();
  }
}

class AddInstanceController extends ChangeNotifier {
  final Map<DatabaseType, DatabaseFormController> databaseFormControllers = {};

  DatabaseType selectedDatabaseType = connectionMetas.first.type;

  DatabaseFormController get selectedDatabaseFormController => databaseFormControllers[selectedDatabaseType]!;

  bool validateForm() => selectedDatabaseFormController.connectForm.validate();

  @override
  void dispose() {
    for (final f in databaseFormControllers.values) {
      f.dispose();
    }
    super.dispose();
  }

  // 数据库连接测试的状态
  bool? isDatabaseConnectable;
  bool isDatabasePingDoing = false;
  String? databaseConnectError;

  // 向导步骤
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

  void onDatabaseTypeChange(DatabaseType type) {
    if (selectedDatabaseType == type) {
      return;
    }
    selectedDatabaseType = type;
    isDatabasePingDoing = false;
    isDatabaseConnectable = null;
    databaseConnectError = null;
    notifyListeners();
  }

  void clear() {
    for (final f in databaseFormControllers.values) {
      f.reset();
    }
    _wizardStep = 0;
    isDatabasePingDoing = false;
    isDatabaseConnectable = null;
    databaseConnectError = null;
    notifyListeners();
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

    final controller = selectedDatabaseFormController;
    final initCode = controller.initQueryCodeController;
    final fields = controller.fieldControllers;
    for (final meta in getConnMetas(selectedDatabaseType)) {
      switch (meta) {
        case NameMeta():
          name = fields[meta.name]!.text;
        case TargetNetworkMeta():
          addr = fields[settingMetaNameTargetNetworkHost]!.text;
          port = int.tryParse(fields[settingMetaNameTargetNetworkPort]!.text.trim());
        case TargetDBFileMeta():
          final text = fields[meta.name]!.text;
          dbFile = text;
          addr = dbFile;
        case UserMeta():
          user = fields[meta.name]!.text;
        case PasswordMeta():
          password = fields[meta.name]!.text;
        case DescMeta():
          desc = fields[meta.name]!.text;
        case CustomMeta():
          custom[meta.name] = fields[meta.name]!.text;
      }
    }
    List<String> querys = splitSQL(
      selectedDatabaseType.dialectType,
      initCode.text.trim(),
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

  AddInstanceController() {
    for (final cm in connectionMetas) {
      databaseFormControllers[cm.type] = DatabaseFormController(cm.type);
    }
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
                        onTap: () {
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
      actions: [],
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
          listenable: addInstanceController.selectedDatabaseFormController.connectForm,
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

class DatabaseTypeCard extends StatefulWidget {
  final DatabaseType type;
  final String name;
  final String logoPath;
  final VoidCallback? onTap;

  const DatabaseTypeCard({
    super.key,
    required this.type,
    required this.name,
    required this.logoPath,
    this.onTap,
  });

  @override
  State<DatabaseTypeCard> createState() => _DatabaseTypeCardState();
}

class _DatabaseTypeCardState extends State<DatabaseTypeCard> {
  bool _hovering = false;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      onEnter: (_) => setState(() => _hovering = true),
      onExit: (_) => setState(() => _hovering = false),
      child: Container(
        constraints: const BoxConstraints(minHeight: 84, minWidth: 100),
        decoration: BoxDecoration(
          color: _hovering ? Theme.of(context).colorScheme.surfaceContainer : null,
          borderRadius: BorderRadius.circular(10),
        ),
        child: InkWell(
          borderRadius: BorderRadius.circular(10),
          onTap: widget.onTap,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Padding(
                padding: const EdgeInsets.fromLTRB(kSpacingTiny, kSpacingSmall, kSpacingTiny, kSpacingTiny),
                child: Image.asset(widget.logoPath),
              ),
              Padding(
                padding: const EdgeInsets.fromLTRB(kSpacingTiny, kSpacingTiny, kSpacingTiny, kSpacingSmall),
                child: Text(widget.name),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class InstanceFormWidget extends StatelessWidget {
  final AddInstanceController controller;
  final CodeLineEditingController codeController;

  const InstanceFormWidget({
    super.key,
    required this.controller,
    required this.codeController,
  });

  factory InstanceFormWidget.forAddInstanceWizard() {
    final c = addInstanceController;
    return InstanceFormWidget(
      controller: c,
      codeController: c.selectedDatabaseFormController.initQueryCodeController,
    );
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final c = controller;
    // Tab 顺序由 [groupId] 首次出现顺序决定；每个 meta 一行；初始化 SQL 固定追加在末尾。
    final children = <Widget>[];
    for (final meta in getConnMetas(c.selectedDatabaseType)) {
      switch (meta) {
        case NameMeta():
          children.add(
            TrackedTextFormField(
              fieldName: settingMetaNameName,
              groupId: meta.group,
              label: l10n.db_instance_name,
              isRequired: true,
              controller: c.selectedDatabaseFormController.fieldControllers[settingMetaNameName]!,
            ),
          );
        case TargetNetworkMeta():
          children.add(
            TrackedHostPortFields(
              groupId: meta.group,
              hostFieldName: settingMetaNameTargetNetworkHost,
              hostController: c.selectedDatabaseFormController.fieldControllers[settingMetaNameTargetNetworkHost]!,
              hostLabel: l10n.db_instance_host,
              hostRequired: true,
              portFieldName: settingMetaNameTargetNetworkPort,
              portController: c.selectedDatabaseFormController.fieldControllers[settingMetaNameTargetNetworkPort]!,
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
              controller: c.selectedDatabaseFormController.fieldControllers[settingMetaNameTargetDBFile]!,
              pickTooltip: l10n.tooltip_select_directory,
            ),
          );
        case UserMeta():
          children.add(
            TrackedTextFormField(
              fieldName: settingMetaNameUser,
              groupId: meta.group,
              label: l10n.db_instance_user,
              controller: c.selectedDatabaseFormController.fieldControllers[settingMetaNameUser]!,
            ),
          );
        case PasswordMeta():
          children.add(
            TrackedPasswordFormField(
              fieldName: settingMetaNamePassword,
              groupId: meta.group,
              label: l10n.db_instance_password,
              controller: c.selectedDatabaseFormController.fieldControllers[settingMetaNamePassword]!,
            ),
          );
        case DescMeta():
          children.add(
            TrackedDescFormField(
              fieldName: settingMetaNameDesc,
              groupId: meta.group,
              label: l10n.db_instance_desc,
              controller: c.selectedDatabaseFormController.fieldControllers[settingMetaNameDesc]!,
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
                    controller: c.selectedDatabaseFormController.fieldControllers[meta.name]!,
                    enumValues: meta.enumValues!,
                    defaultValue: meta.defaultValue,
                    helperText: meta.comment,
                  )
                : TrackedTextFormField(
                    fieldName: meta.name,
                    groupId: meta.group,
                    isRequired: meta.isRequired,
                    label: meta.name,
                    controller: c.selectedDatabaseFormController.fieldControllers[meta.name]!,
                  ),
          );
      }
    }
    children.add(
      TrackedCodeEditorFormField(
        fieldName: settingMetaGroupInitQuery,
        groupId: settingMetaGroupInitQuery,
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
      controller: c.selectedDatabaseFormController.connectForm,
      tabLabels: {
        settingMetaGroupBase: l10n.db_base_config,
        settingMetaGroupParams: l10n.db_conn_params,
        settingMetaGroupInitQuery: l10n.db_conn_init_query,
      },
      children: children,
    );
  }
}

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
