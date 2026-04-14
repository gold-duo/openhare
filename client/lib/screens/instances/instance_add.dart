import 'package:client/models/instances.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/button.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:db_driver/db_driver.dart';
import 'package:client/screens/page_skeleton.dart';
import 'package:collection/collection.dart';
import 'package:go_router/go_router.dart';
import 'package:sql_editor/re_editor.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:sql_parser/parser.dart';
import 'package:client/widgets/sql_highlight.dart';
import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/loading.dart';
import 'package:file_picker/file_picker.dart';
import 'package:path/path.dart' as p;

class AddInstancePage extends StatefulWidget {
  const AddInstancePage({super.key});

  @override
  State<AddInstancePage> createState() => _AddInstancePageState();
}

class _AddInstancePageState extends State<AddInstancePage> {
  @override
  void initState() {
    super.initState();
    addInstanceController.addListener(() => mounted ? setState(() {}) : null);
  }

  @override
  void dispose() {
    addInstanceController.removeListener(() {});
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return PageSkeleton(
      topBar: Row(
        children: [
          SizedBox(width: kSpacingMedium),
          RectangleIconButton.medium(
            icon: Icons.arrow_back,
            iconColor: Theme.of(context).colorScheme.onSurfaceVariant, // 新建数据源页面返回按钮颜色
            onPressed: () => GoRouter.of(context).go('/instances/list'),
          ),
        ],
      ),
      bottomBar: AddInstanceBottomBar(
        isDatabasePingDoing: addInstanceController.isDatabasePingDoing,
        isDatabaseConnectable: addInstanceController.isDatabaseConnectable,
        databaseConnectError: addInstanceController.databaseConnectError,
      ),
      child: const AddInstance(),
    );
  }
}

class AddInstance extends ConsumerStatefulWidget {
  const AddInstance({super.key});

  @override
  ConsumerState<AddInstance> createState() => _AddInstanceState();
}

class _AddInstanceState extends ConsumerState<AddInstance> {
  @override
  void initState() {
    super.initState();
    addInstanceController.addListener(() => mounted ? setState(() {}) : null);
  }

  @override
  void dispose() {
    addInstanceController.removeListener(() {});
    super.dispose();
  }

  Color? selectedColor(AddInstanceController addInstanceController) {
    if (addInstanceController.isDatabasePingDoing) {
      return null;
    }
    if (addInstanceController.isDatabaseConnectable == null) {
      return null;
    }
    if (addInstanceController.isDatabaseConnectable == true) {
      return Colors.green;
    } else {
      return Colors.red;
    }
  }

  @override
  Widget build(BuildContext context) {
    return BodyPageSkeleton(
      header: Row(
        children: [
          Text(
            AppLocalizations.of(context)!.add_db_instance,
            style: Theme.of(context).textTheme.titleLarge,
            overflow: TextOverflow.ellipsis,
          ),
          const Spacer(),
          TextButton(
            onPressed: addInstanceController.isDatabasePingDoing
                ? null
                : () {
                    addInstanceController.databasePing();
                  },
            child: Text(AppLocalizations.of(context)!.db_instance_test),
          ),
          TextButton(
            onPressed: () async {
              if (addInstanceController.validate()) {
                ref.read(instancesServicesProvider.notifier).addInstance(addInstanceController.getInstanceModel());

                addInstanceController.clear();

                ref.read(instancesProvider.notifier).changePage("");
              }
            },
            child: Text(AppLocalizations.of(context)!.submit_and_continue),
          ),
          TextButton(
            onPressed: () async {
              if (addInstanceController.validate()) {
                ref.read(instancesServicesProvider.notifier).addInstance(addInstanceController.getInstanceModel());

                addInstanceController.clear();

                ref.read(instancesProvider.notifier).changePage("");

                GoRouter.of(context).go('/instances/list');
              }
            },
            child: Text(AppLocalizations.of(context)!.submit),
          ),
        ],
      ),
      child: Container(
        padding: const EdgeInsets.fromLTRB(0, kSpacingSmall, 0, 0),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.start,
          children: [
            DatabaseTypeCardList(
              connectionMetas: connectionMetas,
              selectedDatabaseType: addInstanceController.selectedDatabaseType,
              onDatabaseTypeChange: (type) {
                addInstanceController.onDatabaseTypeChange(type);
              },
              selectedColor: selectedColor(addInstanceController),
            ),
            const SizedBox(height: kSpacingMedium),
            Expanded(
              child: AddInstanceForm(
                infos: addInstanceController.dbInfos,
                selectedGroup: addInstanceController.selectedGroup,
                onValid: (info, isValid) {
                  addInstanceController.updateValidState(info, isValid);
                },
                onGroupChange: (group) {
                  addInstanceController.onGroupChange(group);
                },
                codeController: addInstanceController.initQueryCodeController,
              ),
            ),
          ],
        ),
      ),
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

  const DatabaseTypeCard({
    super.key,
    required this.type,
    required this.name,
    required this.logoPath,
    this.selected = false,
    this.selectedColor,
    this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      constraints: const BoxConstraints(minHeight: 80, minWidth: 100),
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
        child: Column(
          children: [
            Padding(
              padding: const EdgeInsets.only(top: kSpacingSmall),
              child: Image.asset(logoPath),
            ),
            Padding(
              padding: const EdgeInsets.fromLTRB(0, kSpacingTiny, 0, kSpacingSmall),
              child: Text(name),
            ),
          ],
        ),
      ),
    );
  }
}

class DatabaseTypeCardList extends StatelessWidget {
  final List<ConnectionMeta> connectionMetas;
  final Function(DatabaseType type)? onDatabaseTypeChange;
  final DatabaseType? _selectedDatabaseType;
  final Color? selectedColor;

  const DatabaseTypeCardList({
    super.key,
    required this.connectionMetas,
    this.onDatabaseTypeChange,
    DatabaseType? selectedDatabaseType,
    this.selectedColor,
  }) : _selectedDatabaseType = selectedDatabaseType;

  DatabaseType? get selectedDatabaseType => _selectedDatabaseType ?? connectionMetas.first.type;

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Expanded(
          child: SingleChildScrollView(
            scrollDirection: Axis.horizontal,
            child: Row(
              children: [
                for (final connMeta in connectionMetas) ...[
                  DatabaseTypeCard(
                    name: connMeta.displayName,
                    type: connMeta.type,
                    logoPath: connMeta.logoAssertPath,
                    selected: connMeta.type == selectedDatabaseType,
                    selectedColor: selectedColor,
                    onTap: (type) => onDatabaseTypeChange?.call(type),
                  ),
                  const SizedBox(width: kSpacingTiny),
                ],
              ],
            ),
          ),
        ),
      ],
    );
  }
}

class CommonFormField extends StatefulWidget {
  final String label;
  final TextEditingController controller;
  final FormFieldValidator? validator;
  final bool readOnly;
  final GlobalKey<FormFieldState>? state;
  final bool obscureText;

  const CommonFormField({
    super.key,
    required this.label,
    required this.controller,
    this.state,
    this.validator,
    this.readOnly = false,
    this.obscureText = false,
  });

  @override
  State<CommonFormField> createState() => _CommonFormFieldState();
}

class _CommonFormFieldState extends State<CommonFormField> {
  @override
  Widget build(BuildContext context) {
    return Container(
      constraints: const BoxConstraints(minHeight: 80),
      child: TextFormField(
        key: widget.state,
        readOnly: widget.readOnly,
        obscureText: widget.obscureText,
        autovalidateMode: AutovalidateMode.onUnfocus,
        controller: widget.controller,
        validator: widget.validator,
        decoration: InputDecoration(
          border: OutlineInputBorder(borderRadius: BorderRadius.circular(5)),
          labelText: widget.label,
          contentPadding: const EdgeInsets.all(10),
        ),
      ),
    );
  }
}

class DescFormField extends StatelessWidget {
  final TextEditingController controller;
  final GlobalKey<FormFieldState>? state;

  const DescFormField({
    super.key,
    required this.controller,
    this.state,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      constraints: const BoxConstraints(minHeight: 120),
      child: TextFormField(
        key: state,
        controller: controller,
        maxLength: 50,
        maxLines: 4,
        decoration: InputDecoration(
          border: OutlineInputBorder(borderRadius: BorderRadius.circular(5)),
          labelText: AppLocalizations.of(context)!.db_instance_desc,
          contentPadding: const EdgeInsets.all(10),
        ),
      ),
    );
  }
}

class AddInstanceForm extends StatelessWidget {
  final String? selectedGroup;
  final Map<String, FormInfo> infos;
  final Function(FormInfo info, bool isValid)? onValid;
  final Function(String group)? onGroupChange;
  final CodeLineEditingController codeController;

  const AddInstanceForm({
    super.key,
    required this.infos,
    this.onValid,
    this.onGroupChange,
    this.selectedGroup,
    required this.codeController,
  });

  FormFieldValidator validatorFn(BuildContext context, FormInfo info, FormFieldValidator validate) {
    return (value) {
      final result = validate(value);
      if (result == null) {
        onValid?.call(info, true);
      } else {
        onValid?.call(info, false);
      }
      return result;
    };
  }

  FormFieldValidator validatorName(BuildContext context) {
    return (value) {
      if (value == null || value.isEmpty) {
        return AppLocalizations.of(context)!.field_val_msg_value_reqiured;
      }
      return null;
    };
  }

  FormFieldValidator validatorValueRequired(BuildContext context) {
    return (value) {
      if (value == null || value.isEmpty) {
        return AppLocalizations.of(context)!.field_val_msg_value_reqiured;
      }
      return null;
    };
  }

  bool hasField(String fieldName) => infos.containsKey(fieldName);

  Future<void> selectDBFile(BuildContext context, FormInfo addr) async {
    final currentPath = addr.ctrl.text.trim();
    final result = await FilePicker.platform.pickFiles(
      initialDirectory: currentPath.isNotEmpty ? p.dirname(currentPath) : null,
      type: FileType.custom,
      allowedExtensions: const ["db", "sqlite", "sqlite3"],
    );
    final filePath = result?.files.single.path;
    if (filePath != null && filePath.isNotEmpty) {
      addr.ctrl.text = filePath;
      addr.state.currentState?.validate();
    }
  }

  int get selectedGroupIndex {
    if (selectedGroup == null) {
      return 0;
    }
    return groups.indexOf(selectedGroup!);
  }

  List<String> get groups {
    final groups = infos.values
        .groupListsBy((info) => info.meta.group)
        .keys
        .whereNot((e) => e == settingMetaGroupBase)
        .toList();
    groups.add("initize");
    return groups;
  }

  Widget buildNameField(BuildContext context) {
    FormInfo name = infos[settingMetaNameName]!;
    return CommonFormField(
      state: name.state,
      label: AppLocalizations.of(context)!.db_instance_name,
      controller: name.ctrl,
      validator: validatorFn(context, name, validatorName(context)),
    );
  }

  Widget buildAddressField(BuildContext context) {
    final addr = infos[settingMetaNameTargetNetworkHost];
    if (addr == null) {
      return const SizedBox.shrink();
    }
    final hasPort = hasField(settingMetaNameTargetNetworkPort);
    final FormInfo? port = hasPort ? infos[settingMetaNameTargetNetworkPort] : null;
    final addressLabel = AppLocalizations.of(context)!.db_instance_host;

    if (!hasPort || port == null) {
      return CommonFormField(
        label: addressLabel,
        controller: addr.ctrl,
        state: addr.state,
        validator: validatorFn(context, addr, validatorValueRequired(context)),
      );
    }

    return Container(
      constraints: const BoxConstraints(minHeight: 80),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Expanded(
            child: CommonFormField(
              label: addressLabel,
              controller: addr.ctrl,
              state: addr.state,
              validator: validatorFn(context, addr, validatorValueRequired(context)),
            ),
          ),
          const SizedBox(
            width: kSpacingTiny,
          ),
          Container(
            constraints: const BoxConstraints(maxWidth: 120),
            child: CommonFormField(
              label: AppLocalizations.of(context)!.db_instance_port,
              controller: port.ctrl,
              state: port.state,
              validator: validatorFn(context, port, validatorValueRequired(context)),
            ),
          ),
        ],
      ),
    );
  }

  Widget buildDBFileField(BuildContext context) {
    final dbFile = infos[settingMetaNameTargetDBFile];
    if (dbFile == null) {
      return const SizedBox.shrink();
    }
    return Container(
      constraints: const BoxConstraints(minHeight: 80),
      child: TextFormField(
        key: dbFile.state,
        autovalidateMode: AutovalidateMode.onUnfocus,
        controller: dbFile.ctrl,
        validator: validatorFn(context, dbFile, validatorValueRequired(context)),
        decoration: InputDecoration(
          border: OutlineInputBorder(borderRadius: BorderRadius.circular(5)),
          labelText: "Path",
          contentPadding: const EdgeInsets.all(10),
          suffixIcon: Padding(
            padding: const EdgeInsets.only(right: 5),
            child: RectangleIconButton.medium(
              icon: Icons.folder_open,
              tooltip: AppLocalizations.of(context)!.tooltip_select_directory,
              iconColor: Theme.of(context).colorScheme.primary, // file 按钮颜色
              onPressed: () => selectDBFile(context, dbFile),
            ),
          ),
        ),
      ),
    );
  }

  Widget buildUserField(BuildContext context) {
    final user = infos[settingMetaNameUser];
    if (user == null) {
      return const SizedBox.shrink();
    }
    return CommonFormField(
      label: AppLocalizations.of(context)!.db_instance_user,
      controller: user.ctrl,
      state: user.state,
    );
  }

  Widget buildPasswordField(BuildContext context) {
    final password = infos[settingMetaNamePassword];
    if (password == null) {
      return const SizedBox.shrink();
    }
    return CommonFormField(
      label: AppLocalizations.of(context)!.db_instance_password,
      controller: password.ctrl,
      state: password.state,
      obscureText: true,
    );
  }

  Widget buildDescField(BuildContext context) {
    FormInfo desc = infos[settingMetaNameDesc]!;
    return DescFormField(
      controller: desc.ctrl,
      state: desc.state,
    );
  }

  Widget buildCustomField(BuildContext context, String group) {
    if (group == "initize") {
      return Container(
        constraints: const BoxConstraints(maxHeight: 430),
        child: CodeEditor(
          borderRadius: BorderRadius.circular(10),
          style: CodeEditorStyle(
            backgroundColor: Theme.of(context).colorScheme.surfaceContainerLow, // SQL 编辑器背景色
            textStyle: GoogleFonts.robotoMono(
              color: Theme.of(context).colorScheme.onSurface, // SQL 编辑器文字颜色
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
      );
    }
    return ListView(
      children: [
        for (final info in infos.values)
          if (info.meta.group == group && info.meta is CustomMeta)
            CommonFormField(
              state: info.state,
              label: info.meta.name,
              controller: info.ctrl,
              validator: validatorFn(
                context,
                info,
                validatorValueRequired(context),
              ),
            ),
      ],
    );
  }

  bool isGroupValid(String group) {
    for (final info in infos.values) {
      if (info.meta.group == group) {
        if (!info.isValid) {
          return false;
        }
      }
    }
    return true;
  }

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Container(
          constraints: const BoxConstraints(maxWidth: 500),
          child: Column(
            children: [
              Container(
                alignment: Alignment.centerLeft,
                padding: const EdgeInsets.fromLTRB(0, kSpacingTiny, 0, kSpacingTiny),
                child: Text(
                  AppLocalizations.of(context)!.db_base_config,
                  textAlign: TextAlign.left,
                  style: Theme.of(context).textTheme.titleMedium,
                ),
              ),
              const SizedBox(height: kSpacingSmall),
              Expanded(
                child: ListView(
                  children: [
                    if (hasField(settingMetaNameName)) buildNameField(context),
                    if (hasField(settingMetaNameTargetNetworkHost)) buildAddressField(context),
                    if (hasField(settingMetaNameTargetDBFile)) buildDBFileField(context),
                    if (hasField(settingMetaNameUser)) buildUserField(context),
                    if (hasField(settingMetaNamePassword)) buildPasswordField(context),
                    if (hasField(settingMetaNameDesc)) buildDescField(context),
                  ],
                ),
              ),
            ],
          ),
        ),
        const SizedBox(
          width: kSpacingLarge,
        ),
        Expanded(
          child: Container(
            constraints: const BoxConstraints(maxWidth: 500),
            child: Column(
              children: [
                Row(
                  children: [
                    for (var group in groups)
                      TextButton(
                        onPressed: () {
                          onGroupChange?.call(group);
                        },
                        style: TextButton.styleFrom(
                          backgroundColor: group == selectedGroup
                              ? Theme.of(context)
                                    .colorScheme
                                    .primaryContainer // custom config tab selected color
                              : null,
                        ),
                        child: Text(
                          group,
                          textAlign: TextAlign.left,
                          style: Theme.of(
                            context,
                          ).textTheme.titleMedium!.merge(TextStyle(color: !isGroupValid(group) ? Colors.red : null)),
                        ),
                      ),
                  ],
                ),
                const SizedBox(height: kSpacingSmall),
                Expanded(
                  child: IndexedStack(
                    index: selectedGroupIndex,
                    children: [
                      for (final group in groups) buildCustomField(context, group),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }
}

class AddInstanceBottomBar extends StatelessWidget {
  final bool? isDatabaseConnectable;
  final String? databaseConnectError;
  final bool isDatabasePingDoing;

  const AddInstanceBottomBar({
    super.key,
    this.databaseConnectError,
    required this.isDatabasePingDoing,
    this.isDatabaseConnectable,
  });

  @override
  Widget build(BuildContext context) {
    Widget msg;
    Widget status;

    if (isDatabasePingDoing) {
      msg = Text(AppLocalizations.of(context)!.testing);
      status = const Loading.medium();
    } else if (isDatabaseConnectable == null) {
      msg = const Text("");
      status = const SizedBox.shrink();
    } else if (isDatabaseConnectable == true) {
      msg = Text(AppLocalizations.of(context)!.test_success);
      status = const Icon(
        Icons.check_circle,
        size: kIconSizeSmall,
        color: Colors.green,
      );
    } else {
      msg = Text(databaseConnectError ?? "", overflow: TextOverflow.ellipsis);
      status = const Icon(
        Icons.error,
        size: kIconSizeSmall,
        color: Colors.red,
      );
    }

    return Row(
      crossAxisAlignment: CrossAxisAlignment.center,
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(kSpacingSmall, 0, kSpacingSmall, 0),
          child: status,
        ),
        Expanded(child: msg),
      ],
    );
  }
}

class FormInfo {
  DatabaseType dbType;
  final SettingMeta meta;
  TextEditingController ctrl;
  GlobalKey<FormFieldState> state = GlobalKey<FormFieldState>();
  bool isValid = true;

  FormInfo(this.dbType, this.meta) : ctrl = TextEditingController(text: meta.defaultValue);
}

class AddInstanceController extends ChangeNotifier {
  final Map<DatabaseType, Map<String, FormInfo>> infos = {};

  final Map<DatabaseType, CodeLineEditingController> initQueryCodeControllers = {};

  DatabaseType selectedDatabaseType = DatabaseType.mysql;
  String? _selectedGroup;

  bool? isDatabaseConnectable;
  bool isDatabasePingDoing = false;
  String? databaseConnectError;

  CodeLineEditingController get initQueryCodeController {
    return initQueryCodeControllers[selectedDatabaseType]!;
  }

  AddInstanceController() {
    for (var connMeta in connectionMetas) {
      final dbInfos = infos.putIfAbsent(connMeta.type, () => {});
      for (var meta in connMeta.connMeta) {
        dbInfos[meta.name] = FormInfo(connMeta.type, meta);
      }
    }
    // 为每个数据库类型都初始化init query, 切换数据库时要同步改变
    for (var connMeta in connectionMetas) {
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

  String _fieldText(DatabaseType dbType, String fieldName) {
    return infos[dbType]?[fieldName]?.ctrl.text ?? "";
  }

  String _addressFieldText(DatabaseType dbType) {
    final dbInfos = infos[dbType];
    if (dbInfos == null) {
      return "";
    }
    return dbInfos[settingMetaNameTargetNetworkHost]?.ctrl.text ??
        dbInfos[settingMetaNameTargetDBFile]?.ctrl.text ??
        "";
  }

  void _setFieldText(DatabaseType dbType, String fieldName, String value) {
    final field = infos[dbType]?[fieldName];
    if (field != null) {
      field.ctrl.text = value;
    }
  }

  void onDatabaseTypeChange(DatabaseType type) {
    final sourceType = selectedDatabaseType;
    final sourcePortField = infos[sourceType]?[settingMetaNameTargetNetworkPort];
    final sourceDefaultPort = sourcePortField?.meta.defaultValue ?? "";
    final isPortChanged = sourcePortField != null && sourcePortField.ctrl.text != sourceDefaultPort;
    final name = _fieldText(sourceType, settingMetaNameName);
    final desc = _fieldText(sourceType, settingMetaNameDesc);
    final addr = _addressFieldText(sourceType);
    final user = _fieldText(sourceType, settingMetaNameUser);
    final password = _fieldText(sourceType, settingMetaNamePassword);

    selectedDatabaseType = type;
    _selectedGroup = null;

    _setFieldText(type, settingMetaNameName, name);
    _setFieldText(type, settingMetaNameDesc, desc);
    _setFieldText(type, settingMetaNameTargetNetworkHost, addr);
    _setFieldText(type, settingMetaNameTargetDBFile, addr);
    _setFieldText(type, settingMetaNameUser, user);
    _setFieldText(type, settingMetaNamePassword, password);
    // 数据库切换则port 默认值要切换，除非用户自己编辑了特殊端口
    if (!isPortChanged && infos[type]?.containsKey(settingMetaNameTargetNetworkPort) == true) {
      port = defaultPort;
    }
    notifyListeners();
  }

  void clear() {
    for (final dbInfos in infos.values) {
      for (final info in dbInfos.values) {
        info.ctrl.text = info.meta.defaultValue ?? "";
      }
    }
  }

  Map<String, FormInfo> get dbInfos {
    return infos[selectedDatabaseType]!;
  }

  String get defaultPort {
    return infos[selectedDatabaseType]?[settingMetaNameTargetNetworkPort]?.meta.defaultValue ?? "";
  }

  set port(String port) {
    final portField = infos[selectedDatabaseType]?[settingMetaNameTargetNetworkPort];
    if (portField != null) {
      portField.ctrl.text = port;
    }
  }

  bool isGroupValid(String group) {
    for (final info in infos[selectedDatabaseType]!.values) {
      if (info.dbType == selectedDatabaseType && info.meta.group == group) {
        if (!info.isValid) {
          return false;
        }
      }
    }
    return true;
  }

  bool validate() {
    var isValid = true;
    for (final info in infos[selectedDatabaseType]!.values) {
      if (info.dbType == selectedDatabaseType) {
        if (!info.state.currentState!.validate()) {
          isValid = false;
        }
      }
    }
    return isValid;
  }

  void updateValidState(FormInfo info, bool isValid) {
    if (info.isValid == isValid) {
      return;
    }
    info.isValid = isValid;
    notifyListeners();
  }

  List<String> get customSettingGroup {
    final connMeta = connectionMetaMap[selectedDatabaseType]?.connMeta;
    if (connMeta == null) {
      return [];
    }
    return connMeta
        .groupFoldBy<String, List<String>>(
          (meta) => meta.group,
          (previous, meta) => (previous ?? [])..add(meta.group),
        )
        .keys
        .whereNot((e) => e == settingMetaGroupBase)
        .toList();
  }

  String? get selectedGroup {
    return _selectedGroup;
  }

  void onGroupChange(String group) {
    _selectedGroup = group;
    notifyListeners();
  }

  List<SettingMeta> getSettingMeta(String group) {
    final connMeta = connectionMetaMap[selectedDatabaseType]?.connMeta;
    if (connMeta == null) {
      return [];
    }
    return connMeta
        .groupListsBy((meta) => meta.group)
        .entries
        .where((entry) => entry.key == group)
        .expand((entry) => entry.value)
        .toList();
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

    for (final info in infos[selectedDatabaseType]!.values) {
      switch (info.meta) {
        case NameMeta():
          name = info.ctrl.text;
        case TargetNetworkHostMeta():
          addr = info.ctrl.text;
        case TargetDBFileMeta():
          dbFile = info.ctrl.text;
          addr = dbFile;
        case TargetNetworkPortMeta():
          port = int.tryParse(info.ctrl.text);
        case UserMeta():
          user = info.ctrl.text;
        case PasswordMeta():
          password = info.ctrl.text;
        case DescMeta():
          desc = info.ctrl.text;
        case CustomMeta():
          custom[(info.meta as CustomMeta).name] = info.ctrl.text;
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
}

AddInstanceController addInstanceController = AddInstanceController();
