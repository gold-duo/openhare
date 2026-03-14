import 'dart:io';

import 'package:client/l10n/app_localizations.dart';
import 'package:client/models/instances.dart';
import 'package:client/models/tasks.dart';
import 'package:client/services/ai/agent.dart';
import 'package:client/services/tasks/export_data.dart';
import 'package:client/utils/file_utils.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/dialog.dart';
import 'package:client/widgets/loading.dart';
import 'package:client/widgets/sql_highlight.dart';
import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:google_fonts/google_fonts.dart';

class _NoScrollbarBehavior extends ScrollBehavior {
  @override
  Widget buildScrollbar(BuildContext context, Widget child, ScrollableDetails details) {
    return child;
  }
}

Future<ExportDataParameters?> showExportDataDialog(
  BuildContext context, {
  required InstanceId instanceId,
  required String schema,
  required String query,
}) async {
  return showDialog<ExportDataParameters>(
    context: context,
    builder: (_) => _ExportDataDialogContent(
      instanceId: instanceId,
      schema: schema,
      query: query,
    ),
  );
}

class _ExportDataDialogContent extends ConsumerStatefulWidget {
  final InstanceId instanceId;
  final String schema;
  final String query;

  const _ExportDataDialogContent({
    required this.instanceId,
    required this.schema,
    required this.query,
  });

  @override
  ConsumerState<_ExportDataDialogContent> createState() => _ExportDataDialogContentState();
}

class _ExportDataDialogContentState extends ConsumerState<_ExportDataDialogContent> {
  bool _isGenerating = false;
  String? _errorMessage;
  late final TextEditingController dirController;
  late final TextEditingController fileNameController;
  late final TextEditingController descController;
  final _formKey = GlobalKey<FormState>();
  final _dirFieldKey = GlobalKey<FormFieldState<String>>();

  @override
  void initState() {
    super.initState();
    // 填充文件名
    fileNameController = TextEditingController(
      text: 'export-${DateTime.now().toIso8601String().split('.').first.replaceAll(':', '-')}.csv',
    );
    // 自动填充目录, 确保有权限访问
    final latestTask = ref.read(latestExportTaskProvider);
    final latestDir = latestTask?.parameters?.fileDir;
    if (latestDir != null && checkDirectoryAccessible(latestDir) == null) {
      dirController = TextEditingController(text: latestDir);
    } else {
      dirController = TextEditingController();
    }

    descController = TextEditingController();
  }

  @override
  void dispose() {
    dirController.dispose();
    fileNameController.dispose();
    descController.dispose();
    super.dispose();
  }

  Future<void> _selectDirectory() async {
    final directory = await FilePicker.platform.getDirectoryPath(
      dialogTitle: AppLocalizations.of(context)!.display_msg_downlaod,
    );
    if (directory != null) {
      dirController.text = directory;
      // 触发验证
      _dirFieldKey.currentState?.validate();
    }
  }

  ExportDataParameters _getExportDataParameters() {
    return ExportDataParameters(
      instanceId: widget.instanceId,
      schema: widget.schema,
      query: widget.query,
      fileDir: dirController.text.trim(),
      fileName: fileNameController.text.trim(),
    );
  }

  void _handleSubmit() {
    if (!_formKey.currentState!.validate()) {
      return;
    }

    final parameters = _getExportDataParameters();

    ref
        .read(exportDataTasksServicesProvider.notifier)
        .exportData(
          parameters,
          desc: descController.text,
        );

    Navigator.of(context).pop();
  }

  Future<void> _generateFileNameWithAI() async {
    final llmAgents = ref.read(lLMAgentProvider);
    final lastUsedAgent = llmAgents.lastUsedLLMAgent;

    if (lastUsedAgent == null) {
      setState(() {
        _errorMessage = AppLocalizations.of(context)!.error_llm_agent_not_found;
      });
      return;
    }

    setState(() {
      _errorMessage = null;
      _isGenerating = true;
    });

    try {
      // 构建参数
      final parameters = _getExportDataParameters();

      // 调用AI生成文件名和描述
      final result = await ref
          .read(lLMAgentServiceProvider.notifier)
          .generateExportFileName(
            lastUsedAgent.id,
            parameters,
          );

      // 设置文件名，确保带有.csv后缀
      String fileName = result.fileName.trim();
      if (!fileName.toLowerCase().endsWith('.csv')) {
        fileName = '$fileName.csv';
      }
      fileNameController.text = fileName;

      // 设置描述（如果有）
      if (result.desc != null && result.desc!.isNotEmpty) {
        descController.text = result.desc!;
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _errorMessage = AppLocalizations.of(context)!.error_generate_file_name_failed(e.toString());
        });
      }
    } finally {
      if (mounted) {
        setState(() {
          _isGenerating = false;
        });
      }
    }
  }

  Widget _buildTaskInfoCard() {
    final textStyle = Theme.of(context).textTheme.bodyMedium;

    return Row(
      children: [
        Expanded(
          child: Container(
            padding: const EdgeInsets.symmetric(
              horizontal: kSpacingSmall,
              vertical: kSpacingMedium,
            ),
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(6),
              border: Border.all(color: Theme.of(context).colorScheme.outlineVariant), // 导出任务SQL信息卡片边框颜色
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                RichText(
                  text: TextSpan(
                    style: textStyle,
                    children: [
                      TextSpan(text: AppLocalizations.of(context)!.export_data_exporting),
                      const TextSpan(text: ' '),
                      TextSpan(
                        text: '`${widget.schema}`',
                        style: textStyle?.copyWith(
                          color: Theme.of(context).colorScheme.primary, // 导出任务SQL信息卡片schema高亮颜色
                        ),
                      ),
                      TextSpan(text: ' ${AppLocalizations.of(context)!.export_data_schema_sql}'),
                    ],
                  ),
                ),
                const SizedBox(height: kSpacingMedium),
                Expanded(
                  child: ScrollConfiguration(
                    behavior: _NoScrollbarBehavior(),
                    child: SingleChildScrollView(
                      scrollDirection: Axis.vertical,
                      child: SingleChildScrollView(
                        scrollDirection: Axis.horizontal,
                        child: SelectableText.rich(
                          getSQLHighlightTextSpan(
                            widget.query,
                            defalutStyle: GoogleFonts.robotoMono(textStyle: Theme.of(context).textTheme.bodySmall),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildFileNameSuffixIcons() {
    return Container(
      padding: const EdgeInsets.only(right: kSpacingTiny),
      width: kIconButtonSizeSmall + kIconButtonSizeMedium + kSpacingTiny,
      child: Row(
        mainAxisAlignment: MainAxisAlignment.end,
        children: [
          if (_errorMessage != null)
            RectangleIconButton.small(
              icon: Icons.error_outline,
              iconColor: Theme.of(context).colorScheme.error,
              tooltip: _errorMessage!,
            ),
          (_isGenerating)
              ? const Loading.medium()
              : RectangleIconButton.medium(
                  icon: Icons.auto_awesome,
                  tooltip: AppLocalizations.of(context)!.tooltip_ai_generate_file_name,
                  iconColor: Colors.purple[600]!,
                  onPressed: _generateFileNameWithAI,
                ),
        ],
      ),
    );
  }

  InputDecoration _buildInputDecoration({
    required String labelText,
    Widget? suffixIcon,
    bool required = false,
  }) {
    final defaultBorder = OutlineInputBorder(
      borderSide: BorderSide(
        color: Theme.of(context).colorScheme.outline, // 输入框边框颜色
      ),
    );
    final errorBorderStyle = OutlineInputBorder(
      borderSide: BorderSide(
        color: Theme.of(context).colorScheme.error,
      ),
    );

    return InputDecoration(
      label: required
          ? Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(labelText),
                Text('*', style: TextStyle(color: Theme.of(context).colorScheme.error)),
              ],
            )
          : Text(labelText),
      border: defaultBorder,
      enabledBorder: defaultBorder,
      disabledBorder: defaultBorder,
      focusedBorder: defaultBorder,
      errorBorder: errorBorderStyle,
      focusedErrorBorder: errorBorderStyle,
      suffixIcon: suffixIcon,
    );
  }

  @override
  Widget build(BuildContext context) {
    return CustomDialog(
      title: AppLocalizations.of(context)!.export_data_title,
      titleIcon: const RectangleIconButton.medium(
        icon: Icons.file_download,
        iconColor: Colors.green,
        verticalOffset: 1.5,
      ),
      content: Form(
        key: _formKey,
        child: Column(
          mainAxisSize: MainAxisSize.max,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // 目录选择
            TextFormField(
              key: _dirFieldKey,
              controller: dirController,
              autovalidateMode: AutovalidateMode.onUserInteraction,
              validator: (value) {
                if (value == null || value.trim().isEmpty) {
                  return AppLocalizations.of(context)!.export_data_directory_required;
                }
                // 使用工具函数验证目录路径
                final error = checkDirectoryAccessible(value.trim());
                if (error == null) {
                  return null;
                }
                // 根据错误类型返回国际化消息
                final localizations = AppLocalizations.of(context)!;
                switch (error) {
                  case DirectoryAccessError.notExists:
                    return localizations.error_directory_not_exists;
                  case DirectoryAccessError.noPermission:
                    // macOS 需要用户自己选择授权，使用不同的提示消息
                    if (Platform.isMacOS) {
                      return localizations.error_directory_no_permission_macos;
                    } else {
                      return localizations.error_directory_no_permission;
                    }
                }
              },
              decoration: _buildInputDecoration(
                labelText: AppLocalizations.of(context)!.export_data_directory_label,
                required: true,
                suffixIcon: Padding(
                  padding: const EdgeInsets.only(right: 5),
                  child: RectangleIconButton.medium(
                    icon: Icons.folder_open,
                    tooltip: AppLocalizations.of(context)!.tooltip_select_directory,
                    iconColor: Theme.of(context).colorScheme.primary, // 导出任务的目录选择按钮颜色
                    onPressed: _selectDirectory,
                  ),
                ),
              ),
            ),
            const SizedBox(height: kSpacingMedium),
            // 文件名输入
            TextFormField(
              controller: fileNameController,
              enabled: !_isGenerating,
              autovalidateMode: AutovalidateMode.onUserInteraction,
              validator: (value) {
                if (value == null || value.trim().isEmpty) {
                  return AppLocalizations.of(context)!.export_data_file_name_required;
                }
                return null;
              },
              decoration: _buildInputDecoration(
                labelText: AppLocalizations.of(context)!.task_column_file_name,
                required: true,
                suffixIcon: _buildFileNameSuffixIcons(),
              ),
            ),
            const SizedBox(height: kSpacingMedium),
            // 备注
            TextField(
              controller: descController,
              enabled: !_isGenerating,
              decoration: _buildInputDecoration(
                labelText: AppLocalizations.of(context)!.db_instance_desc,
              ),
              maxLines: 2,
            ),
            const SizedBox(height: kSpacingMedium),
            // 任务信息卡片
            Expanded(child: _buildTaskInfoCard()),
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: Text(AppLocalizations.of(context)!.cancel),
        ),
        const SizedBox(width: kSpacingSmall),
        TextButton(
          onPressed: _handleSubmit,
          child: Text(AppLocalizations.of(context)!.submit),
        ),
      ],
    );
  }
}
