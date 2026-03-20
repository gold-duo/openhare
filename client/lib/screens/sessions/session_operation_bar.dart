import 'package:client/models/instances.dart';
import 'package:client/models/sessions.dart';
import 'package:client/screens/tasks/export_data.dart';
import 'package:client/screens/tasks/task_overview.dart';
import 'package:client/services/sessions/session_drawer.dart';
import 'package:client/services/sessions/session_sql_result.dart';
import 'package:client/services/sessions/session_conn.dart';
import 'package:client/services/sessions/session_metadata.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/menu.dart';
import 'package:client/widgets/dialog.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/divider.dart';
import 'package:client/widgets/loading.dart';
import 'package:client/widgets/sql_highlight.dart';
import 'package:client/widgets/tooltip.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:sql_parser/parser.dart';
import 'package:hugeicons/hugeicons.dart';
import 'package:sql_editor/re_editor.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:client/l10n/app_localizations.dart';

class SessionOpBar extends ConsumerWidget {
  final CodeLineEditingController codeController;
  final double height;

  const SessionOpBar({
    super.key,
    required this.codeController,
    this.height = 36,
  });

  String getQuery(SessionOpBarModel model) {
    var content = codeController.text.toString();
    List<SQLChunk> querys = splitSQL(
      model.dbType?.dialectType ?? DialectType.mysql,
      content,
      skipWhitespace: true,
      skipComment: true,
    );
    CodeLineSelection s = codeController.selection;
    String query;
    // 当界面手动选中了文本片段则仅执行该片段，当前还不支持多SQL执行.
    if (!s.isCollapsed) {
      query = codeController.selectedText;
    } else {
      Pos cursor = Pos(0, s.baseIndex + 1, s.baseOffset);
      SQLChunk chunk = querys.firstWhere((chunk) {
        if (cursor.between(chunk.start, chunk.end)) {
          return true;
        }
        return false;
      }, orElse: () => SQLChunk.empty());
      query = chunk.content;
    }
    return query.trim();
  }

  void disconnectDialog(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    // 如果正在执行语句，则提示连接繁忙，请稍后执行
    if (SQLConnectState.isBusy(model.state)) {
      return doActionDialog(
        context,
        AppLocalizations.of(context)!.tip_connect_busy,
        AppLocalizations.of(context)!.tip_connect_busy_desc,
        () {
          // do nothing, just close the dialog
        },
        icon: Icon(Icons.warning_amber_rounded, color: Theme.of(context).colorScheme.error),
      );
    }
    return doActionDialog(
      context,
      AppLocalizations.of(context)!.tip_disconnect,
      AppLocalizations.of(context)!.tip_disconnect_desc,
      () async {
        await ref.read(sessionsServicesProvider.notifier).disconnectSession(model.sessionId);
      },
      icon: Icon(Icons.link_off_rounded, color: Theme.of(context).colorScheme.error),
    );
  }

  void connectDialog(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    // 如果是connIsBusy，则提示连接繁忙，请稍后执行
    if (SQLConnectState.isBusy(model.state)) {
      return doActionDialog(
        context,
        AppLocalizations.of(context)!.tip_connect_busy,
        AppLocalizations.of(context)!.tip_connect_busy_desc,
        () {
          // do nothing, just close the dialog
        },
        icon: Icon(Icons.warning_amber_rounded, color: Theme.of(context).colorScheme.error),
      );
    }
    // 如果是connIsDisconnected，则提示连接未建立，请先连接
    return doActionDialog(
      context,
      AppLocalizations.of(context)!.tip_connect,
      AppLocalizations.of(context)!.tip_connect_desc,
      () async {
        await ref.read(sessionsServicesProvider.notifier).connectSession(model.sessionId);
      },
      icon: Icon(Icons.link_rounded, color: Theme.of(context).colorScheme.primary),
    );
  }

  Widget connectWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    if (SQLConnectState.isDisconnected(model.state)) {
      return RectangleIconButton.medium(
        tooltip: AppLocalizations.of(context)!.button_tooltip_connect,
        icon: Icons.link_rounded,
        iconColor: Theme.of(context).colorScheme.primary, // 连接数据库按钮颜色
        onPressed: () async {
          await ref.read(sessionsServicesProvider.notifier).connectSession(model.sessionId);
        },
      );
    } else if (SQLConnectState.isConnecting(model.state)) {
      return const Loading.medium();
    } else {
      // disconnect
      return RectangleIconButton.medium(
        tooltip: AppLocalizations.of(context)!.button_tooltip_disconnect,
        icon: Icons.link_off_rounded,
        iconColor: Theme.of(context).colorScheme.primary, // 连接数据库按钮颜色
        onPressed: () async {
          disconnectDialog(context, ref, model);
        },
      );
    }
  }

  Widget executeWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    return RectangleIconButton.medium(
      tooltip: AppLocalizations.of(context)!.button_tooltip_run_sql,
      icon: Icons.play_circle_outline_rounded,
      iconColor: SQLConnectState.isIdle(model.state) ? Colors.green : Colors.grey,
      onPressed: SQLConnectState.isIdle(model.state)
          ? () {
              String query = getQuery(model);
              if (query.isNotEmpty) {
                final df = parser(model.dbType?.dialectType ?? DialectType.mysql, query);
                if (df.isDangerousSQL) {
                  queryDangerousSQLDialog(
                    context,
                    ref,
                    model.sessionId,
                    model.dbType?.dialectType ?? DialectType.mysql,
                    query,
                  );
                } else {
                  ref.read(sQLResultsServicesProvider.notifier).query(model.sessionId, query);
                }
              }
            }
          : () {
              connectDialog(context, ref, model);
            },
    );
  }

  Widget executeAddWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    return Stack(
      alignment: Alignment.center,
      children: [
        RectangleIconButton.medium(
          tooltip: AppLocalizations.of(context)!.button_tooltip_run_sql_new_tab,
          icon: Icons.not_started_outlined,
          iconColor: SQLConnectState.isIdle(model.state) ? Colors.green : Colors.grey,
          onPressed: SQLConnectState.isIdle(model.state)
              ? () {
                  String query = getQuery(model);
                  if (query.isNotEmpty) {
                    final df = parser(model.dbType?.dialectType ?? DialectType.mysql, query);
                    if (df.isDangerousSQL) {
                      queryDangerousSQLDialog(
                        context,
                        ref,
                        model.sessionId,
                        model.dbType?.dialectType ?? DialectType.mysql,
                        query,
                      );
                    } else {
                      ref.read(sQLResultsServicesProvider.notifier).queryAddResult(model.sessionId, query);
                    }
                  }
                }
              : () {
                  connectDialog(context, ref, model);
                },
        ),
      ],
    );
  }

  Widget explainWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    return RectangleIconButton.medium(
      tooltip: AppLocalizations.of(context)!.button_tooltip_explain_sql,
      icon: Icons.poll_outlined,
      iconColor: SQLConnectState.isIdle(model.state) ? const Color.fromARGB(255, 241, 192, 84) : Colors.grey,
      onPressed: SQLConnectState.isIdle(model.state)
          ? () {
              String query = getQuery(model);
              if (query.isNotEmpty) {
                ref.read(sQLResultsServicesProvider.notifier).queryAddResult(model.sessionId, "explain $query");
              }
            }
          : () {
              connectDialog(context, ref, model);
            },
    );
  }

  Widget exportDataWidget(BuildContext context, SessionOpBarModel model) {
    return RectangleIconButton.medium(
      tooltip: AppLocalizations.of(context)!.button_tooltip_sql_result_download,
      icon: Icons.file_download_sharp,
      iconColor: Colors.green,
      verticalOffset: 1,
      onPressed: () {
        showExportDataDialog(
          context,
          instanceId: model.instanceId!,
          schema: model.currentSchema,
          query: getQuery(model),
          dbType: model.dbType ?? DatabaseType.mysql,
        );
      },
    );
  }

  Widget taskOverviewWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    final l10n = AppLocalizations.of(context)!;

    final childWidget = model.runningTaskCount > 0
        ? const Loading.medium()
        : RectangleIconButton.medium(
            tooltip: l10n.scheduled_task,
            icon: Icons.schedule,
            onPressed: null,
          );

    return TaskOverviewMenu(
      child: childWidget,
    );
  }

  Widget saveWidget(BuildContext context, WidgetRef ref, SessionOpBarModel model) {
    return RectangleIconButton.medium(
      tooltip: AppLocalizations.of(context)!.button_tooltip_save,
      icon: Icons.save,
      onPressed: () {
        ref.read(sessionsServicesProvider.notifier).saveCode(model.sessionId);
      },
    );
  }

  Widget divider(BuildContext context) {
    return const Padding(
      padding: EdgeInsets.symmetric(horizontal: kSpacingTiny),
      child: PixelVerticalDivider(indent: 10, endIndent: 10),
    );
  }

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    SessionOpBarModel? model = ref.watch(sessionOpBarProvider);

    if (model == null) {
      return Container(
        constraints: BoxConstraints(maxHeight: height),
        child: const Spacer(),
      );
    }
    return Container(
      constraints: BoxConstraints(maxHeight: height),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.start,
        crossAxisAlignment: CrossAxisAlignment.center,
        children: [
          // connect
          connectWidget(context, ref, model),

          // schema list
          SchemaBar(
            instanceId: model.instanceId,
            connId: model.connId,
            disable: !SQLConnectState.isIdle(model.state),
            currentSchema: model.currentSchema,
          ),
          divider(context),
          executeWidget(context, ref, model),
          executeAddWidget(context, ref, model),
          explainWidget(context, ref, model),
          exportDataWidget(context, model),
          taskOverviewWidget(context, ref, model),
          divider(context),
          saveWidget(context, ref, model),
          const Expanded(child: SessionDrawerBar()),
        ],
      ),
    );
  }
}

class SchemaBar extends ConsumerStatefulWidget {
  final String? currentSchema;
  final bool disable;
  final InstanceId? instanceId;
  final ConnId? connId;
  final Color? iconColor;

  const SchemaBar({
    super.key,
    this.instanceId,
    required this.disable,
    this.currentSchema,
    this.iconColor,
    this.connId,
  });

  @override
  ConsumerState<SchemaBar> createState() => _SchemaBarState();
}

class _SchemaBarState extends ConsumerState<SchemaBar> {
  bool isEnter = false;
  late final TextEditingController _schemaSearchController;

  @override
  void initState() {
    super.initState();
    _schemaSearchController = TextEditingController();
  }

  @override
  void dispose() {
    _schemaSearchController.dispose();
    super.dispose();
  }

  void _onSchemaSearchChanged() {
    setState(() {});
  }

  List<String> _filteredSchemas(List<String> schemas, String searchText) {
    if (searchText.isEmpty) return schemas;
    return schemas.where((s) => s.toLowerCase().contains(searchText.toLowerCase())).toList();
  }

  @override
  Widget build(BuildContext context) {
    final schemasAsync = ref.watch(selectedSessionSchemaProvider);
    final color = (isEnter && !widget.disable)
        ? Theme.of(context).colorScheme.primary
        : Theme.of(context).colorScheme.onSurface; // schema 框鼠标移入的颜色

    final schemaBarContent = Padding(
      padding: const EdgeInsets.symmetric(horizontal: 5),
      child: MouseRegion(
        onEnter: (_) => setState(() => isEnter = true),
        onExit: (_) => setState(() => isEnter = false),
        child: Container(
          padding: const EdgeInsets.fromLTRB(0, kSpacingTiny, 0, kSpacingTiny),
          child: Row(
            children: [
              HugeIcon(
                icon: HugeIcons.strokeRoundedDatabase,
                color: color,
                size: kIconSizeSmall,
              ),
              Container(
                padding: const EdgeInsets.only(left: kSpacingTiny),
                width: 120,
                child: Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    widget.currentSchema ?? "",
                    overflow: TextOverflow.ellipsis,
                    style: TextStyle(color: color),
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );

    final tabs = schemasAsync.when(
      loading: () => [
        OverlayMenuItem(
          height: 72,
          child: const Center(child: Loading.medium()),
        ),
      ],
      error: (error, _) => [
        OverlayMenuItem(
          height: 36,
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
            child: Align(
              alignment: Alignment.centerLeft,
              child: TooltipText(
                text: error.toString(),
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            ),
          ),
        ),
      ],
      data: (schemas) {
        final filteredSchemas = _filteredSchemas(schemas, _schemaSearchController.text);
        if (filteredSchemas.isEmpty) {
          return [
            OverlayMenuItem(
              height: 36,
              child: Padding(
                padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
                child: Align(
                  alignment: Alignment.centerLeft,
                  child: Text(AppLocalizations.of(context)!.display_msg_no_data),
                ),
              ),
            ),
          ];
        }
        return filteredSchemas.map((schema) {
          final isSelected = schema == widget.currentSchema;
          final color = isSelected
              ? Theme.of(context).colorScheme.primary
              : Theme.of(context).colorScheme.onSurface; // schema list 里当前schema的颜色

          return OverlayMenuItem(
            height: 30,
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: kSpacingSmall),
              child: Align(
                alignment: Alignment.centerLeft,
                child: Row(
                  children: [
                    HugeIcon(
                      icon: HugeIcons.strokeRoundedDatabase,
                      color: color,
                      size: kIconSizeSmall,
                    ),
                    const SizedBox(width: kSpacingTiny),
                    Expanded(
                      child: TooltipText(
                        text: schema,
                        style: TextStyle(color: color),
                      ),
                    ),
                  ],
                ),
              ),
            ),
            onTabSelected: () async {
              await ref.read(sessionConnsServicesProvider.notifier).setCurrentSchema(widget.connId!, schema);
            },
          );
        }).toList();
      },
    );

    if (widget.disable) {
      return schemaBarContent;
    }

    final header = OverlayMenuHeader(
      height: 42,
      child: Padding(
        padding: const EdgeInsets.fromLTRB(kSpacingSmall, kSpacingSmall, kSpacingSmall, kSpacingTiny),
        child: SearchBarTheme(
          data: SearchBarThemeData(
            textStyle: WidgetStatePropertyAll(Theme.of(context).textTheme.bodySmall),
            backgroundColor: WidgetStatePropertyAll(
              Theme.of(context).colorScheme.surfaceContainerLowest, // schema 页面搜索框背景色
            ),
            elevation: const WidgetStatePropertyAll(0),
            constraints: const BoxConstraints(minHeight: 24),
          ),
          child: SearchBar(
            // todo: 抽取搜索框, 哪里都一样重复的代码
            side: WidgetStatePropertyAll(
              BorderSide(
                color: Theme.of(context).colorScheme.outlineVariant, // session 页面搜索框边框颜色
              ),
            ),
            controller: _schemaSearchController,
            onChanged: (_) => _onSchemaSearchChanged(),
            trailing: const [Icon(Icons.search, size: kIconSizeSmall)],
          ),
        ),
      ),
    );

    final footer = OverlayMenuFooter(
      height: kIconButtonSizeSmall + kSpacingTiny * 2,
      child: Padding(
        padding: const EdgeInsets.only(right: kSpacingSmall, top: kSpacingTiny, bottom: kSpacingTiny),
        child: Align(
          alignment: Alignment.bottomRight,
          child: RectangleIconButton.small(
            tooltip: AppLocalizations.of(context)!.button_tooltip_refresh_metadata,
            icon: Icons.refresh,
            onPressed: () async {
              ref.read(selectedSessionMetadataProvider.notifier).refreshMetadata();
            },
          ),
        ),
      ),
    );

    return OverlayMenu(
      spacing: kSpacingTiny,
      maxHeight: 300,
      maxWidth: 300,
      tabs: tabs,
      header: header,
      footer: footer,
      child: schemaBarContent,
    );
  }
}

class SessionDrawerBar extends ConsumerWidget {
  final double height;

  const SessionDrawerBar({
    super.key,
    this.height = 36,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final model = ref.watch(sessionDrawerProvider);
    final services = ref.read(sessionDrawerServicesProvider(model.sessionId).notifier);
    return Row(
      mainAxisAlignment: MainAxisAlignment.start,
      crossAxisAlignment: CrossAxisAlignment.center,
      children: [
        const Spacer(),
        if (model.isRightPageOpen) ...[
          RectangleIconButton.medium(
            tooltip: AppLocalizations.of(context)!.button_tooltip_metadata_tree,
            icon: Icons.account_tree_outlined,
            backgroundColor: (model.drawerPage == DrawerPage.metadataTree)
                ? Theme.of(context)
                      .colorScheme
                      .primaryContainer // 元数据tree页面 icon 背景色
                : null,
            onPressed: () {
              services.goToTree();
            },
          ),
          RectangleIconButton.medium(
            tooltip: AppLocalizations.of(context)!.button_tooltip_sql_result,
            icon: Icons.article_outlined,
            backgroundColor: (model.drawerPage == DrawerPage.sqlResult)
                ? Theme.of(context)
                      .colorScheme
                      .primaryContainer // 结果页面 icon 背景色
                : null,
            onPressed: () {
              services.showSQLResult();
            },
          ),
          // AI chat
          RectangleIconButton.medium(
            tooltip: AppLocalizations.of(context)!.button_tooltip_ai_chat,
            icon: Icons.auto_awesome,
            iconColor: Colors.purple[600]!,
            backgroundColor: (model.drawerPage == DrawerPage.aiChat)
                ? Theme.of(context)
                      .colorScheme
                      .primaryContainer // AI chat 页面 icon 背景色
                : null,
            onPressed: () {
              services.showChat();
            },
          ),
          const SizedBox(width: kSpacingSmall),
          RectangleIconButton.medium(
            icon: model.isRightPageOpen ? Icons.menu : Icons.menu_open,
            iconColor: Theme.of(context).colorScheme.onSurface, // 关闭drawer页面 icon 颜色
            onPressed: () => services.hideRightPage(),
          ),
        ],
        if (!model.isRightPageOpen)
          RectangleIconButton.medium(
            icon: model.isRightPageOpen ? Icons.menu : Icons.menu_open,
            iconColor: Theme.of(context).colorScheme.onSurface, // 打开drawer页面 icon 颜色
            onPressed: () => services.showRightPage(),
          ),
      ],
    );
  }
}

void queryDangerousSQLDialog(
  BuildContext context,
  WidgetRef ref,
  SessionId sessionId,
  DialectType dialectType,
  String query,
) {
  final textTheme = Theme.of(context).textTheme;
  showDialog(
    context: context,
    builder: (_) => CustomDialog(
      title: "tips",
      titleIcon: Icon(
        Icons.play_circle_outline_rounded,
        color: Colors.green,
      ),
      maxHeight: 420,
      content: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(
            AppLocalizations.of(context)!.tip_dangerous_sql_desc,
            style: textTheme.titleMedium,
          ),
          const SizedBox(height: kSpacingMedium),
          Expanded(
            child: Container(
              padding: const EdgeInsets.symmetric(
                horizontal: kSpacingSmall,
                vertical: kSpacingMedium,
              ),
              decoration: BoxDecoration(
                borderRadius: BorderRadius.circular(6),
                border: Border.all(
                  color: Theme.of(context).colorScheme.outline,
                ),
              ),
              child: SingleChildScrollView(
                scrollDirection: Axis.vertical,
                child: SingleChildScrollView(
                  scrollDirection: Axis.horizontal,
                  child: SelectableText.rich(
                    getSQLHighlightTextSpan(
                      dialectType,
                      query,
                      defalutStyle: GoogleFonts.robotoMono(
                        textStyle: textTheme.bodySmall,
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ],
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: Text(AppLocalizations.of(context)!.cancel),
        ),
        const SizedBox(width: kSpacingSmall),
        TextButton(
          onPressed: () {
            ref.read(sQLResultsServicesProvider.notifier).query(sessionId, query);
            Navigator.of(context).pop();
          },
          child: Text(AppLocalizations.of(context)!.confirm),
        ),
      ],
    ),
  );
}
