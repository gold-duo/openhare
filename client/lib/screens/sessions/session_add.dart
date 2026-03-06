import 'package:client/screens/page_skeleton.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/screens/instances/instance_tables.dart';
import 'package:client/services/sessions/sessions.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/divider.dart';
import 'package:client/widgets/empty.dart';
import 'package:client/widgets/paginated_bar.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:client/l10n/app_localizations.dart';

const double paddingButtonLeftSize = 10.0; // 为了使其他组件与`最近使用的数据库`的TextButton对齐

class AddSession extends HookConsumerWidget {
  const AddSession({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final model = ref.watch(instancesProvider);

    if (model.instances.count == 0) {
      return EmptyPage(
        color: Theme.of(context).colorScheme.surfaceContainerLow,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            Text(
              AppLocalizations.of(context)!.display_no_instance_and_add_instance,
              style: Theme.of(context).textTheme.bodySmall,
            ),
            const SizedBox(height: kSpacingSmall),
            LinkButton(
              text: AppLocalizations.of(context)!.display_no_instance_and_add_instance_button,
              onPressed: () {
                GoRouter.of(context).go('/instances/add');
              },
            ),
          ],
        ),
      );
    }

    return BodyPageSkeleton(
      header: Row(
        children: [
          Text(
            AppLocalizations.of(context)!.recently_used_db_instance,
            style: Theme.of(context).textTheme.titleLarge,
          )
        ],
      ),
      child: Column(
        children: [
          Row(
            children: [
              SizedBox(width: 200, child: Text(AppLocalizations.of(context)!.db_instance_name)),
              Container(
                  padding: EdgeInsets.only(left: paddingButtonLeftSize),
                  child: Text(AppLocalizations.of(context)!.recently_used_schema))
            ],
          ),
          const SizedBox(height: kSpacingSmall),
          for (var inst in ref.read(instancesServicesProvider.notifier).activeInstances()) // todo
            LayoutBuilder(
              builder: (context, constraints) {
                // 计算schema按钮的最大宽度
                final schemaCount = inst.activeSchemas.length;
                // 预留200给左侧instance，剩余宽度分配给schema
                final availableWidth = constraints.maxWidth - 200;
                final schemaButtonWidth = schemaCount > 0 ? (availableWidth / schemaCount).clamp(80.0, 200.0) : 0.0;
                return Row(
                  // mainAxisAlignment: MainAxisAlignment。,
                  children: [
                    SizedBox(
                      width: 200,
                      child: Row(
                        children: [
                          Image.asset(
                            connectionMetaMap[inst.dbType]!.logoAssertPath,
                            height: 24,
                          ),
                          LinkButton(
                              onPressed: () {
                                ref.read(sessionsServicesProvider.notifier).addSession(inst);
                              },
                              text: inst.connectValue.name),
                        ],
                      ),
                    ),
                    for (final schema in inst.activeSchemas.toList())
                      LinkButton(
                        text: schema,
                        maxWidth: schemaButtonWidth,
                        onPressed: () {
                          ref.read(sessionsServicesProvider.notifier).addSession(inst, schema: schema);
                        },
                      ),
                  ],
                );
              },
            ),
          const SizedBox(height: kSpacingMedium),
          Row(
            children: [
              Text(
                AppLocalizations.of(context)!.db_instance,
                style: Theme.of(context).textTheme.titleLarge,
              ),
              Expanded(
                child: Container(
                  padding: const EdgeInsets.only(right: 5),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.end,
                    children: [
                      SearchBarTheme(
                        data: const SearchBarThemeData(
                            elevation: WidgetStatePropertyAll(0),
                            constraints: BoxConstraints(
                              minHeight: kIconSizeLarge,
                              maxWidth: 200,
                            )),
                        child: SearchBar(
                          controller: instanceSearchTextController,
                          backgroundColor: WidgetStatePropertyAll(
                            Theme.of(context).colorScheme.surfaceContainerLow,
                          ),
                          side: WidgetStatePropertyAll(
                            BorderSide(
                              color: Theme.of(context).colorScheme.surfaceContainerHigh,
                              width: 0.5,
                            ),
                          ),
                          onChanged: (value) {
                            ref.read(instancesProvider.notifier).changePage(
                                  value,
                                  pageNumber: model.currentPage,
                                  pageSize: model.pageSize,
                                );
                          },
                          trailing: const [Icon(Icons.search)],
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: kSpacingSmall),
          const PixelDivider(),
          const SizedBox(height: kSpacingMedium),
          Expanded(
            child: Scrollbar(
              child: SingleChildScrollView(
                scrollDirection: Axis.vertical,
                child: DataTable(
                  dataRowMinHeight: 42,
                  dataRowMaxHeight: 42,
                  headingRowHeight: 42,
                  horizontalMargin: 0,
                  columnSpacing: 0,
                  columns: [
                    DataColumn(
                      label: Text(AppLocalizations.of(context)!.db_instance_name),
                      columnWidth: const FixedColumnWidth(200),
                    ),
                    DataColumn(
                      label: Padding(
                        padding: EdgeInsets.only(left: paddingButtonLeftSize),
                        child: Text(AppLocalizations.of(context)!.db_instance_target),
                      ),
                      columnWidth: const FlexColumnWidth(1),
                    ),
                    DataColumn(
                      label: Text(AppLocalizations.of(context)!.db_instance_user),
                      columnWidth: const FixedColumnWidth(120),
                    ),
                    DataColumn(
                      label: Text(AppLocalizations.of(context)!.db_instance_desc),
                      columnWidth: const FlexColumnWidth(1),
                    ),
                  ],
                  rows: model.instances.instances.map((inst) {
                    return DataRow(
                      cells: [
                        DataCell(
                          Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              Image.asset(
                                connectionMetaMap[inst.dbType]!.logoAssertPath,
                                height: kIconSizeMedium,
                              ),
                              LinkButton(
                                text: inst.connectValue.name,
                                onPressed: () {
                                  ref.read(sessionsServicesProvider.notifier).addSession(inst);
                                },
                              ),
                            ],
                          ),
                        ),
                        DataCell(
                          Padding(
                            padding: EdgeInsets.only(left: paddingButtonLeftSize),
                            child: Text(inst.connectValue.target.toString(), overflow: TextOverflow.ellipsis),
                          ),
                        ),
                        DataCell(
                          Text(inst.connectValue.user, overflow: TextOverflow.ellipsis),
                        ),
                        DataCell(
                          Text(inst.connectValue.desc, overflow: TextOverflow.ellipsis),
                        ),
                      ],
                    );
                  }).toList(),
                ),
              ),
            ),
          ),
          TablePaginatedBar(
            count: model.instances.count,
            pageSize: model.pageSize,
            pageNumber: model.currentPage,
            onChange: (pageNumber) {
              ref.read(instancesProvider.notifier).changePage(
                    instanceSearchTextController.text,
                    pageNumber: pageNumber,
                    pageSize: model.pageSize,
                  );
            },
          ),
        ],
      ),
    );
  }
}
