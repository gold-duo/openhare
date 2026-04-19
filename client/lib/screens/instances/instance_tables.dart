import 'package:client/models/instances.dart';
import 'package:client/screens/instances/instance_add.dart';
import 'package:client/screens/instances/instance_update.dart';
import 'package:client/services/instances/instances.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/dialog.dart';
import 'package:client/widgets/const.dart';
import 'package:client/widgets/paginated_bar.dart';
import 'package:db_driver/db_driver.dart';
import 'package:flutter/material.dart';
import 'package:client/screens/page_skeleton.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:client/l10n/app_localizations.dart';

class InstancesPage extends StatelessWidget {
  const InstancesPage({super.key});

  @override
  Widget build(BuildContext context) {
    return const PageSkeleton(
      key: Key("instances"),
      child: InstanceTable(),
    );
  }
}

final instanceSearchTextController = TextEditingController();

class InstanceTable extends ConsumerStatefulWidget {
  const InstanceTable({super.key});

  @override
  ConsumerState<InstanceTable> createState() => _InstanceTableState();
}

class _InstanceTableState extends ConsumerState<InstanceTable> {
  DataRow buildDataRow(InstanceModel instance) {
    return DataRow(
      cells: [
        DataCell(
          Row(
            children: [
              Image.asset(
                connectionMetaMap[instance.dbType]!.logoAssertPath,
                width: kIconSizeMedium,
                height: kIconSizeMedium,
              ),
              Padding(
                padding: const EdgeInsets.only(left: kSpacingSmall),
                child: Text(instance.connectValue.name),
              ),
            ],
          ),
        ),
        DataCell(Text(instance.connectValue.target.toString())),
        DataCell(Text(instance.connectValue.user)),
        DataCell(Text(instance.connectValue.desc, overflow: TextOverflow.ellipsis)),
        DataCell(
          Row(
            children: [
              RectangleIconButton.small(
                icon: Icons.edit,
                onPressed: () {
                  showUpdateInstanceDialog(context, ref, instance);
                },
              ),
              RectangleIconButton.small(
                icon: Icons.delete,
                onPressed: () {
                  doActionDialog(
                    context,
                    AppLocalizations.of(context)!.tip_delete_instance,
                    AppLocalizations.of(context)!.tip_delete_instance_desc,
                    () async {
                      await ref.read(instancesServicesProvider.notifier).deleteInstance(instance.id);
                    },
                    icon: Icon(Icons.delete_outline, color: Theme.of(context).colorScheme.error),
                  );
                },
              ),
              RectangleIconButton.small(
                icon: Icons.more_vert_outlined,
                onPressed: () {},
              ),
            ],
          ),
        ),
      ],
    );
  }

  @override
  Widget build(BuildContext context) {
    final column = [
      DataColumn(
        label: Text(AppLocalizations.of(context)!.db_instance_name),
        columnWidth: const FlexColumnWidth(2),
      ),
      DataColumn(
        label: Text(AppLocalizations.of(context)!.db_instance_target),
        columnWidth: const FlexColumnWidth(3),
      ),
      DataColumn(
        label: Text(AppLocalizations.of(context)!.db_instance_user),
        columnWidth: const FlexColumnWidth(1),
      ),
      DataColumn(
        label: Text(AppLocalizations.of(context)!.db_instance_desc),
        columnWidth: const FlexColumnWidth(3),
      ),
      DataColumn(
        label: Text(AppLocalizations.of(context)!.db_instance_op),
        columnWidth: const FlexColumnWidth(2),
      ),
    ];

    final model = ref.watch(instancesProvider);

    final rows = model.instances.instances.map((instance) => buildDataRow(instance)).toList();

    return BodyPageSkeleton(
      bottomSpaceSize: kSpacingSmall,
      header: Row(
        children: [
          Text(
            AppLocalizations.of(context)!.db_instance,
            style: Theme.of(context).textTheme.titleLarge,
            overflow: TextOverflow.ellipsis,
          ),
          Expanded(
            child: Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                SizedBox(
                  width: kIconSizeLarge,
                  height: kIconSizeLarge,
                  child: FloatingActionButton.small(
                    elevation: 2,
                    onPressed: () => showAddInstanceDialog(context),
                    child: const Icon(Icons.add),
                  ),
                ),
                const SizedBox(width: kSpacingSmall),
                SearchBarTheme(
                  data: const SearchBarThemeData(
                    elevation: WidgetStatePropertyAll(0),
                    constraints: BoxConstraints(
                      minHeight: kIconSizeLarge,
                      maxWidth: 200,
                    ),
                  ),
                  child: SearchBar(
                    controller: instanceSearchTextController,
                    backgroundColor: WidgetStatePropertyAll(
                      Theme.of(context).colorScheme.surfaceContainerLow, // instance 页面搜索框背景色
                    ),
                    side: WidgetStatePropertyAll(
                      BorderSide(
                        color: Theme.of(context).colorScheme.outlineVariant, // instance 页面搜索框边框颜色
                      ),
                    ),
                    onChanged: (value) {
                      ref
                          .read(instancesProvider.notifier)
                          .changePage(
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
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Expanded(
            child: Scrollbar(
              child: SingleChildScrollView(
                scrollDirection: Axis.vertical,
                child: DataTable(
                  checkboxHorizontalMargin: 0,
                  horizontalMargin: 0,
                  columnSpacing: 0,
                  dividerThickness: kDividerThickness,
                  showBottomBorder: true,
                  columns: column,
                  rows: rows,
                  sortAscending: false,
                  showCheckboxColumn: true,
                ),
              ),
            ),
          ),
          TablePaginatedBar(
            count: model.instances.count,
            filteredCount: model.instances.filteredCount,
            pageSize: model.pageSize,
            pageNumber: model.currentPage,
            onChange: (pageNumber) {
              ref
                  .read(instancesProvider.notifier)
                  .changePage(
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
