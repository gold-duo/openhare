import 'package:client/l10n/app_localizations.dart';
import 'package:client/widgets/button.dart';
import 'package:client/widgets/const.dart';
import 'package:flutter/material.dart';

class TablePaginatedBar extends StatelessWidget {
  final int count;
  final int filteredCount;
  final int pageSize;
  final int pageNumber;
  final void Function(int pageNumber) onChange;

  const TablePaginatedBar({
    super.key,
    required this.count,
    required this.filteredCount,
    required this.pageSize,
    required this.pageNumber,
    required this.onChange,
  });

  @override
  Widget build(BuildContext context) {
    int totalPageNumber = (filteredCount / pageSize).ceil();
    if (totalPageNumber < 1) totalPageNumber = 1;
    bool isFirstPage = (pageNumber <= 1);
    bool isLastPage = (pageNumber >= totalPageNumber);
    return Container(
      padding: const EdgeInsets.only(top: kSpacingSmall, bottom: kSpacingSmall, right: kSpacingSmall),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.end,
        children: [
          Padding(
            padding: const EdgeInsets.only(right: kIconSizeSmall),
            child: Text(AppLocalizations.of(context)!.paginated_total_filtered(count, filteredCount)),
          ),
          Padding(
            padding: const EdgeInsets.only(right: kIconSizeSmall),
            child: Text(AppLocalizations.of(context)!.paginated_page(pageNumber, totalPageNumber)),
          ),
          RectangleIconButton.medium(
            icon: Icons.first_page,
            iconColor: isFirstPage ? Theme.of(context).colorScheme.onSurfaceVariant : null,
            onPressed: isFirstPage ? null : () => onChange(1),
          ),
          RectangleIconButton.medium(
            icon: Icons.keyboard_arrow_left,
            iconColor: isFirstPage ? Theme.of(context).colorScheme.onSurfaceVariant : null,
            onPressed: isFirstPage ? null : () => onChange(pageNumber - 1),
          ),
          RectangleIconButton.medium(
            icon: Icons.keyboard_arrow_right_outlined,
            iconColor: isLastPage ? Theme.of(context).colorScheme.onSurfaceVariant : null,
            onPressed: isLastPage ? null : () => onChange(pageNumber + 1),
          ),
          RectangleIconButton.medium(
            icon: Icons.last_page,
            iconColor: isLastPage ? Theme.of(context).colorScheme.onSurfaceVariant : null,
            onPressed: isLastPage ? null : () => onChange(totalPageNumber),
          ),
        ],
      ),
    );
  }
}
