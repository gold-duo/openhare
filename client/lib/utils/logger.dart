import 'dart:async';
import 'dart:io';
import 'package:logger/logger.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

final BootLogger bootLogger = BootLogger();

Logger get log => bootLogger.logger;

class BootLogger {
  static const String _fileName = 'boot.log';

  late final Logger logger;

  String? path;

  Future<void> init() async {
    final outputs = <LogOutput>[ConsoleOutput()];

    try {
      /// macOS:   `~/Library/Application Support/openhare/logs/boot.log`
      /// Windows: `%APPDATA%\openhare\logs\boot.log`
      final dir = Directory(
        p.join((await getApplicationSupportDirectory()).path, 'logs'),
      );
      await dir.create(recursive: true);
      final file = File(p.join(dir.path, _fileName));
      path = file.path;
      outputs.add(FileOutput(file: file, overrideExisting: true));
    } catch (_) {
      // 文件不可用时仍保留控制台输出。
    }

    logger = Logger(
      filter: ProductionFilter(),
      printer: SimplePrinter(printTime: true, colors: false),
      output: MultiOutput(outputs),
    );
    Logger.level = Level.trace;
    await logger.init;
  }
}
