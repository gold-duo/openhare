import 'package:freezed_annotation/freezed_annotation.dart';

part 'db_driver_conn_meta.freezed.dart';
part 'db_driver_conn_meta.g.dart';

enum DatabaseType { mysql, pg, oracle, mssql, sqlite }

enum ConnectTargetType { network, dbFile }

@Freezed(toStringOverride: false)
abstract class ConnectTarget with _$ConnectTarget {
  const ConnectTarget._();
  const factory ConnectTarget.network({required String host, required int port}) = _ConnectTargetNetwork;
  const factory ConnectTarget.dbFile({required String dbFile}) = _ConnectTargetDbFile;

  factory ConnectTarget.fromJson(Map<String, dynamic> json) => _$ConnectTargetFromJson(json);

  @override
  String toString() {
    return when(
      dbFile: (dbFile) => dbFile.trim(),
      network: (host, port) => "${host.trim()}:$port",
    );
  }
}

const String settingMetaGroupBase = "base";

const String settingMetaNameName = "name";
const String settingMetaNameUser = "user";
const String settingMetaNamePassword = "password";
const String settingMetaNameTargetDBFile = "tartget_db_file";
const String settingMetaNameTargetNetworkHost = "tartget_network_host";
const String settingMetaNameTargetNetworkPort = "tartget_network_port";
const String settingMetaNameDesc = "desc";

class ConnectionMeta {
  String displayName;
  DatabaseType type;
  // todo: 这里反向依赖了flutter 主体的 asserts
  String logoAssertPath;

  List<SettingMeta> connMeta = [];

  List<String> initQuerys;

  ConnectionMeta(
      {required this.connMeta,
      required this.type,
      this.logoAssertPath = "",
      required this.displayName,
      this.initQuerys = const []});

  String initQueryText() {
    if (initQuerys.isEmpty) {
      return "";
    }
    return initQuerys.map((e) {
      e = e.trimRight();
      if (e.endsWith(";")) {
        return e;
      } else {
        return "$e;";
      }
    }).join("\n");
  }
}

sealed class SettingMeta {
  final String group;

  String get name;

  String? get defaultValue => "";

  SettingMeta({this.group = settingMetaGroupBase});
}

class NameMeta extends SettingMeta {
  @override
  String get name => settingMetaNameName;
}

class TargetNetworkHostMeta extends SettingMeta {
  @override
  String get name => settingMetaNameTargetNetworkHost;
}

class TargetNetworkPortMeta extends SettingMeta {
  @override
  String get name => settingMetaNameTargetNetworkPort;

  @override
  String? defaultValue; // set default port for host:port
  
  TargetNetworkPortMeta(this.defaultValue);
}

class UserMeta extends SettingMeta {
  @override
  String get name => settingMetaNameUser;
}

class PasswordMeta extends SettingMeta {
  @override
  String get name => settingMetaNamePassword;
}

class TargetDBFileMeta extends SettingMeta {
  @override
  String get name => settingMetaNameTargetDBFile;
}

class DescMeta extends SettingMeta {
  @override
  String get name => settingMetaNameDesc;
}

class CustomMeta extends SettingMeta {
  @override
  String name;
  String type;

  @override
  String? defaultValue;
  String? comment;
  bool isRequired = false;

  CustomMeta(
      {required this.name,
      required this.type,
      required super.group,
      this.defaultValue,
      this.comment,
      this.isRequired = false});
}

class ConnectValue {
  String name;
  ConnectTarget target;
  String user;
  String password;
  String desc;
  Map<String, String> custom = {};
  List<String> initQuerys;

  ConnectValue(
      {required this.name,
      required this.target,
      required this.user,
      required this.password,
      required this.desc,
      required this.custom,
      this.initQuerys = const []});

  String getHost() {
    return target.when(
      network: (host, port) => host.trim(),
      dbFile: (dbFile) => "",
    );
  }

  int? getPort() {
    return target.when(
      network: (host, port) => port,
      dbFile: (dbFile) => null,
    );
  }

  String getDbFile() {
    return target.when(
      dbFile: (dbFile) => dbFile.trim(),
      network: (host, port) => "",
    );
  }

  String endpointText() {
    return target.toString();
  }

  String getValue(String name, [String defaultValue = ""]) {
    return custom[name] ?? defaultValue;
  }

  int getIntValue(String name, [int defaultValue = 0]) {
    return int.tryParse(custom[name] ?? "") ?? defaultValue;
  }

  String initQueryText() {
    if (initQuerys.isEmpty) {
      return "";
    }
    return initQuerys.map((e) {
      e = e.trimRight();
      if (e.endsWith(";")) {
        return e;
      } else {
        return "$e;";
      }
    }).join("\n");
  }
}
