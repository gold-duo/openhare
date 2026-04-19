import 'db_driver_interface.dart';
import 'db_driver_conn_meta.dart';
import 'db_driver_mysql.dart';
import 'db_driver_oracle.dart';
import 'db_driver_mssql.dart';
import 'db_driver_sqlite.dart';
import 'db_driver_pg.dart';
import 'db_driver_redis.dart';
import 'db_driver_mongodb.dart';

class ConnectionFactory {
  static Future<BaseConnection> open(
      {required DatabaseType type,
      required ConnectValue meta,
      String? schema,
      Function(String)? onSchemaChangedCallback}) async {
    BaseConnection? conn;
    try {
      conn = switch (type) {
        DatabaseType.mysql =>
          await MySQLConnection.open(meta: meta, schema: schema),
        DatabaseType.pg => await PGConnection.open(meta: meta, schema: schema),
        DatabaseType.oracle =>
          await OracleConnection.open(meta: meta, schema: schema),
        DatabaseType.mssql =>
          await MSSQLConnection.open(meta: meta, schema: schema),
        DatabaseType.sqlite =>
          await SQLiteConnection.open(meta: meta, schema: schema),
        DatabaseType.redis =>
          await RedisConnection.open(meta: meta, schema: schema),
        DatabaseType.mongodb =>
          await MongoConnection.open(meta: meta, schema: schema),
      };
      conn.listen(onSchemaChangedCallback: onSchemaChangedCallback);

      for (var sql in meta.initQuerys) {
        await conn.query(sql);
      }
    } catch (e) {
      conn?.close();
      rethrow;
    }
    return conn;
  }
}

List<ConnectionMeta> connectionMetas = [
  ConnectionMeta(
    displayName: "MySQL",
    type: DatabaseType.mysql,
    logoAssertPath: "assets/icons/mysql_icon.png",
    connMeta: [
      NameMeta(),
      TargetNetworkMeta(defaultPort: "3306"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
    ],
    initQuerys: [
      "SET NAMES utf8mb4;",
      "SET CHARACTER SET utf8mb4;",
      "SET character_set_connection=utf8mb4;",
      "SET sql_mode = 'STRICT_ALL_TABLES';",
    ],
  ),
  ConnectionMeta(
      displayName: "PostgreSQL",
      type: DatabaseType.pg,
      logoAssertPath: "assets/icons/pg_icon.png",
      connMeta: [
        NameMeta(),
        TargetNetworkMeta(defaultPort: "5432"),
        UserMeta(),
        PasswordMeta(),
        DescMeta(),
        CustomMeta(
            name: "database",
            type: SettingMetaType.text,
            group: settingMetaGroupBase,
            isRequired: true,
            defaultValue: "postgres"),
        CustomMeta(
          name: "connectTimeout",
          type: SettingMetaType.text,
          group: settingMetaGroupVariables,
          defaultValue: "10",
        ),
        CustomMeta(
          name: "queryTimeout",
          type: SettingMetaType.text,
          group: settingMetaGroupVariables,
          defaultValue: "600",
        ),
      ],
      // postgresql init sql
      initQuerys: [
        "SET client_encoding = 'UTF8';",
      ]),
  ConnectionMeta(
    displayName: "Oracle",
    type: DatabaseType.oracle,
    logoAssertPath: "assets/icons/oracle_icon.png",
    connMeta: [
      NameMeta(),
      TargetNetworkMeta(defaultPort: "1521"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
          name: "service",
          type: SettingMetaType.text,
          group: settingMetaGroupBase,
          isRequired: true,
          defaultValue: "FREEPDB1"),
    ],
    initQuerys: const [],
  ),
  ConnectionMeta(
    displayName: "SQL Server",
    type: DatabaseType.mssql,
    logoAssertPath: "assets/icons/mssql_icon.png",
    connMeta: [
      NameMeta(),
      TargetNetworkMeta(defaultPort: "1433"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
          name: "database",
          type: SettingMetaType.text,
          group: settingMetaGroupBase,
          isRequired: true,
          defaultValue: "master"),
      CustomMeta(
        name: "encrypt",
        type: SettingMetaType.enumValue,
        group: settingMetaGroupVariables,
        defaultValue: "true",
        enumValues: ['true', 'false'],
      ),
      CustomMeta(
        name: "trustServerCertificate",
        type: SettingMetaType.enumValue,
        group: settingMetaGroupVariables,
        defaultValue: "true",
        enumValues: ['true', 'false'],
      ),
    ],
    initQuerys: const [],
  ),
  ConnectionMeta(
    displayName: "SQLite",
    type: DatabaseType.sqlite,
    logoAssertPath: "assets/icons/sqlite_icon.png",
    connMeta: [
      NameMeta(),
      TargetDBFileMeta(),
      DescMeta(),
    ],
    initQuerys: const [
      "PRAGMA temp_store = MEMORY;",
      "PRAGMA journal_mode = MEMORY;",
    ],
  ),
  ConnectionMeta(
    displayName: "Redis",
    type: DatabaseType.redis,
    logoAssertPath: "assets/icons/redis_icon.png",
    connMeta: [
      NameMeta(),
      TargetNetworkMeta(defaultPort: "6379"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
        name: "db",
        type: SettingMetaType.text,
        group: settingMetaGroupBase,
        defaultValue: "0",
      ),
      CustomMeta(
        name: "tls",
        type: SettingMetaType.enumValue,
        group: settingMetaGroupVariables,
        defaultValue: "false",
        enumValues: ['true', 'false'],
      ),
    ],
    initQuerys: const [],
  ),
  ConnectionMeta(
    displayName: 'MongoDB',
    description: """
The connection driver uses mongosh-compatible shell syntax and leverages the gomongo library. Summary of unsupported features:
1. No interactive features (cursor methods, native shell functions), no JavaScript execution, and no database switching. 
2. It also excludes cluster/administration features (replication, sharding, user/role management, encryption) and Atlas-specific capabilities.
3. Database is set at connection time only, no database switching.
""",
    type: DatabaseType.mongodb,
    logoAssertPath: 'assets/icons/mongodb_icon.png',
    connMeta: [
      NameMeta(),
      TargetNetworkMeta(defaultPort: '27017'),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
        name: 'database',
        type: SettingMetaType.text,
        group: settingMetaGroupBase,
        isRequired: true,
        defaultValue: 'test',
        comment: '连接 URI 路径中的默认数据库名，并作为 shell 执行的默认库',
      ),
      CustomMeta(
        name: 'authSource',
        type: SettingMetaType.text,
        group: settingMetaGroupVariables,
        defaultValue: 'admin',
        comment: 'SCRAM 等认证时查找用户凭证所在的数据库（URI 参数 authSource）',
      ),
      CustomMeta(
        name: 'tls',
        type: SettingMetaType.enumValue,
        group: settingMetaGroupVariables,
        enumValues: ['true', 'false'],
        defaultValue: 'false',
        comment: '是否启用 TLS（true/false，对应 URI 中 tls 选项）',
      ),
      CustomMeta(
        name: 'directConnection',
        type: SettingMetaType.enumValue,
        group: settingMetaGroupVariables,
        defaultValue: 'true',
        enumValues: ['true', 'false'],
        comment: '为 true 时只连当前主机端口，不解析副本集拓扑（URI 参数 directConnection）',
      ),
    ],
    initQuerys: const [],
  ),
];

List<DatabaseType> allDatabaseType =
    connectionMetas.map((meta) => meta.type).toList();

Map<DatabaseType, ConnectionMeta> connectionMetaMap = {
  for (var meta in connectionMetas) meta.type: meta
};
