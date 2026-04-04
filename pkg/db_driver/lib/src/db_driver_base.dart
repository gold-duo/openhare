import 'db_driver_interface.dart';
import 'db_driver_conn_meta.dart';
import 'db_driver_mysql.dart';
import 'package:rust_impl/rust_impl.dart' as rust_impl;
import 'db_driver_oracle.dart';
import 'db_driver_mssql.dart';
import 'db_driver_sqlite.dart';
import 'db_driver_pg.dart';

class ConnectionFactory {
  static Future<void> init() async {
    await rust_impl.RustLib.init();
  }

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
      TargetNetworkHostMeta(),
      TargetNetworkPortMeta("3306"),
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
        TargetNetworkHostMeta(),
        TargetNetworkPortMeta("5432"),
        UserMeta(),
        PasswordMeta(),
        DescMeta(),
        CustomMeta(
            name: "database",
            type: "text",
            group: "connection",
            isRequired: true,
            defaultValue: "postgres"),
        CustomMeta(
          name: "connectTimeout",
          type: "text",
          group: "connection",
          defaultValue: "10",
        ),
        CustomMeta(
          name: "queryTimeout",
          type: "text",
          group: "connection",
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
      TargetNetworkHostMeta(),
      TargetNetworkPortMeta("1521"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
          name: "service",
          type: "text",
          group: "connection",
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
      TargetNetworkHostMeta(),
      TargetNetworkPortMeta("1433"),
      UserMeta(),
      PasswordMeta(),
      DescMeta(),
      CustomMeta(
          name: "database",
          type: "text",
          group: "connection",
          isRequired: true,
          defaultValue: "master"),
      CustomMeta(
        name: "encrypt",
        type: "text",
        group: "connection",
        defaultValue: "true",
      ),
      CustomMeta(
        name: "trustServerCertificate",
        type: "text",
        group: "connection",
        defaultValue: "true",
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
];

List<DatabaseType> allDatabaseType =
    connectionMetas.map((meta) => meta.type).toList();

Map<DatabaseType, ConnectionMeta> connectionMetaMap = {
  for (var meta in connectionMetas) meta.type: meta
};
