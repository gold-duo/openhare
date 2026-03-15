<p align="center">
  <img src="./logo_full.png" alt="logo"/>
</p>

openhare is an AI-powered, cross-platform desktop SQL client with multi-database support, built for everyday development, data analysis, and DBA management workflows.

<p align="center">
  <a href="https://github.com/sjjian/openhare/stargazers"><img alt="GitHub Repo stars" src="https://img.shields.io/github/stars/sjjian/openhare?style=flat-square"/></a>
  <a href="./LICENSE"><img alt="License" src="https://img.shields.io/github/license/sjjian/openhare?style=flat-square"/></a>
  <a href="https://github.com/sjjian/openhare/releases"><img alt="GitHub all releases" src="https://img.shields.io/github/downloads/sjjian/openhare/total?style=flat-square"/></a>
  <a href="https://github.com/sjjian/openhare/releases"><img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/sjjian/openhare?style=flat-square"/></a>
  <img alt="macOS" src="https://img.shields.io/badge/macOS-supported-000000?style=flat-square&logo=apple"/>
  <img alt="Windows" src="https://img.shields.io/badge/Windows-supported-0078D6?style=flat-square&logo=windows"/>
  <img alt="Linux" src="https://img.shields.io/badge/Linux-supported-FCC624?style=flat-square&logo=linux&logoColor=black"/>
</p>

<p align="center">
  <img src="./product.png" alt="openhare product screenshot" width="800"/>
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

## Support
1. [How to Install and Update the Application](https://github.com/sjjian/openhare/discussions/76)

## Key Features
- **AI-Powered Assistance**: Now with enhanced AI features to help you write, optimize, and understand SQL queries.
- **Cross-Platform**: Seamlessly runs on Windows, macOS, and Linux.
- **Fully Open Source**: Licensed under the [Apache License 2.0](./LICENSE), openhare is transparent and community-driven.
- **Simple & Intuitive UI**: Modern interface focused on ease of use and productivity.
- **Multi-Database Support**: Effortlessly connect to and manage various SQL databases.

## Framework
1. Application: [Flutter](https://flutter.dev/), [flutter_rust_bridge](https://github.com/fzyzcjy/flutter_rust_bridge)
2. State Management: [Riverpod](https://riverpod.dev/), [GoRouter](https://pub.dev/packages/go_router)
3. UI: [SQL Editor](https://github.com/reqable/re-editor), [HugeIcons](https://github.com/hugeicons/hugeicons-flutter), [Window Manager](https://github.com/leanflutter/window_manager)
4. Storage: [ObjectBox](https://objectbox.io/)
5. DB Driver:
   - [MySQL](https://github.com/blackbeam/mysql_async), dev on rust with flutter_rust_bridge;
   - [Postgresql](https://github.com/isoos/postgresql-dart), dart pure;
   - [SQL Server](https://github.com/prisma/tiberius), dev on rust with flutter_rust_bridge;
   - [SQLite](https://github.com/rusqlite/rusqlite), dev on rust with flutter_rust_bridge;
   - [Oracle](https://github.com/sijms/go-ora), dev on go with flutter ffi plugin.

## Star History
[![Star History Chart](https://api.star-history.com/svg?repos=sjjian/openhare&type=date&legend=top-left)](https://www.star-history.com/#sjjian/openhare&type=date&legend=top-left)