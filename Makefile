pub_get:
	cd client && flutter pub get
	cd pkg/db_driver/impl/mysql && flutter pub get
	cd pkg/db_driver/impl/pg && flutter pub get
	cd pkg/db_driver && flutter pub get
	cd pkg/sql_parser && flutter pub get
	cd pkg/sql-editor && flutter pub get

dart_gen_code:
	cd client && dart run build_runner build --delete-conflicting-outputs
	cd client && flutter pub run flutter_launcher_icons
	cd client && flutter gen-l10n --verbose

	cd pkg/db_driver/impl/mysql && flutter_rust_bridge_codegen generate

APP_NAME := openhare
VERSION := $(shell grep '^version:' client/pubspec.yaml | sed 's/version: *//')
DMG_NAME := $(APP_NAME)-$(VERSION)-macos-arm64.dmg

build_macos:
	cd client && flutter build macos --release
	rm -f $(DMG_NAME)
	/opt/homebrew/bin/create-dmg \
		--volname "$(APP_NAME)" \
		--window-size 600 400 \
		--icon-size 100 \
		--app-drop-link 450 150 \
		$(DMG_NAME) \
		client/build/macos/Build/Products/Release/$(APP_NAME).app