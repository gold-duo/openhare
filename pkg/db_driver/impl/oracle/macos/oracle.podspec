#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint oracle.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'oracle'
  s.version          = '0.0.1'
  s.summary          = 'A new Flutter FFI plugin project.'
  s.description      = <<-DESC
A new Flutter FFI plugin project.
                       DESC
  s.homepage         = 'http://example.com'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Company' => 'email@example.com' }

  # This will ensure the source files in Classes/ are included in the native
  # builds of apps using this FFI plugin. Podspec does not support relative
  # paths, so Classes contains a forwarder C file that relatively imports
  # `../src/*` so that the C sources can be shared among all target platforms.
  s.source           = { :path => '.' }
  s.source_files = 'Classes/**/*'

  # If your plugin requires a privacy manifest, for example if it collects user
  # data, update the PrivacyInfo.xcprivacy file to describe your plugin's
  # privacy impact, and then uncomment this line. For more information,
  # see https://developer.apple.com/documentation/bundleresources/privacy_manifest_files
  # s.resource_bundles = {'oracle_privacy' => ['Resources/PrivacyInfo.xcprivacy']}

  s.dependency 'FlutterMacOS'

  s.platform = :osx, '10.11'
  s.script_phase = {
    :name => 'Build Go library',
    :script => <<-SCRIPT,
set -e
cd "$PODS_TARGET_SRCROOT/../go"
OUT="${BUILT_PRODUCTS_DIR}/liboracle_go.a"

# 构建通用二进制：为 ARCHS 中的每个架构分别构建，然后用 lipo 合并
build_arch() {
  case "$1" in
    arm64)  GOARCH="arm64" ;;
    x86_64) GOARCH="amd64" ;;
    *) echo "Unsupported architecture: $1"; exit 1 ;;
  esac
  CGO_ENABLED=1 GOOS=darwin GOARCH=$GOARCH go build -buildmode=c-archive -o "$2" .
}

LIPO_INPUTS=""
for arch in $ARCHS; do
  build_arch "$arch" "${OUT}.${arch}"
  LIPO_INPUTS="$LIPO_INPUTS ${OUT}.${arch}"
done
if echo "$ARCHS" | grep -q " "; then
  lipo -create -output "$OUT" $LIPO_INPUTS
  rm -f ${OUT}.*
else
  mv "${OUT}.${ARCHS}" "$OUT"
fi
SCRIPT
    :execution_position => :before_compile,
    :input_files => ['${BUILT_PRODUCTS_DIR}/oracle_go_phony'],
    :output_files => ["${BUILT_PRODUCTS_DIR}/liboracle_go.a"],
  }

  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'OTHER_LDFLAGS' => '-force_load ${BUILT_PRODUCTS_DIR}/liboracle_go.a',
  }
  s.swift_version = '5.0'
end
#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html.
# Run `pod lib lint oracle.podspec` to validate before publishing.
#
Pod::Spec.new do |s|
  s.name             = 'oracle'
  s.version          = '0.0.1'
  s.summary          = 'Oracle driver via Go + go-ora.'
  s.description      = <<-DESC
Oracle driver via Go + go-ora.
                       DESC
  s.homepage         = 'http://example.com'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Company' => 'email@example.com' }

  # This will ensure the source files in Classes/ are included in the native
  # builds of apps using this FFI plugin. Podspec does not support relative
  # paths, so Classes contains a forwarder C file that relatively imports
  # `../src/*` so that the C sources can be shared among all target platforms.
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'

  s.dependency 'FlutterMacOS'

  s.platform = :osx, '10.11'
  s.script_phase = {
    :name => 'Build Go library',
    :script => <<-SCRIPT,
set -e
cd "$PODS_TARGET_SRCROOT/../go"
OUT="${BUILT_PRODUCTS_DIR}/liboracle_go.a"

# 构建通用二进制：为 ARCHS 中的每个架构分别构建，然后用 lipo 合并
build_arch() {
  case "$1" in
    arm64)  GOARCH="arm64" ;;
    x86_64) GOARCH="amd64" ;;
    *) echo "Unsupported architecture: $1"; exit 1 ;;
  esac
  CGO_ENABLED=1 GOOS=darwin GOARCH=$GOARCH go build -buildmode=c-archive -o "$2" .
}

LIPO_INPUTS=""
for arch in $ARCHS; do
  build_arch "$arch" "${OUT}.${arch}"
  LIPO_INPUTS="$LIPO_INPUTS ${OUT}.${arch}"
done
if echo "$ARCHS" | grep -q " "; then
  lipo -create -output "$OUT" $LIPO_INPUTS
  rm -f ${OUT}.*
else
  mv "${OUT}.${ARCHS}" "$OUT"
fi
SCRIPT
    :execution_position => :before_compile,
    :input_files => ['${BUILT_PRODUCTS_DIR}/oracle_go_phony'],
    :output_files => ["${BUILT_PRODUCTS_DIR}/liboracle_go.a"],
  }

  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'OTHER_LDFLAGS' => '-force_load ${BUILT_PRODUCTS_DIR}/liboracle_go.a',
  }
  s.swift_version = '5.0'
end

