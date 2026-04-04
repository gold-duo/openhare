Pod::Spec.new do |s|
  s.name             = 'go_impl'
  s.version          = '0.0.1'
  s.summary          = 'Shared Go FFI layer (Oracle via go-ora, future drivers).'
  s.description      = <<-DESC
Go-based shared native library for Dart FFI: Oracle and future drivers in one dylib.
                       DESC
  s.homepage         = 'http://example.com'
  s.license          = { :file => '../LICENSE' }
  s.author           = { 'Your Company' => 'email@example.com' }

  s.source           = { :path => '.' }
  s.source_files     = 'Classes/dummy.c'
  s.dependency 'FlutterMacOS'
  s.platform         = :osx, '10.11'

  s.script_phase = {
    :name => 'Build Go static library (go_impl)',
    :script => <<-SCRIPT,
set -e
cd "$PODS_TARGET_SRCROOT/../src"

# Use DERIVED_FILE_DIR so the Run Script output path matches OTHER_LDFLAGS at link time.
# BUILT_PRODUCTS_DIR for the pod target can differ from where -force_load resolves, which
# caused undefined symbols for go_impl_* despite dummy.c referencing them.
OUT_A="${DERIVED_FILE_DIR}/libgo_impl.a"

build_arch() {
  case "$1" in
    arm64)  GOARCH="arm64" ;;
    x86_64) GOARCH="amd64" ;;
    *) echo "Unsupported architecture: $1"; exit 1 ;;
  esac
  CGO_ENABLED=1 GOOS=darwin GOARCH=$GOARCH go build -buildmode=c-archive -o "${OUT_A}.$1" .
}

LIPO_INPUTS=""
for arch in $ARCHS; do
  build_arch "$arch"
  LIPO_INPUTS="$LIPO_INPUTS ${OUT_A}.${arch}"
done

if echo "$ARCHS" | grep -q " "; then
  lipo -create -output "$OUT_A" $LIPO_INPUTS
else
  mv "${OUT_A}.${ARCHS}" "$OUT_A"
fi
rm -f ${OUT_A}.*
SCRIPT
    :execution_position => :before_compile,
    :input_files => [
      '${PODS_TARGET_SRCROOT}/../src/impl.go',
      '${PODS_TARGET_SRCROOT}/../src/oracle.go',
      '${PODS_TARGET_SRCROOT}/../src/stream_bridge.c',
    ],
    :output_files => ['${DERIVED_FILE_DIR}/libgo_impl.a'],
  }

  # -force_load entire archive: ld64 link order can skip .a members; Dart-only symbols need dummy.c too.
  # Must match OUT_A in the Run Script above (DERIVED_FILE_DIR).
  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'OTHER_LDFLAGS' => '$(inherited) -force_load $(DERIVED_FILE_DIR)/libgo_impl.a',
  }
  s.swift_version = '5.0'
end
