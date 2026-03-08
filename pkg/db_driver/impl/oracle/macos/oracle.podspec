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

  s.source           = { :path => '.' }
  s.source_files     = 'Classes/dummy.c'
  s.dependency 'FlutterMacOS'
  s.platform         = :osx, '10.11'

  s.script_phase = {
    :name => 'Build Go static library',
    :script => <<-SCRIPT,
set -e
cd "$PODS_TARGET_SRCROOT/../go"

OUT_A="${BUILT_PRODUCTS_DIR}/liboracle.a"

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
    :input_files => ['${BUILT_PRODUCTS_DIR}/oracle_go_phony'],
    :output_files => ['${BUILT_PRODUCTS_DIR}/liboracle.a'],
  }

  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
  }
  s.swift_version = '5.0'
end

