#!/bin/bash
# Generate gRPC Python code from protobuf definitions (multi-file, cross-platform)

set -e

echo "🚀 Generating gRPC Python code..."

# Detect Python executable
PYTHON_BIN=$(which python3 || which python)
echo "Using Python: $PYTHON_BIN"

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "❌ Error: protoc (Protocol Buffer Compiler) is not installed"
    echo "👉 Install protoc: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

# Check if grpcio-tools is installed
if ! $PYTHON_BIN -c "import grpc_tools" &> /dev/null; then
    echo "❌ Error: grpcio-tools is not installed"
    echo "👉 Install it: pip install grpcio-tools mypy-protobuf"
    exit 1
fi

# Directories
PROTO_DIR="src/proto"
OUTPUT_DIR="src/proto"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Generate Python code for all proto files
echo "📦 Compiling proto files..."
for proto_file in "$PROTO_DIR"/*.proto; do
    echo "  → Processing $proto_file"
    $PYTHON_BIN -m grpc_tools.protoc \
        --proto_path="$PROTO_DIR" \
        --python_out="$OUTPUT_DIR" \
        --grpc_python_out="$OUTPUT_DIR" \
        --mypy_out="$OUTPUT_DIR" \
        "$proto_file"
done

# Fix import paths in generated gRPC files
echo "🔧 Fixing import paths..."
for grpc_file in "$OUTPUT_DIR"/*_pb2_grpc.py; do
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' 's/^import \(.*_pb2\) as/from . import \1 as/' "$grpc_file"
    else
        sed -i 's/^import \(.*_pb2\) as/from . import \1 as/' "$grpc_file"
    fi
done

# Ensure __init__.py exists
touch "$OUTPUT_DIR/__init__.py"

# Verify generated files
echo "✅ Verifying generated files..."
for proto_file in "$PROTO_DIR"/*.proto; do
    base_name=$(basename "$proto_file" .proto)
    if [[ -f "$OUTPUT_DIR/${base_name}_pb2.py" && -f "$OUTPUT_DIR/${base_name}_pb2_grpc.py" ]]; then
        echo "  - $base_name OK"
    else
        echo "❌ Missing generated files for $base_name"
        exit 1
    fi
done

# Test import to confirm correctness
echo "🧪 Testing imports..."
if $PYTHON_BIN -c "from src.proto import *" 2>/dev/null; then
    echo "✅ All generated files can be imported successfully"
else
    echo "⚠️ Warning: Import test failed"
    echo "   Likely due to PYTHONPATH issues. Run:"
    echo "   export PYTHONPATH=\$PYTHONPATH:$(pwd)/src"
fi

echo "🎉 gRPC code generation completed successfully!"
# End of script