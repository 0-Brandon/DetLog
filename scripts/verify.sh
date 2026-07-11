#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_dir="$root/out/manual-debug"
cxx=${CXX:-c++}

mkdir -p "$build_dir"

for test in "$root"/tests/*_tests.cpp; do
  name=$(basename "$test" .cpp)
  executable="$build_dir/$name"
  "$cxx" -std=c++20 -O0 -g \
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow \
    -I"$root/include" "$root"/src/*.cpp "$test" -pthread -o "$executable"
  "$executable"
done

scenario_fuzz="$root/fuzz/scenario_fuzz.cpp"
if [ -f "$scenario_fuzz" ]; then
  executable="$build_dir/scenario_fuzz_smoke"
  "$cxx" -std=c++20 -O0 -g \
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow \
    -DDETLOG_SCENARIO_FUZZ_STANDALONE=1 \
    -I"$root/include" "$root"/src/*.cpp "$scenario_fuzz" \
    -pthread -o "$executable"
  "$executable"
fi

echo "All standalone tests passed."
