#include "detlog/wal.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                       std::size_t size) {
  const auto* bytes = reinterpret_cast<const std::byte*>(data);
  const std::span<const std::byte> input(bytes, size);
  detlog::WalOptions limits;
  limits.max_frame_bytes = 1024U * 1024U;
  limits.max_group_bytes = 4U * 1024U * 1024U;
  limits.max_entries_per_frame = 4096;
  limits.max_key_bytes = 16U * 1024U;
  limits.max_value_bytes = 256U * 1024U;
  limits.max_log_entries = 100'000;

  try {
    (void)detlog::scan_wal_bytes(
        input, detlog::WalIdentity{0x1122334455667788ULL,
                                  0x99aabbccddeeff00ULL, 1},
        limits);
  } catch (const detlog::WalError&) {
    // Rejection is the expected result for arbitrary input. Other exception
    // types and sanitizer failures remain visible to the fuzzing engine.
  }
  return 0;
}

