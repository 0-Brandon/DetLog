#include "detlog/codec.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                       std::size_t size) {
  const auto* bytes = reinterpret_cast<const std::byte*>(data);
  const std::span<const std::byte> input(bytes, size);

  // Each decoder must reject malformed or oversized data without allocation
  // surprises, out-of-bounds reads, exceptions, or accepting trailing bytes.
  (void)detlog::codec::decode_message(input);
  (void)detlog::codec::decode_log_entry(input);
  (void)detlog::codec::decode_command(input);
  return 0;
}

