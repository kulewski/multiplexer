// A message protobuf will not serialize, over 2 GiB, raises
// std::length_error where its frame is built, as one over MAX_MESSAGE_SIZE
// does: protobuf left the serialized string empty, and the frame went out
// with no body, which the multiplexer answers by closing the connection.
// RawMessage::FromMessage frames every C++ send and query; the message is
// built once, a payload of 2 GiB and a byte, and protobuf refuses it before
// it allocates anything more.
#include <gtest/gtest.h>

#include <climits>
#include <cstddef>
#include <memory>
#include <stdexcept>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/io/raw_message.h"

namespace {

TEST(RawMessage, AMessageOverTwoGibibytesIsRefusedWhereItIsFramed) {
  multiplexer::MultiplexerMessage mxmsg;
  mxmsg.set_id(1);
  mxmsg.set_type(1000);
  mxmsg.mutable_message()->assign(static_cast<std::size_t>(INT_MAX) + 1, 'x');
  std::unique_ptr<multiplexer::RawMessage> frame;
  EXPECT_THROW(frame.reset(multiplexer::RawMessage::FromMessage(mxmsg)), std::length_error);
  EXPECT_EQ(nullptr, frame.get()) << "a frame of " << frame->get_message().size() << " bytes was built";
}

}  // namespace
