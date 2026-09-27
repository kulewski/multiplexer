// The audio worker: a C++ backend on BaseThreadedMultiplexerServer that
// applies a participant's effect to every AUDIO_FRAME and answers with
// the frame and its spectrum, in tens of microseconds. State per stream,
// the filters' memory and the echo's quarter second, lives here, keyed by
// participant, which is why the gateway keeps a participant's frames on
// one worker.
//
// Usage: worker [host:port,host:port] [name]     (default 127.0.0.1:1980, host:pid)
//
// The page shows the name's first sixteen bytes; the default is the host
// name's last eight characters, where the pods of one deployment differ,
// and the pid, which tells two workers of one host apart.
//
// SIGTERM asks the worker to leave: the handler only sets a flag, which
// periodic_task() reads between polls; the worker then tells the
// multiplexers to route it nothing new, serves what was already on its
// way and what is still addressed to it, and exits once they have
// confirmed, three seconds at the latest. No new stream comes to it; a
// stream addressed to it moves when it closes, its next frame refused at
// once, and starts afresh on the next worker. A frame on its way at the
// moment it closes is lost to the gateway's timeout.
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "audio.pb.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/multiplexer.constants.h"  // generated from audio.rules
#include "worker/dsp.h"

using multiplexer::backend::BaseThreadedMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using multiplexer::backend::RequestPtr;

static volatile std::sig_atomic_t leave_requested = 0;

void on_signal(int) { leave_requested = 1; }

class DspWorker : public BaseThreadedMultiplexerServer {
 public:
  DspWorker(const MultiplexerAddresses& addresses, const std::string& name)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::DSP), name_(name) {}

  // One frame: its stream's effect applied, the spectrum computed, both answered.
  void handle_message(const RequestPtr& request) override {
    if (request->mxmsg().type() != multiplexer::types::AUDIO_FRAME) {
      request->no_response();
      return;
    }
    const auto started = std::chrono::steady_clock::now();
    const audio::AudioFrame frame = request->parse_message<audio::AudioFrame>();
    audio::Frame samples = audio::decode(frame.pcm());
    {
      std::lock_guard<std::mutex> lock(mutex_);
      Held& held = streams_[frame.participant()];
      // A new effect, or a stream back after a tenth of a second away, on
      // another worker or silent: the state held is from another time.
      if (held.stream.effect() != frame.effect() || started - held.last > std::chrono::milliseconds(100)) {
        held.stream.set_effect(frame.effect());
      }
      held.stream.apply(samples);
      held.last = started;
    }
    const audio::Spectrum bands = audio::spectrum(samples);
    audio::AudioProcessed processed;
    processed.set_participant(frame.participant());
    processed.set_seq(frame.seq());
    processed.set_pcm(audio::encode(samples));
    processed.set_spectrum(std::string(bands.begin(), bands.end()));
    processed.set_worker(name_);
    processed.set_captured_at(frame.captured_at());
    processed.set_micros(static_cast<std::uint32_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count()));
    request->reply(processed.SerializeAsString(), multiplexer::types::AUDIO_PROCESSED);
  }

  // Every poll: leave when asked, and forget streams that went quiet.
  void periodic_task() override {
    if (leave_requested && !draining()) {
      start_draining();
      std::cout << "leaving" << std::endl;  // for whoever watches the drain begin, the test included
    }
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = streams_.begin(); it != streams_.end();) {
      if (now - it->second.last > std::chrono::seconds(5)) {
        it = streams_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  struct Held {
    audio::Stream stream;
    std::chrono::steady_clock::time_point last;
  };
  const std::string name_;
  std::mutex mutex_;  // handle_message() runs on a worker thread, periodic_task() on the serving thread
  std::map<std::uint32_t, Held> streams_;
};

// "host:port,host:port" as the addresses the library takes.
MultiplexerAddresses parse_addresses(const std::string& text) {
  MultiplexerAddresses addresses;
  std::size_t start = 0;
  while (start <= text.size()) {
    std::size_t comma = text.find(',', start);
    if (comma == std::string::npos) {
      comma = text.size();
    }
    const std::string item = text.substr(start, comma - start);
    const std::size_t colon = item.rfind(':');
    addresses.push_back({item.substr(0, colon), static_cast<unsigned short>(std::stoi(item.substr(colon + 1)))});
    start = comma + 1;
  }
  return addresses;
}

int main(int argc, char** argv) {
  const std::string addresses = argc > 1 ? argv[1] : "127.0.0.1:1980";
  char hostname[256] = "worker";
  gethostname(hostname, sizeof(hostname));
  const std::string host(hostname);
  const std::string name =
      argc > 2 ? argv[2] : host.substr(host.size() > 8 ? host.size() - 8 : 0) + ":" + std::to_string(getpid());
  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
  DspWorker worker(parse_addresses(addresses), name);
  // The line carries the instance id for the test, which waits for the
  // worker on the multiplexers; serve_forever() connects.
  std::cout << "ready: worker " << name << ", instance " << worker.instance_id() << std::endl;
  worker.serve_forever(0.1f, 3.0f);
  std::cout << "left" << std::endl;
  return 0;
}
