// The signal processing of the audio worker: 10 ms frames of 48 kHz PCM,
// a radix-2 FFT of its own, thirty-two spectrum bands for drawing, and
// four effects that keep state per stream. Nothing beyond the standard
// library, so that the worker is the library and this file.
#ifndef AUDIO_WORKER_DSP_H_
#define AUDIO_WORKER_DSP_H_

#include <array>
#include <complex>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace audio {

constexpr int SAMPLE_RATE = 48000;
constexpr int FRAME = 480;     // samples per frame: 10 ms
constexpr int FFT_SIZE = 512;  // the frame, zero-padded to a power of two
constexpr int BANDS = 32;

typedef std::array<float, FRAME> Frame;            // samples in [-1, 1]
typedef std::array<std::uint8_t, BANDS> Spectrum;  // 0 to 255 per band

// 16-bit little-endian PCM to floats, and back with clipping; a frame
// nothing changed comes back as it came.
Frame decode(const std::string& pcm);
std::string encode(const Frame& frame);

// The fast Fourier transform, in place; values.size() must be a power of two.
void fft(std::vector<std::complex<float>>& values);

// The frame's magnitude spectrum for drawing: a Hann window, the FFT, and
// thirty-two bands spaced logarithmically from 60 Hz to 12 kHz, each the
// peak of the bins whose middle falls in the band, in decibels relative
// to a full-scale sine, -60 dB to 0 dB as 0 to 255. A low band that holds
// no bin's middle shows the bin nearest its own.
Spectrum spectrum(const Frame& frame);
// The band a frequency falls in.
int band_of(float hz);

// A second-order filter section, the RBJ cookbook's, with its memory.
class Biquad {
 public:
  void high_pass(float hz);
  void low_pass(float hz);
  float apply(float x);

 private:
  float b0_ = 1, b1_ = 0, b2_ = 0, a1_ = 0, a2_ = 0;
  float x1_ = 0, x2_ = 0, y1_ = 0, y2_ = 0;
};

// One participant's effect and the state it keeps between frames, which
// is why a stream has to stay on one worker: the filters' memory, the
// modulator's phase, a quarter second of the past for the echo.
class Stream {
 public:
  explicit Stream(const std::string& effect = "none");
  const std::string& effect() const { return effect_; }
  void set_effect(const std::string& effect);  // starts the state afresh
  void apply(Frame& frame);

 private:
  std::string effect_;
  Biquad high_, low_;         // telephone: 300 Hz to 3400 Hz
  double phase_ = 0;          // robot: the modulator's phase
  std::vector<float> delay_;  // echo: 250 ms of what was heard
  std::size_t position_ = 0;
};

}  // namespace audio

#endif  // AUDIO_WORKER_DSP_H_
