// The signal processing: see dsp.h.
#include "worker/dsp.h"

#include <algorithm>
#include <cmath>

namespace audio {

namespace {

constexpr float PI = 3.14159265358979f;
constexpr float LOWEST_HZ = 60.0f;
constexpr float SPAN = 200.0f;  // 60 Hz times 200 is 12 kHz: the bands cover that, logarithmically

}  // namespace

Frame decode(const std::string& pcm) {
  Frame frame{};
  const std::size_t samples = std::min<std::size_t>(FRAME, pcm.size() / 2);
  for (std::size_t i = 0; i < samples; ++i) {
    const int low = static_cast<unsigned char>(pcm[2 * i]);
    const int high = static_cast<unsigned char>(pcm[2 * i + 1]);
    frame[i] = static_cast<std::int16_t>(low | (high << 8)) / 32768.0f;
  }
  return frame;
}

std::string encode(const Frame& frame) {
  std::string pcm(FRAME * 2, '\0');
  for (int i = 0; i < FRAME; ++i) {
    // decode()'s scale, so that a frame nothing changed comes back as it came.
    const long scaled = std::lrint(frame[i] * 32768.0f);
    const std::int16_t sample = static_cast<std::int16_t>(std::max(-32768L, std::min(32767L, scaled)));
    pcm[2 * i] = static_cast<char>(sample & 0xff);
    pcm[2 * i + 1] = static_cast<char>((sample >> 8) & 0xff);
  }
  return pcm;
}

void fft(std::vector<std::complex<float>>& values) {
  const std::size_t n = values.size();
  // The bit-reversal permutation: values[i] goes to the index with i's bits reversed.
  for (std::size_t i = 1, j = 0; i < n; ++i) {
    std::size_t bit = n >> 1;
    for (; j & bit; bit >>= 1) {
      j ^= bit;
    }
    j ^= bit;
    if (i < j) {
      std::swap(values[i], values[j]);
    }
  }
  // The butterflies, over transforms of length 2, 4, ..., n.
  for (std::size_t length = 2; length <= n; length <<= 1) {
    const float angle = -2 * PI / static_cast<float>(length);
    const std::complex<float> root(std::cos(angle), std::sin(angle));
    for (std::size_t start = 0; start < n; start += length) {
      std::complex<float> twiddle(1, 0);
      for (std::size_t k = 0; k < length / 2; ++k) {
        const std::complex<float> even = values[start + k];
        const std::complex<float> odd = values[start + k + length / 2] * twiddle;
        values[start + k] = even + odd;
        values[start + k + length / 2] = even - odd;
        twiddle *= root;
      }
    }
  }
}

int band_of(float hz) {
  const float band = BANDS * std::log(hz / LOWEST_HZ) / std::log(SPAN);
  return std::max(0, std::min(BANDS - 1, static_cast<int>(band)));
}

Spectrum spectrum(const Frame& frame) {
  std::vector<std::complex<float>> values(FFT_SIZE);
  for (int i = 0; i < FRAME; ++i) {
    const float window = 0.5f - 0.5f * std::cos(2 * PI * i / (FRAME - 1));  // Hann
    values[i] = frame[i] * window;
  }
  fft(values);
  const float full_scale = FRAME / 4.0f;  // a full-scale sine under a Hann window peaks here
  const float bin_hz = static_cast<float>(SAMPLE_RATE) / FFT_SIZE;
  // Each bin counts in the band its middle frequency falls in; bin 0, the
  // constant part, is no tone.
  std::array<float, BANDS> peaks{};
  std::array<bool, BANDS> covered{};
  for (int bin = 1; bin <= FFT_SIZE / 2 && bin * bin_hz < LOWEST_HZ * SPAN; ++bin) {
    if (bin * bin_hz < LOWEST_HZ) {
      continue;
    }
    const int band = band_of(bin * bin_hz);
    peaks[band] = std::max(peaks[band], std::abs(values[bin]));
    covered[band] = true;
  }
  Spectrum bands{};
  for (int band = 0; band < BANDS; ++band) {
    float peak = peaks[band];
    if (!covered[band]) {  // narrower than a bin, at the low end: the bin nearest its middle
      const float middle = LOWEST_HZ * std::pow(SPAN, (band + 0.5f) / BANDS);
      peak = std::abs(values[std::max(1, static_cast<int>(std::lround(middle / bin_hz)))]);
    }
    const float db = 20 * std::log10(std::max(peak / full_scale, 1e-6f));
    bands[band] = static_cast<std::uint8_t>(std::max(0.0f, std::min(255.0f, (db + 60) / 60 * 255)));
  }
  return bands;
}

void Biquad::high_pass(float hz) {
  const float w0 = 2 * PI * hz / SAMPLE_RATE, alpha = std::sin(w0) / (2 * 0.7071f), c = std::cos(w0);
  const float a0 = 1 + alpha;
  b0_ = (1 + c) / 2 / a0;
  b1_ = -(1 + c) / a0;
  b2_ = (1 + c) / 2 / a0;
  a1_ = -2 * c / a0;
  a2_ = (1 - alpha) / a0;
  x1_ = x2_ = y1_ = y2_ = 0;
}

void Biquad::low_pass(float hz) {
  const float w0 = 2 * PI * hz / SAMPLE_RATE, alpha = std::sin(w0) / (2 * 0.7071f), c = std::cos(w0);
  const float a0 = 1 + alpha;
  b0_ = (1 - c) / 2 / a0;
  b1_ = (1 - c) / a0;
  b2_ = (1 - c) / 2 / a0;
  a1_ = -2 * c / a0;
  a2_ = (1 - alpha) / a0;
  x1_ = x2_ = y1_ = y2_ = 0;
}

float Biquad::apply(float x) {
  const float y = b0_ * x + b1_ * x1_ + b2_ * x2_ - a1_ * y1_ - a2_ * y2_;
  x2_ = x1_;
  x1_ = x;
  y2_ = y1_;
  y1_ = y;
  return y;
}

Stream::Stream(const std::string& effect) { set_effect(effect); }

void Stream::set_effect(const std::string& effect) {
  effect_ = effect;
  high_ = Biquad();
  low_ = Biquad();
  high_.high_pass(300);
  low_.low_pass(3400);
  phase_ = 0;
  delay_.assign(SAMPLE_RATE / 4, 0.0f);
  position_ = 0;
}

void Stream::apply(Frame& frame) {
  if (effect_ == "telephone") {
    for (float& sample : frame) {
      sample = low_.apply(high_.apply(sample)) * 1.5f;
    }
  } else if (effect_ == "robot") {
    const double step = 2 * PI * 50 / SAMPLE_RATE;  // ring modulation with a 50 Hz sine
    for (float& sample : frame) {
      sample *= static_cast<float>(std::sin(phase_));
      phase_ += step;
      if (phase_ > 2 * PI) {
        phase_ -= 2 * PI;
      }
    }
  } else if (effect_ == "echo") {
    for (float& sample : frame) {
      const float delayed = delay_[position_];
      const float out = sample + 0.5f * delayed;
      delay_[position_] = sample + 0.45f * delayed;  // and the echo echoes, fading
      position_ = (position_ + 1) % delay_.size();
      sample = out;
    }
  }
  // "none", and any name the page does not know: the frame as it is.
}

}  // namespace audio
