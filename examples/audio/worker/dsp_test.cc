// The signal processing against known signals: the spectrum of a tone
// peaks in the tone's band, silence is dark, PCM comes back exactly from
// a round trip, and each effect does what its name says.
#include "worker/dsp.h"

#include <gtest/gtest.h>

#include <cmath>

using audio::Frame;

namespace {

Frame sine(float hz, float amplitude, int from_sample = 0) {
  Frame frame;
  for (int i = 0; i < audio::FRAME; ++i) {
    frame[i] = amplitude * std::sin(2 * 3.14159265f * hz * (from_sample + i) / audio::SAMPLE_RATE);
  }
  return frame;
}

float energy(const Frame& frame) {
  float sum = 0;
  for (float sample : frame) {
    sum += sample * sample;
  }
  return sum / audio::FRAME;
}

int loudest_band(const audio::Spectrum& spectrum) {
  int best = 0;
  for (int band = 1; band < audio::BANDS; ++band) {
    if (spectrum[band] > spectrum[best]) {
      best = band;
    }
  }
  return best;
}

}  // namespace

TEST(Fft, MatchesTheDirectTransform) {
  std::vector<std::complex<float>> values(64), direct(64);
  for (int i = 0; i < 64; ++i) {
    values[i] = std::complex<float>(std::sin(0.37f * i) + 0.2f * i, std::cos(1.3f * i));
  }
  for (int k = 0; k < 64; ++k) {
    for (int n = 0; n < 64; ++n) {
      const float angle = -2 * 3.14159265f * k * n / 64;
      direct[k] += values[n] * std::complex<float>(std::cos(angle), std::sin(angle));
    }
  }
  audio::fft(values);
  for (int k = 0; k < 64; ++k) {
    EXPECT_NEAR(direct[k].real(), values[k].real(), 1e-2f) << "bin " << k;
    EXPECT_NEAR(direct[k].imag(), values[k].imag(), 1e-2f) << "bin " << k;
  }
}

TEST(Spectrum, ATonePeaksInItsBandAtFullScale) {
  const float on_a_bin = 11 * static_cast<float>(audio::SAMPLE_RATE) / audio::FFT_SIZE;  // 1031.25 Hz
  const audio::Spectrum bands = audio::spectrum(sine(on_a_bin, 1.0f));
  EXPECT_EQ(audio::band_of(on_a_bin), loudest_band(bands));
  EXPECT_GT(bands[audio::band_of(on_a_bin)], 235) << "about 0 dB";
  EXPECT_LT(bands[audio::band_of(5000)], 60) << "far from the tone, far down";
}

TEST(Spectrum, ATonesBandIsWhereItsNearestBinFalls) {
  EXPECT_EQ(audio::band_of(440), loudest_band(audio::spectrum(sine(440, 1.0f))));
  EXPECT_EQ(audio::band_of(2500), loudest_band(audio::spectrum(sine(2500, 1.0f))));
  // 1000 Hz is on the edge of two bands, and its nearest bin is in the upper one.
  EXPECT_EQ(audio::band_of(1031.25f), loudest_band(audio::spectrum(sine(1000, 1.0f))));
  // A low band narrower than a bin shows its nearest bin: 150 Hz lights its own band first.
  EXPECT_EQ(audio::band_of(150), loudest_band(audio::spectrum(sine(150, 1.0f))));
}

TEST(Spectrum, AQuietToneIsLower) {
  const audio::Spectrum loud = audio::spectrum(sine(2000, 1.0f));
  const audio::Spectrum quiet = audio::spectrum(sine(2000, 0.1f));  // -20 dB: a third of the way down
  EXPECT_EQ(audio::band_of(2000), loudest_band(quiet));
  EXPECT_NEAR(loud[audio::band_of(2000)] - 85, quiet[audio::band_of(2000)], 12);
}

TEST(Spectrum, SilenceIsDark) {
  for (std::uint8_t band : audio::spectrum(Frame{})) {
    EXPECT_EQ(0, band);
  }
}

TEST(Pcm, ComesBackExactly) {
  std::string pcm(960, '\0');
  for (int i = 0; i < audio::FRAME; ++i) {  // every magnitude, both extremes included
    const auto sample = static_cast<std::int16_t>(-32768 + i * 137);
    pcm[2 * i] = static_cast<char>(sample & 0xff);
    pcm[2 * i + 1] = static_cast<char>((sample >> 8) & 0xff);
  }
  pcm[958] = static_cast<char>(0xff);  // the last sample 32767
  pcm[959] = 0x7f;
  EXPECT_EQ(pcm, audio::encode(audio::decode(pcm)));
}

TEST(Effects, NoneLeavesTheFrameAlone) {
  Frame frame = sine(440, 0.5f);
  const Frame before = frame;
  audio::Stream("none").apply(frame);
  EXPECT_EQ(before, frame);
}

TEST(Effects, RobotModulatesAndKeepsItsPhaseAcrossFrames) {
  audio::Stream stream("robot");
  for (int frame = 0; frame < 3; ++frame) {
    const Frame input = sine(440, 0.5f, frame * audio::FRAME);
    Frame output = input;
    stream.apply(output);
    for (int i = 0; i < audio::FRAME; i += 7) {  // a 50 Hz sine running on across the frames
      const double t = static_cast<double>(frame * audio::FRAME + i) / audio::SAMPLE_RATE;
      EXPECT_NEAR(input[i] * std::sin(2 * M_PI * 50 * t), output[i], 1e-4) << "frame " << frame << ", sample " << i;
    }
  }
}

TEST(Effects, EchoComesBackAQuarterSecondLater) {
  audio::Stream stream("echo");
  Frame loud = sine(440, 0.5f);
  stream.apply(loud);
  Frame tenth{};
  Frame twenty_fifth{};
  for (int frame = 1; frame < 30; ++frame) {
    Frame silence{};
    stream.apply(silence);
    if (frame == 10) {
      tenth = silence;
    } else if (frame == 25) {
      twenty_fifth = silence;
    }
  }
  EXPECT_LT(energy(tenth), 1e-6f) << "nothing yet";
  EXPECT_GT(energy(twenty_fifth), 0.1f * energy(sine(440, 0.5f))) << "the echo, at half the amplitude";
}

TEST(Effects, TelephoneKeepsTheVoiceBandAndCutsTheRest) {
  float kept = 0, low_cut = 0, high_cut = 0;
  const float reference = energy(sine(1000, 0.5f));
  for (const auto& [hz, out] : {std::pair<float, float*>{1000, &kept}, {100, &low_cut}, {8000, &high_cut}}) {
    audio::Stream stream("telephone");
    Frame frame{};
    for (int step = 0; step < 5; ++step) {  // let the filters settle
      frame = sine(hz, 0.5f, step * audio::FRAME);
      stream.apply(frame);
    }
    *out = energy(frame) / reference;
  }
  EXPECT_GT(kept, 0.5f);
  EXPECT_LT(low_cut, 0.2f);
  EXPECT_LT(high_cut, 0.2f);
}
