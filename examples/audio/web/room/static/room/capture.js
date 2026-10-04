// The capture worklet: the microphone's samples, 128 at a time as the
// browser renders them, gathered into frames of 480 (10 ms at 48 kHz) as
// 16-bit PCM, each posted to the page, which sends it.
class Capture extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buffer = new Float32Array(480);
    this.filled = 0;
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || !input[0]) return true;
    const samples = input[0];
    let offset = 0;
    while (offset < samples.length) {
      const count = Math.min(480 - this.filled, samples.length - offset);
      this.buffer.set(samples.subarray(offset, offset + count), this.filled);
      this.filled += count;
      offset += count;
      if (this.filled === 480) {
        const pcm = new Int16Array(480);
        for (let i = 0; i < 480; i++) {
          const sample = Math.max(-1, Math.min(1, this.buffer[i]));
          pcm[i] = sample < 0 ? sample * 32768 : sample * 32767;
        }
        this.port.postMessage(pcm, [pcm.buffer]);
        this.filled = 0;
      }
    }
    return true;
  }
}

registerProcessor("capture", Capture);
