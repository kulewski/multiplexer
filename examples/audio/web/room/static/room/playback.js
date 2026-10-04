// The playback worklet: a queue of frames per participant, a jitter
// buffer of two frames before a participant starts, at most six so that
// a slow moment never turns into a lasting delay, and the output the sum
// of everyone's current frame. The frames' numbers keep each queue in
// order: one older than the last queued is dropped, and a gap is filled
// with silence, 10 ms per frame missing. It reports how much it holds,
// for the meter.
const TARGET = 2;
const LIMIT = 6;

class Playback extends AudioWorkletProcessor {
  constructor() {
    super();
    this.streams = new Map();  // participant -> {frames: [Float32Array], position, started}
    this.underruns = 0;
    this.quanta = 0;
    this.port.onmessage = (event) => this.enqueue(event.data);
  }

  enqueue({participant, seq, pcm}) {
    let stream = this.streams.get(participant);
    if (!stream) {
      stream = {frames: [], position: 0, started: false, last: currentTime, seq: seq - 1};
      this.streams.set(participant, stream);
    }
    if (seq <= stream.seq) return;  // late: its place was played already
    for (let missing = Math.min(seq - stream.seq - 1, LIMIT); missing > 0; missing--) {
      stream.frames.push(new Float32Array(pcm.length));
    }
    stream.seq = seq;
    const frame = new Float32Array(pcm.length);
    for (let i = 0; i < pcm.length; i++) frame[i] = pcm[i] / 32768;
    stream.frames.push(frame);
    stream.last = currentTime;
    while (stream.frames.length > LIMIT) stream.frames.shift();  // too far behind: skip ahead
    if (!stream.started && stream.frames.length >= TARGET) stream.started = true;
  }

  process(inputs, outputs) {
    const output = outputs[0][0];
    output.fill(0);
    for (const [participant, stream] of this.streams) {
      if (currentTime - stream.last > 2) { this.streams.delete(participant); continue; }
      if (!stream.started) continue;
      for (let i = 0; i < output.length; i++) {
        if (stream.frames.length === 0) { this.underruns++; stream.started = false; break; }
        output[i] += stream.frames[0][stream.position++];
        if (stream.position === stream.frames[0].length) { stream.frames.shift(); stream.position = 0; }
      }
    }
    if (++this.quanta % 50 === 0) {  // about every 130 ms
      const buffered = {};
      for (const [participant, stream] of this.streams) buffered[participant] = stream.frames.length;
      this.port.postMessage({buffered, underruns: this.underruns});
    }
    return true;
  }
}

registerProcessor("playback", Playback);
