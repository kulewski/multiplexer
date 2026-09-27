// The room page: the microphone into frames over the socket, the room's
// frames into the playback worklet and onto the canvas, and the meter.
// Frame layouts as in room/consumers.py.
const UP_HEADER = 12;      // seq uint32, captured_at float64
const DOWN_HEADER = 40;    // participant, seq, captured_at, gateway_us, worker_us, worker[16]
const BANDS = 32;

let socket = null, context = null, playback = null, closed = false;
let participant = 0, seq = 0;
const spectra = new Map();   // participant -> {bands, worker, at}
const recent = {e2e: [], gateway: [], worker: []};
let sent = 0, received = 0, buffered = {}, underruns = 0;

function median(values) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function keep(list, value) {
  list.push(value);
  if (list.length > 100) list.shift();
}

// The audio graph and the microphone first, the socket last: the room's
// frames start arriving the moment the socket joins it.
async function start() {
  document.querySelector("#start").disabled = true;
  context = new AudioContext({sampleRate: 48000});
  await context.audioWorklet.addModule(document.querySelector("script[src$='room.js']").src.replace("room.js", "capture.js"));
  await context.audioWorklet.addModule(document.querySelector("script[src$='room.js']").src.replace("room.js", "playback.js"));
  const media = await navigator.mediaDevices.getUserMedia({
    audio: {channelCount: 1, echoCancellation: false, noiseSuppression: false, autoGainControl: false},
  });
  playback = new AudioWorkletNode(context, "playback", {numberOfInputs: 0, outputChannelCount: [1]});
  playback.port.onmessage = (event) => { buffered = event.data.buffered; underruns = event.data.underruns; };
  playback.connect(context.destination);
  const capture = new AudioWorkletNode(context, "capture", {numberOfInputs: 1, numberOfOutputs: 0});
  context.createMediaStreamSource(media).connect(capture);

  socket = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws/audio/" + ROOM + "/");
  socket.binaryType = "arraybuffer";
  socket.onmessage = (event) => {
    if (typeof event.data === "string") hello(JSON.parse(event.data));
    else onFrame(event.data);
  };
  socket.onclose = () => { closed = true; };
  capture.port.onmessage = (event) => {
    const buffer = new ArrayBuffer(UP_HEADER + event.data.byteLength);
    const view = new DataView(buffer);
    view.setUint32(0, seq++, true);
    view.setFloat64(4, performance.now(), true);
    new Int16Array(buffer, UP_HEADER).set(event.data);
    if (socket.readyState === WebSocket.OPEN) { socket.send(buffer); sent++; }
  };
  requestAnimationFrame(draw);
}

function hello(data) {
  participant = data.participant;
  const effects = document.querySelector("#effects");
  effects.innerHTML = "";
  for (const effect of data.effects) {
    const label = document.createElement("label");
    const radio = document.createElement("input");
    radio.type = "radio"; radio.name = "effect"; radio.value = effect; radio.checked = effect === "none";
    radio.onchange = () => socket.send(JSON.stringify({effect}));
    label.append(radio, " " + effect);
    effects.append(label);
  }
  document.querySelector("#meter").textContent = "participant " + participant + " on gateway " + data.gateway;
}

function onFrame(data) {
  const view = new DataView(data);
  const from = view.getUint32(0, true);
  const frameSeq = view.getUint32(4, true);
  const capturedAt = view.getFloat64(8, true);
  const gatewayUs = view.getUint32(16, true);
  const workerUs = view.getUint32(20, true);
  const worker = new TextDecoder().decode(new Uint8Array(data, 24, 16)).replace(/\0+$/, "");
  const bands = new Uint8Array(data.slice(DOWN_HEADER, DOWN_HEADER + BANDS));
  const pcm = new Int16Array(data.slice(DOWN_HEADER + BANDS));
  received++;
  if (from === participant) {
    keep(recent.e2e, performance.now() - capturedAt);
    keep(recent.gateway, gatewayUs / 1000);
    keep(recent.worker, workerUs / 1000);
  }
  if (!(from === participant && !document.querySelector("#self").checked)) {
    playback.port.postMessage({participant: from, seq: frameSeq, pcm}, [pcm.buffer]);
  }
  spectra.set(from, {bands, worker, at: performance.now()});
}

function draw() {
  const canvas = document.querySelector("#spectrum");
  const now = performance.now();
  for (const [from, {at}] of spectra) if (now - at > 1000) spectra.delete(from);
  const height = Math.max(1, spectra.size) * 40;  // a row per participant, however many
  if (canvas.height !== height) canvas.height = height;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  let row = 0;
  for (const [from, {bands, worker}] of spectra) {
    const top = row * 40, width = canvas.width / BANDS;
    ctx.fillStyle = from === participant ? "#2a6" : "#36c";
    for (let band = 0; band < BANDS; band++) {
      const height = bands[band] / 255 * 36;
      ctx.fillRect(band * width + 1, top + 38 - height, width - 2, height);
    }
    ctx.fillStyle = "#000";
    ctx.fillText((from === participant ? "you" : from) + " via " + worker, 4, top + 12);
    row++;
  }
  const meter = document.querySelector("#meter");
  const held = Object.entries(buffered).map(([who, frames]) => (who == participant ? "you" : who) + ":" + frames).join(" ");
  meter.textContent = (closed ? "the socket closed\n" : "") +
    "participant " + participant + "   sent " + sent + "   received " + received + "\n" +
    "your frames, round trip capture to arrival: " + median(recent.e2e).toFixed(1) + " ms" +
    "   of which the broker round trip, at the gateway: " + median(recent.gateway).toFixed(2) + " ms" +
    "   of which the worker: " + median(recent.worker).toFixed(3) + " ms\n" +
    "playback buffer (frames of 10 ms): " + held + "   underruns " + underruns;
  requestAnimationFrame(draw);
}

document.querySelector("#start").onclick = () => start().catch((error) => {
  if (socket) socket.close();
  if (context) context.close();
  socket = context = playback = null;
  document.querySelector("#meter").textContent = "could not start: " + error;
  document.querySelector("#start").disabled = false;
});
