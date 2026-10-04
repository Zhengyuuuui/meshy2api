// Meshy .meshy -> .glb decryptor (pure Node, no browser/quota).
// Reverse-engineered from /resource/decrypt/{loader-worker.min.js,mesh_loader.js,mesh_loader.wasm}.
// Usage: node meshy_decrypt.mjs <input.meshy> [output.glb] [mode=default|texture-editor]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import M from "./mesh_loader.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const MASK = (1n << 64n) - 1n;
const PRIME = 1099511628211n;

// FNV-1a 64-bit over key first, then message, then murmur-style finalizer.
// Bundle: c(e,t) = fn(e+":"+t, "Meshy_Crypto_Key"); worker authorize(hostname, ts, signature)
function signature(hostname, timestamp) {
  const msg = `${hostname}:${timestamp}`;
  const key = "Meshy_Crypto_Key";
  let r = 14695981039346656037n;
  for (let i = 0; i < key.length; i++) { r ^= BigInt(key.charCodeAt(i)); r = (r * PRIME) & MASK; }
  for (let i = 0; i < msg.length; i++) { r ^= BigInt(msg.charCodeAt(i)); r = (r * PRIME) & MASK; }
  r ^= r >> 33n; r = (r * 0xff51afd7ed558ccdn) & MASK;
  r ^= r >> 33n; r = (r * 0xc4ceb9fe1a85ec53n) & MASK;
  r ^= r >> 33n;
  return r.toString(16).padStart(16, "0");
}

let _module = null;
export async function getDecryptor() {
  if (!_module) {
    _module = await M({ locateFile: (f) => (f.endsWith(".wasm") ? path.join(HERE, "mesh_loader.wasm") : f) });
    const host = "localhost"; // in WASM hostname allowlist
    const ts = Date.now();
    if (!_module.authorize(host, ts, signature(host, ts))) throw new Error("meshy decrypt authorize failed");
  }
  return _module;
}

export function isMeshyEncrypted(bytes) {
  return bytes.length >= 8 && Buffer.from(bytes.subarray(0, 8)).toString("latin1") === "MESHY.AI";
}

export async function decryptMeshy(bytes, mode = "default") {
  const m = await getDecryptor();
  const fn = mode === "texture-editor" ? m.processMeshyFileForTextureEditor : m.processMeshyFile;
  const r = fn(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes));
  if (!r.success) throw new Error(r.error || "processMeshyFile failed");
  if (!r.data) throw new Error("processMeshyFile returned no data");
  return Buffer.from(r.data.buffer.slice(r.data.byteOffset, r.data.byteOffset + r.data.byteLength));
}

// CLI
if (import.meta.url === `file://${process.argv[1]}`) {
  const [, , input, output, mode = "default"] = process.argv;
  if (!input) { console.error("usage: node meshy_decrypt.mjs <input.meshy> [output.glb] [mode]"); process.exit(1); }
  const enc = fs.readFileSync(input);
  if (!isMeshyEncrypted(enc)) { console.error("not a MESHY.AI container"); process.exit(2); }
  const glb = await decryptMeshy(enc, mode);
  const out = output || input.replace(/\.meshy$/i, "") + ".glb";
  fs.writeFileSync(out, glb);
  console.log(`decrypted ${input} -> ${out} (${glb.length} bytes, magic ${glb.subarray(0, 4).toString("latin1")})`);
}
