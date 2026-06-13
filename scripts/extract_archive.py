"""One-pass streaming extraction of the truncated polymarket tar.zst archive.

- Members already compressed (.jsonl.zst) are written verbatim.
- Raw .jsonl members are recompressed to .zst on the fly (level 3, multithread)
  so the 50GB+ price_change files don't eat the disk.
- Truncation is handled gracefully: whatever bytes exist are kept and the
  manifest records partial members.
"""
import io
import os
import sys
import json
import tarfile
import time

import zstandard as zstd

ARCHIVE = r"C:\Users\tico_\Fable\5minSnip\polymarket_recorder_2026-06-04_to_06-11.tar.zst"
OUTDIR = r"C:\Users\tico_\Fable\5minSnip\data\raw"
MANIFEST = r"C:\Users\tico_\Fable\5minSnip\data\extract_manifest.json"

os.makedirs(OUTDIR, exist_ok=True)

manifest = []
start = time.time()

dctx = zstd.ZstdDecompressor()
with open(ARCHIVE, "rb") as fh:
    reader = dctx.stream_reader(fh, read_across_frames=True)
    buffered = io.BufferedReader(reader, buffer_size=32 * 1024 * 1024)
    tf = tarfile.open(fileobj=buffered, mode="r|")
    try:
        for member in tf:
            if not member.isfile():
                continue
            rel = member.name.replace("polymarket/", "", 1)
            src = tf.extractfile(member)
            entry = {"name": member.name, "size": member.size, "written": 0,
                     "partial": False}
            if member.name.endswith(".zst") or member.size < 200 * 1024 * 1024:
                # write verbatim
                dest_rel = rel
                dest = os.path.join(OUTDIR, dest_rel.replace("/", os.sep))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                written = 0
                try:
                    with open(dest, "wb") as out:
                        while True:
                            chunk = src.read(16 * 1024 * 1024)
                            if not chunk:
                                break
                            out.write(chunk)
                            written += len(chunk)
                except Exception as e:  # truncated mid-member
                    entry["partial"] = True
                    entry["error"] = repr(e)
                entry["written"] = written
                entry["dest"] = dest_rel
            else:
                # giant raw jsonl -> recompress streaming
                dest_rel = rel + ".zst"
                dest = os.path.join(OUTDIR, dest_rel.replace("/", os.sep))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                cctx = zstd.ZstdCompressor(level=3, threads=8)
                written = 0
                try:
                    with open(dest, "wb") as out:
                        with cctx.stream_writer(out) as cw:
                            while True:
                                chunk = src.read(16 * 1024 * 1024)
                                if not chunk:
                                    break
                                cw.write(chunk)
                                written += len(chunk)
                except Exception as e:
                    entry["partial"] = True
                    entry["error"] = repr(e)
                entry["written"] = written
                entry["dest"] = dest_rel
            if entry["written"] < member.size:
                entry["partial"] = True
            manifest.append(entry)
            print(f"[{time.time()-start:7.0f}s] {member.name} size={member.size:,} "
                  f"written={entry['written']:,} partial={entry['partial']}", flush=True)
    except Exception as e:
        print(f"STREAM ENDED: {e!r}", flush=True)
        manifest.append({"stream_error": repr(e)})

with open(MANIFEST, "w") as f:
    json.dump(manifest, f, indent=2)
print(f"DONE in {time.time()-start:.0f}s")
