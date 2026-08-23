#!/usr/bin/env python3
"""Re-measure ACP tearing from WebStorm's own log.

Counts, for a window of the log, how many adjacent AgentMessageChunk pairs have
another notification between them. That is what the JetBrains client flushes the
Markdown block on, so it is the number the fix has to move.
"""
import collections, os, re, sys

LOG = os.path.expanduser("~/Library/Logs/JetBrains/WebStorm2026.2/acp/acp.log")
SINCE = sys.argv[1] if len(sys.argv) > 1 else "1970-01-01 00:00:00"

raw = open(LOG, encoding="utf-8", errors="replace").read()
recs = []
for m in re.finditer(r"Received notification: (\w+)\(", raw):
    line_start = raw.rfind("\n", 0, m.start()) + 1
    ts = raw[line_start:line_start + 19]
    nxt = raw.find("Received notification: ", m.end())
    body = raw[m.end():nxt if nxt != -1 else len(raw)]
    mid = re.search(r"messageId=([^,\)\s]*)", body)
    if ts >= SINCE:
        recs.append((ts, m.group(1), mid.group(1) if mid else None))

kinds = collections.Counter(k for _, k, _ in recs)
chunks = [r for r in recs if r[1] == "AgentMessageChunk"]
ids = collections.Counter("null" if c[2] in (None, "null") else "set" for c in chunks)
idx = [i for i, r in enumerate(recs) if r[1] == "AgentMessageChunk"]
pairs = list(zip(idx, idx[1:]))
between = collections.Counter()
torn = 0
for a, b in pairs:
    mids = [recs[k][1] for k in range(a + 1, b)]
    if mids:
        torn += 1
        between.update(mids)

print("window from", SINCE, "->", recs[-1][0] if recs else "(empty)")
print("notifications:", kinds.most_common())
print("chunks:", len(chunks), "messageId:", dict(ids), "distinct ids:", len({c[2] for c in chunks}))
print("torn pairs: %d of %d (%d%%)" % (torn, len(pairs), 100 * torn // max(1, len(pairs))))
print("interleavers:", between.most_common(6))
