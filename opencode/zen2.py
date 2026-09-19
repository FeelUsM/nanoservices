#!/usr/bin/env python3
"""Call OpenCode Zen free models over plain HTTP.

Server validates only the ID *format* (prefix + 12 hex + 14 base62) of
x-opencode-session / -request, a full client User-Agent, and that the body
carries opencode-style tool names. Descriptions/parameters are ignored.
"""
import argparse, json, os, secrets, sys, time, urllib.request

BASE = "https://opencode.ai/zen/v1"
TOOLS = [{"type": "function", "function": {"name": n, "description": "Don't use this tool."}} for n in
         ["bash", "glob", "grep", "read"]]

def make_id(prefix):
    ts = int(time.time() * 1000)
    v = ((ts << 12) | 1) ^ 0xFFFFFFFFFFFFFF  # opencode stores ~(ts<<12|ctr) for desc sort
    v &= 0xFFFFFFFFFFFF
    hexp = "".join(f"{(v >> (40 - 8 * i)) & 0xFF:02x}" for i in range(6))
    b62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    return prefix + hexp + "".join(secrets.choice(b62) for _ in range(14))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt", nargs="*", default=["Say exactly: hi"])
    ap.add_argument("-k", "--key", default=None)
    ap.add_argument("-m", "--model", default="big-pickle")
    ap.add_argument("-s", "--system", default="You are opencode.")
    ap.add_argument("-q", "--quiet", action="store_true")
    a = ap.parse_args()

    body = {
        "model": a.model, "max_tokens": 32000,
        "messages": [{"role": "system", "content": a.system},
                     {"role": "user", "content": " ".join(a.prompt)}],
        "tools": TOOLS, 
        "tool_choice": "auto",
        "stream": True, 
        "stream_options": {"include_usage": True},
    }
    headers = {
        "Authorization": f"Bearer {a.key or os.environ.get('OPENCODE_API_KEY') or 'public'}",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        "x-opencode-client": "cli", 
        "x-opencode-project": "global",
        "x-opencode-request": make_id("msg_"), 
        "x-opencode-session": make_id("ses_"),
    }
    req = urllib.request.Request(BASE + "/chat/completions",
                                 data=json.dumps(body).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        for raw in r:
            line = raw.decode().strip()
            #print(line)
            #continue
            if not line.startswith("data: "):
                continue
            payload = line[6:]
            if payload == "[DONE]":
                continue
            try:
                for ch in json.loads(payload).get("choices", []):
                    c = ch.get("delta", {}).get("content")
                    if c and not a.quiet:
                        sys.stdout.write(c); sys.stdout.flush()
            except Exception:
                pass
    print()

if __name__ == "__main__":
    main()