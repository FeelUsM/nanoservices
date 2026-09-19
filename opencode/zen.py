#!/usr/bin/env python3
"""Call big-pickle (OpenCode Zen free model) via plain HTTP, like the opencode client does.

Works with any valid OPENCODE_API_KEY (sk-...) or with the anonymous flow (Bearer public).
Server validates:
  - x-opencode-session / x-opencode-request must match opencode's ID format
      (prefix + 12 lowercase hex + 14 base62 chars)
  - x-opencode-client, User-Agent
  - request body must carry opencode-style tool definitions (>=4: bash/glob/grep/read)
"""
import argparse, json, secrets, sys, time, urllib.request

BASE = "https://opencode.ai/zen/v1"
TOOLS = ["bash", "glob", "grep", "read"]  # minimal set that unlocks free tier

TOOL_DEFS = [
    {"type": "function", "function": {"name": "bash", "description": "Execute a command in a shell.",
        "parameters": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}},
    {"type": "function", "function": {"name": "glob", "description": "Find files by glob pattern.",
        "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}, "required": ["pattern"]}}},
    {"type": "function", "function": {"name": "grep", "description": "Search file contents with regex.",
        "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}, "required": ["pattern"]}}},
    {"type": "function", "function": {"name": "read", "description": "Read a file.",
        "parameters": {"type": "object", "properties": {"filePath": {"type": "string"}}, "required": ["filePath"]}}},
]

def os_environ(k):
    import os
    return os.environ.get(k)

def make_id(prefix):
    ts = int(time.time() * 1000)
    v = ((ts << 12) | 1) ^ 0xFFFFFFFFFFFFFF  # opencode stores ~(ts<<12|ctr) so ids sort desc
    v &= 0xFFFFFFFFFFFF
    hexp = "".join(f"{(v >> (40 - 8 * i)) & 0xFF:02x}" for i in range(6))
    b62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    rnd = "".join(secrets.choice(b62) for _ in range(14))
    return prefix + hexp + rnd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt", nargs="*", default=["Say exactly: hi"])
    ap.add_argument("-k", "--key", default=None, help="OPENCODE_API_KEY (default: env or 'public')")
    ap.add_argument("-m", "--model", default="big-pickle")
    ap.add_argument("--session", default=None)
    ap.add_argument("--project", default="global")
    args = ap.parse_args()

    key = args.key or os_environ("OPENCODE_API_KEY") or "public"
    body = {
        "model": args.model,
        "max_tokens": 32000,
        "messages": [
            {"role": "system", "content": "You are opencode, an interactive CLI tool that helps users with software engineering tasks."},
            {"role": "user", "content": " ".join(args.prompt)},
        ],
        "tools": TOOL_DEFS,
        "tool_choice": "auto",
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        "x-opencode-client": "cli",
        "x-opencode-project": args.project,
        "x-opencode-request": make_id("msg_"),
        "x-opencode-session": make_id("ses_"),
    }
    if args.session:
        headers["x-opencode-session"] = args.session

    req = urllib.request.Request(BASE + "/chat/completions", data=json.dumps(body).encode(),
                                 headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        for raw in r:
            line = raw.decode().strip()
            if not line.startswith("data: "):
                continue
            payload = line[6:]
            if payload == "[DONE]":
                continue
            try:
                chunk = json.loads(payload)
                for ch in chunk.get("choices", []):
                    d = ch.get("delta", {})
                    if d.get("content"):
                        sys.stdout.write(d["content"])
                        sys.stdout.flush()
            except Exception:
                pass
    print()

if __name__ == "__main__":
    main()