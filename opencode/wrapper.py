import json
import time
from ..pipeline import Handler, RequestInfo

def make_id(prefix):
	ts = int(time.time() * 1000)
	v = ((ts << 12) | 1) ^ 0xFFFFFFFFFFFFFF  # opencode stores ~(ts<<12|ctr) for desc sort
	v &= 0xFFFFFFFFFFFF
	hexp = "".join(f"{(v >> (40 - 8 * i)) & 0xFF:02x}" for i in range(6))
	b62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	return prefix + hexp + "".join(secrets.choice(b62) for _ in range(14))

class OpencodeWrapper(Handler):
	"""
	делает запрос таким как будто его отправил opencode
	"""
	
	def __init__(
		self,
		backend,
		newh = {
			"User-Agent": "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
			"x-opencode-client": "cli", 
			"x-opencode-project": "global",
			"x-opencode-request": lambda : make_id("msg_"), 
			"x-opencode-session": lambda : make_id("ses_"),
		},
		delh = {},
		tools = ["bash", "glob", "grep", "read"],
		body = {
			"tool_choice": "auto",
			"stream": True, 
			"stream_options": {"include_usage": True},
		}
	):
		self.backend = backend
		self.newh = newh
		self.delh = delh
		self.tools = tools
		self.body = body

	async def ahandle(self, request: RequestInfo, body: bytes):
		new_headers = []
		done = set()
		for k,v in request.headers:
			if k in self.delh:        continue
			if k in self.newh:
				v = self.newh[k]
				done.add(k)
				if type(v) is str:    new_headers.append((k,v))
				else:                 new_headers.append((k,v()))
		for k,v in self.newh.items():
			if k in done:         continue
			if type(v) is str:    new_headers.append((k,v))
			else:                 new_headers.append((k,v()))
		request.headers = new_headers

		body = json.loads(body.decode())
		for k,v in self.body.items():
			if k not in body: body[k] = v

		tools = set()
		if "tools" not in body: body["tools"] = []
		for tool in body["tools"]:
			if tool.get("type") == "function":
				tools.add(tool.get("function",{"name":None}).get("name"))
		for tool in self.tools:
			if tool not in tools:
				body["tools"].append({"type": "function", "function": {"name": tool, "description": "Don't use this tool."}})

		return await self.backend.ahandle(request, json.dumps(body).encode())