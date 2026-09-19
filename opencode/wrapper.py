import json
import secrets
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
		# Заголовки: чужие форвардим как есть, свои (newh) — подменяем/дописываем.
		# Сравнение регистронезависимо, иначе Authorization/Content-Type терялись.
		newh_lower = {str(k).lower(): (k, v) for k, v in self.newh.items()}
		if isinstance(self.delh, dict):
			del_names = self.delh.keys()
		else:
			del_names = self.delh
		del_lower = {str(k).lower() for k in del_names}
		new_headers = []
		done = set()
		for k, v in request.headers:
			lk = str(k).lower()
			if lk in del_lower:
				continue
			if lk in newh_lower:
				canon, nv = newh_lower[lk]
				done.add(lk)
				if isinstance(nv, str):
					new_headers.append((canon, nv))
				else:
					new_headers.append((canon, nv()))
			else:
				new_headers.append((k, v))
		for lk, (canon, nv) in newh_lower.items():
			if lk in done:
				continue
			if isinstance(nv, str):
				new_headers.append((canon, nv))
			else:
				new_headers.append((canon, nv()))
		request.headers = new_headers

		try:
			parsed = json.loads(body.decode())
		except Exception:
			# не-JSON (GET, пустое тело и т.п.) — форвардим как есть
			return await self.backend.ahandle(request, body)
		if not isinstance(parsed, dict):
			return await self.backend.ahandle(request, body)
		body = parsed
		for k, v in self.body.items():
			if k not in body:
				body[k] = v

		tools = set()
		if not isinstance(body.get("tools"), list):
			body["tools"] = []
		for tool in body["tools"]:
			if isinstance(tool, dict) and tool.get("type") == "function":
				fn = tool.get("function", {})
				if isinstance(fn, dict):
					tools.add(fn.get("name"))
		for tool in self.tools:
			if tool not in tools:
				body["tools"].append({"type": "function", "function": {"name": tool, "description": "Don't use this tool."}})

		return await self.backend.ahandle(request, json.dumps(body).encode())