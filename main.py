import asyncio
import json
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests
import websockets
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONFIG_FILE = "config.yaml"

def load_config() -> Dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

class RealtimeItemAdded:

    def __init__(self):
        self.cfg = load_config()
        self.server = self.cfg["server_url"].rstrip("/")
        self.forward_url = self.cfg.get("forward_url")
        self.group_id = self.cfg.get("group_id")

        fcfg = self.cfg.get("filters", {}) or {}
        wl = fcfg.get("whitelist", {}) or {}
        bl = fcfg.get("blacklist", {}) or {}
        self.whitelist_enabled = bool(wl.get("enabled", False))
        self.whitelist = [str(x).lower() for x in (wl.get("keywords") or [])]
        self.blacklist_enabled = bool(bl.get("enabled", False))
        self.blacklist = [str(x).lower() for x in (bl.get("keywords") or [])]

        self.library_policies = self._normalize_policies(self.cfg.get("library_policies", []))
        self.library_policies_norm: Dict[str, Dict] = {}
        for k, v in self.library_policies.items():
            if isinstance(k, str):
                self.library_policies_norm[self._norm_name(k)] = v
        self.client = "Yukari Notify Bot"
        self.device = "Yukari Notify Bot"
        self.version = "1.0.0"
        self.session = requests.Session()
        self.token = None
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504), allowed_methods=("GET", "POST"))
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._library_cache: Dict[str, Tuple[str, str]] = {}
        self._ancestors_cache: Dict[str, List[Dict]] = {}
        dedupe_cfg = (self.cfg.get("dedupe") or {})
        try:
            self.season_summary_delay = int(
                dedupe_cfg.get("season_summary_delay_seconds")
                or dedupe_cfg.get("season_summary_ttl_seconds", 300)
            )
        except Exception:
            self.season_summary_delay = 300
        try:
            self.movie_delay_seconds = int(dedupe_cfg.get("movie_delay_seconds", 300))
        except Exception:
            self.movie_delay_seconds = 300
        self.season_summary_delay = max(1, self.season_summary_delay)
        self.movie_delay_seconds = max(1, self.movie_delay_seconds)
        self._season_batches: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self._season_lock: Optional[asyncio.Lock] = None
        self._season_flush_task: Optional[asyncio.Task] = None
        self._movie_queue: Dict[str, Dict[str, Any]] = {}
        self._movie_lock: Optional[asyncio.Lock] = None
        self._warned_libs: set[str] = set()

    def _normalize_policies(self, arr):
        """Expand to {libraryNameOrId: {mode}}; supports 'library' or 'libraries'."""
        policies: Dict[str, Dict] = {}
        for x in arr:
            keys: List[str] = []
            if "libraries" in x and x["libraries"]:
                keys = [str(k).strip() for k in x["libraries"]]
            elif "library" in x and x["library"]:
                keys = [str(x["library"]).strip()]
            else:
                continue
            policy = {"mode": x.get("mode", "per_episode")}
            for key in keys:
                policies[key] = policy
        return policies

    def _pick_policy(self, lib_id: str, lib_name: str):
        """Prefer library ID, then name."""
        p = self.library_policies.get(lib_id)
        if p:
            return p
        p = self.library_policies.get(lib_name)
        if p:
            return p
        norm = self._norm_name(lib_name)
        p = self.library_policies_norm.get(norm)
        if p:
            return p
        if norm:
            for k, v in self.library_policies_norm.items():
                if not k:
                    continue
                if k in norm or norm in k:
                    return v
        return None

    def _norm_name(self, s: Optional[str]) -> str:
        if not s:
            return ""
        table = str.maketrans({
            "（": "(",
            "）": ")",
            "【": "[",
            "】": "]",
            "｛": "{",
            "｝": "}",
        })
        out = s.translate(table)
        out = out.replace("\u3000", " ").strip().lower()
        return out

    def login(self):
        headers = {
            "X-Emby-Authorization": f'MediaBrowser Client="{self.client}", Device="{self.device}", DeviceId="{self.device}", Version="{self.version}"'
        }
        r = self.session.post(
            f"{self.server}/Users/AuthenticateByName",
            headers=headers,
            json={"Username": self.cfg["username"], "Pw": self.cfg["password"]},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["AccessToken"]
        self.session.headers.update({"X-Emby-Token": self.token})
        dest_out = self.forward_url or "未配置"
        gid_out = str(self.group_id) if self.group_id is not None else "未配置"
        print("已连接到 Jellyfin")
        print(f"通知目标: [{dest_out}]")
        print(f"群号: [{gid_out}]")
        print("Kira~")

    def get_items_by_ids(self, ids: List[str]) -> List[Dict]:
        if not ids:
            return []
        r = None
        try:
            r = self.session.get(
                f"{self.server}/Items",
                params={
                    "ids": ",".join(ids),
                    "enableImages": "true",
                    "fields": ",".join(
                        [
                            "SeriesId",
                            "SeriesName",
                            "SeasonId",
                            "ParentId",
                            "ParentIndexNumber",
                            "IndexNumber",
                            "ProductionYear",
                            "RunTimeTicks",
                            "Overview",
                            "Path",
                            "Album",
                            "AlbumArtist",
                            "Artists",
                            "MediaType",
                        ]
                    ),
                },
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            return data.get("Items") or data.get("items") or []
        except Exception as e:
            status_code = None
            try:
                import requests as _rq
                if isinstance(e, _rq.RequestException) and getattr(e, "response", None) is not None:
                    status_code = e.response.status_code
            except Exception:
                pass
            if status_code is None and r is not None:
                status_code = getattr(r, "status_code", None)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]【获取Items失败】状态码 {status_code} 错误 {e}")
            return []

    def get_library_of_item(self, item_id: str) -> Tuple[str, str]:
        if item_id in self._library_cache:
            return self._library_cache[item_id]
        r = None
        try:
            r = self.session.get(f"{self.server}/Items/{item_id}/Ancestors", timeout=15)
            r.raise_for_status()
            lib = ("", "")
            for a in r.json() or []:
                if a.get("Type") == "CollectionFolder":
                    lib = (a.get("Id") or "", a.get("Name") or "")
                    break
            self._library_cache[item_id] = lib
            return lib
        except Exception as e:
            status_code = None
            try:
                import requests as _rq
                if isinstance(e, _rq.RequestException) and getattr(e, "response", None) is not None:
                    status_code = e.response.status_code
            except Exception:
                pass
            if status_code is None and r is not None:
                status_code = getattr(r, "status_code", None)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]【获取库信息失败】Item {item_id} 状态码 {status_code} 错误 {e}")
            return "", ""

    def get_library_for(self, item: Dict) -> Tuple[str, str]:
        """Resolve library using SeriesId/ParentId when possible to reuse cache."""
        key = item.get("SeriesId") or item.get("ParentId") or item.get("Id")
        if not key:
            return "", ""
        if key in self._library_cache:
            return self._library_cache[key]
        lib = self.get_library_of_item(key)
        item_id = item.get("Id")
        if item_id and item_id not in self._library_cache:
            self._library_cache[item_id] = lib
        return lib

    def get_ancestors(self, item_key: str) -> List[Dict]:
        if not item_key:
            return []
        if item_key in self._ancestors_cache:
            return self._ancestors_cache[item_key]
        r = None
        try:
            r = self.session.get(f"{self.server}/Items/{item_key}/Ancestors", timeout=15)
            r.raise_for_status()
            arr = r.json() or []
            self._ancestors_cache[item_key] = arr
            return arr
        except Exception:
            return []

    def _pick_policy_for_item(self, item: Dict):
        key = item.get("SeriesId") or item.get("ParentId") or item.get("Id")
        ancestors = self.get_ancestors(key)
        for a in ancestors:
            pid = a.get("Id") or ""
            pname = a.get("Name") or ""
            pol = self._pick_policy(pid, pname)
            if pol:
                return pol
        lib_id, lib_name = self.get_library_for(item)
        return self._pick_policy(lib_id, lib_name)

    def _hay_from_item(self, item: Dict) -> str:
        parts = [
            item.get("Name"),
            item.get("SeriesName"),
            item.get("Overview"),
            item.get("Path"),
            item.get("ProductionYear"),
            item.get("Type"),
            item.get("Album"),
            item.get("AlbumArtist"),
            ", ".join(item.get("Artists", [])) if item.get("Artists") else None,
        ]
        return " ".join(str(x) for x in parts if x).lower()

    def _hay_for_series(self, series_name: str, example_item: Dict) -> str:
        parts = [series_name, example_item.get("Path")]
        return " ".join(str(x) for x in parts if x).lower()

    def _pass_filters(self, hay_lower: str) -> bool:
        if self.blacklist_enabled and self.blacklist:
            if any(k in hay_lower for k in self.blacklist):
                return False
        if self.whitelist_enabled:
            if not self.whitelist or not any(k in hay_lower for k in self.whitelist):
                return False
        return True

    async def _ensure_season_helpers(self):
        if self._season_lock is None:
            self._season_lock = asyncio.Lock()
        if self._season_flush_task is None or self._season_flush_task.done():
            self._season_flush_task = asyncio.create_task(self._season_batch_flusher())

    async def _queue_season_summary(self, series_id: str, season_no: int, series_name: str, episodes: List[Dict]):
        if not episodes:
            return
        await self._ensure_season_helpers()
        wait = max(1, int(self.season_summary_delay))
        season_key = (series_id or "", int(season_no or 0))
        async with self._season_lock:
            entry = self._season_batches.get(season_key)
            if entry is None:
                entry = {
                    "series_id": series_id,
                    "season_no": int(season_no or 0),
                    "series_name": series_name,
                    "episodes": {},
                    "due_time": time.time() + wait,
                }
                self._season_batches[season_key] = entry
            else:
                if series_name and not entry.get("series_name"):
                    entry["series_name"] = series_name
                entry["due_time"] = time.time() + wait
            episodes_dict: Dict[str, Dict] = entry["episodes"]
            for ep in episodes:
                ep_id = str(ep.get("Id") or ep.get("EpisodeId") or len(episodes_dict))
                episodes_dict[ep_id] = ep

    async def _flush_due_batches(self):
        if self._season_lock is None:
            return
        now = time.time()
        ready: List[Dict[str, Any]] = []
        async with self._season_lock:
            due_keys = []
            for key, entry in self._season_batches.items():
                due_time = entry.get("due_time")
                if due_time and due_time <= now and entry.get("episodes"):
                    due_keys.append(key)
            for key in due_keys:
                entry = self._season_batches.pop(key, None)
                if entry:
                    ready.append(entry)
        for entry in ready:
            episodes_dict: Dict[str, Dict] = entry.get("episodes", {})
            episodes_list = list(episodes_dict.values())
            if not episodes_list:
                continue
            series_name = entry.get("series_name") or (episodes_list[0].get("SeriesName") or "")
            season_no = entry.get("season_no")
            series_id = entry.get("series_id") or (episodes_list[0].get("SeriesId") or "")
            if len(episodes_list) == 1:
                payload = self.build_payload(episodes_list[0])
                await self.forward_async(payload)
            else:
                payload = self.build_season_payload(series_name, season_no, len(episodes_list), series_id)
                await self.forward_async(payload)

    async def _season_batch_flusher(self):
        try:
            while True:
                await asyncio.sleep(5)
                await self._flush_due_batches()
                await self._flush_due_movies()
        except asyncio.CancelledError:
            pass

    async def _queue_movie(self, item: Dict):
        await self._ensure_season_helpers()
        if self._movie_lock is None:
            self._movie_lock = asyncio.Lock()
        key = str(item.get("Id") or "")
        if not key:
            return
        wait = max(1, int(self.movie_delay_seconds))
        now = time.time()
        async with self._movie_lock:
            entry = self._movie_queue.get(key)
            if entry is None:
                self._movie_queue[key] = {"item": item, "due_time": now + wait}
            else:
                entry["item"] = item

    async def _flush_due_movies(self):
        if self._movie_lock is None:
            return
        now = time.time()
        ready: List[Dict[str, Any]] = []
        async with self._movie_lock:
            due_keys = [k for k, v in self._movie_queue.items() if v.get("due_time", 0) <= now]
            for k in due_keys:
                entry = self._movie_queue.pop(k, None)
                if entry:
                    ready.append(entry)
        for entry in ready:
            item = entry.get("item") or {}
            if not self._pass_filters(self._hay_from_item(item)):
                continue
            payload = self.build_payload(item)
            await self.forward_async(payload)

    def build_payload(self, item: Dict) -> Dict:
        t = (item.get("Type") or "").strip()
        name = item.get("Name") or ""
        year = item.get("ProductionYear") or ""
        series_name = item.get("SeriesName") or ""
        season_no = item.get("ParentIndexNumber")
        ep_no = item.get("IndexNumber")
        series_id = item.get("SeriesId") or item.get("Id")
        runtime_ticks = item.get("RunTimeTicks") or 0

        def to_hhmmss(ticks):
            try:
                s = int(ticks // 10_000_000)
            except Exception:
                return "00:00:00"
            h = s // 3600
            m = (s % 3600) // 60
            sec = s % 60
            return f"{h:02d}:{m:02d}:{sec:02d}"

        def hhmmss_for_video():
            return to_hhmmss(runtime_ticks) if (t in ("Episode", "Movie", "MusicVideo") and runtime_ticks) else None

        if t == "Episode":
            title = f"{series_name} 更新啦！\nS{int(season_no or 0):02d}E{int(ep_no or 0):02d}\n{name}"
            subject = f"{series_name} S{int(season_no or 0):02d}E{int(ep_no or 0):02d}"
            dur = hhmmss_for_video()
            msg = title + (f"\n时长：{dur}" if dur else "")
            image_item_id = series_id
        elif t == "Movie":
            dur = hhmmss_for_video()
            title = f"{name} ({year}) 更新啦！"
            subject = f"{name} ({year})" if year else name
            msg = title + (f"\n时长：{dur}" if dur else "")
            image_item_id = item.get("Id")
        elif t == "MusicAlbum":
            album = name or (item.get("Album") or "")
            artist = item.get("AlbumArtist") or (", ".join(item.get("Artists", [])) if item.get("Artists") else "")
            title = f"新专辑！：{artist} - {album} ({year})" if artist else f"新专辑！：{album} ({year})"
            subject = f"{album} - {artist}" if artist else album
            msg = title
            image_item_id = item.get("Id")
        else:
            title = f"{name} 更新啦！\n媒体：{name} ({year})"
            subject = name
            dur = hhmmss_for_video()
            msg = title + (f"\n时长：{dur}" if dur else "")
            image_item_id = item.get("Id")

        image_url = f"{self.server}/Items/{image_item_id}/Images/Primary"
        payload = {"group_id": self.group_id, "message": f"{msg}\n[CQ:image,file={image_url}]", "subject": subject}
        return payload

    def build_season_payload(self, series_name: str, season_no: int, ep_count: int, series_id: str) -> Dict:
        title = f"{series_name} 更新啦！\n第{int(season_no):02d}季（共 {ep_count} 集）"
        image_url = f"{self.server}/Items/{series_id}/Images/Primary"
        payload = {"group_id": self.group_id, "message": f"{title}\n[CQ:image,file={image_url}]", "subject": f"{series_name} S{int(season_no):02d} 合集"}
        return payload

    async def forward_async(self, payload: Dict):
        if not self.forward_url:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]【未配置转发地址】已跳过")
            return
        subj = payload.get("subject") or "通知"
        try:
            resp = await asyncio.to_thread(self.session.post, self.forward_url, json=payload, timeout=10)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if resp.status_code < 300:
                print(f"[{ts}]【{subj}】转发成功~")
            else:
                print(f"[{ts}]【{subj}】转发失败~ 状态码 {resp.status_code}")
        except Exception as e:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]【{subj}】转发异常~ {e}")

    def forward(self, payload: Dict):
        if not self.forward_url:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]，[未配置转发地址]，已跳过")
            return
        subj = payload.get("subject") or "通知"
        try:
            resp = self.session.post(self.forward_url, json=payload, timeout=20)
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if resp.status_code < 300:
                print(f"[{ts}]，[{subj}]，转发成功~")
            else:
                print(f"[{ts}]，[{subj}]，转发失败~ 状态码 {resp.status_code}")
        except Exception as e:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}]，[{subj}]，转发异常~ {e}")

    async def run_ws(self):
        ws_url = (self.server.replace("http", "ws").rstrip("/")) + "/socket"
        headers = {
            "X-Emby-Authorization": f'MediaBrowser Client="{self.client}", Device="{self.device}", DeviceId="{self.device}", Version="{self.version}"',
            "X-Emby-Token": self.token,
        }
        params = f"?api_key={self.token}"
        backoff = 1
        while True:
            try:
                async with websockets.connect(ws_url + params, ping_interval=30) as ws:
                    backoff = 1
                    await ws.send(json.dumps({"MessageType": "KeepAlive"}))
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if (msg.get("MessageType") or msg.get("message_type")) != "LibraryChanged":
                            continue
                        data = msg.get("Data") or msg.get("data") or {}
                        items_added = data.get("ItemsAdded") or data.get("items_added") or []
                        items_updated = data.get("ItemsUpdated") or data.get("items_updated") or []
                        ids_combined: List[str] = []
                        for _id in (items_added or []):
                            if _id and _id not in ids_combined:
                                ids_combined.append(_id)
                        for _id in (items_updated or []):
                            if _id and _id not in ids_combined:
                                ids_combined.append(_id)
                        if not ids_combined:
                            continue
                        items = self.get_items_by_ids(ids_combined)
                        if not items:
                            continue
                        by_lib: Dict[str, Dict] = {}
                        for it in items:
                            item_id = it.get("Id")
                            if not item_id:
                                continue
                            lib_id, lib_name = self.get_library_for(it)
                            key = f"{lib_id or 'NA'}::{lib_name or 'NA'}"
                            pack = by_lib.setdefault(key, {"lib_id": lib_id, "lib_name": lib_name, "items": []})
                            pack["items"].append(it)
                        for pack in by_lib.values():
                            lib_id, lib_name, its = pack["lib_id"], pack["lib_name"], pack["items"]
                            policy = self._pick_policy(lib_id, lib_name) or {}
                            mode = policy.get("mode", "per_episode")
                            if not policy:
                                key = f"{lib_id or 'NA'}::{lib_name or 'NA'}"
                                if key not in self._warned_libs:
                                    print(f"[提示] 未为该库匹配到策略，按 per_episode 处理：Id={lib_id or 'NA'}, Name={lib_name or 'NA'}, Norm={self._norm_name(lib_name)}")
                                    self._warned_libs.add(key)
                            if mode == "mute":
                                continue
                            if mode == "season_summary":
                                groups: Dict[Tuple[str, int, str], List[Dict]] = defaultdict(list)
                                for it in its:
                                    pol_it = self._pick_policy_for_item(it) or policy
                                    mode_it = pol_it.get("mode", "per_episode")
                                    if mode_it != "season_summary":
                                        continue
                                    if it.get("Type") == "Episode":
                                        sid = it.get("SeriesId") or ""
                                        s_no = int(it.get("ParentIndexNumber") or 0)
                                        s_name = it.get("SeriesName") or ""
                                        groups[(sid, s_no, s_name)].append(it)
                                for (sid, s_no, s_name), eps in groups.items():
                                    if not eps:
                                        continue
                                    if not self._pass_filters(self._hay_for_series(s_name, eps[0])):
                                        continue
                                    await self._queue_season_summary(sid, s_no, s_name, eps)
                                for it in its:
                                    pol_it = self._pick_policy_for_item(it) or policy
                                    mode_it = pol_it.get("mode", "per_episode")
                                    if mode_it != "season_summary":
                                        continue
                                    if (it.get("Type") or "") == "Movie":
                                        if not self._pass_filters(self._hay_from_item(it)):
                                            continue
                                        await self._queue_movie(it)
                            elif mode == "album_only":
                                for it in its:
                                    pol_it = self._pick_policy_for_item(it) or policy
                                    mode_it = pol_it.get("mode", "per_episode")
                                    if mode_it != "album_only":
                                        continue
                                    if (it.get("Type") or "") != "MusicAlbum":
                                        continue
                                    if not self._pass_filters(self._hay_from_item(it)):
                                        continue
                                    payload = self.build_payload(it)
                                    await self.forward_async(payload)
                            else:
                                for it in its:
                                    pol_it = self._pick_policy_for_item(it) or policy
                                    mode_it = pol_it.get("mode", "per_episode")
                                    t = (it.get("Type") or "")
                                    if mode_it == "mute":
                                        continue
                                    if mode_it == "album_only":
                                        if (t == "MusicAlbum") and self._pass_filters(self._hay_from_item(it)):
                                            payload = self.build_payload(it)
                                            await self.forward_async(payload)
                                        continue
                                    if mode_it == "season_summary":
                                        if t == "Episode":
                                            sid = it.get("SeriesId") or ""
                                            s_no = int(it.get("ParentIndexNumber") or 0)
                                            s_name = it.get("SeriesName") or ""
                                            if self._pass_filters(self._hay_for_series(s_name, it)):
                                                await self._queue_season_summary(sid, s_no, s_name, [it])
                                            continue
                                        if t == "Movie":
                                            if self._pass_filters(self._hay_from_item(it)):
                                                await self._queue_movie(it)
                                            continue
                                    # per_episode default
                                    if t == "Episode":
                                        if not self._pass_filters(self._hay_from_item(it)):
                                            continue
                                        payload = self.build_payload(it)
                                        await self.forward_async(payload)
                                    elif t == "Movie":
                                        if not self._pass_filters(self._hay_from_item(it)):
                                            continue
                                        await self._queue_movie(it)
                                    else:
                                        continue
            except Exception as e:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}]，[重连等待 {backoff}s]，原因：{e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

def main():
    bot = RealtimeItemAdded()
    bot.login()
    asyncio.run(bot.run_ws())

if __name__ == "__main__":
    main()