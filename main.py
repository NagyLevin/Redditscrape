# main.py
import os
import time
import json
import argparse
from datetime import datetime, timezone
from typing import Optional, Iterable

import praw
from praw.models import Submission
from dotenv import load_dotenv
from tqdm import tqdm

# ========== SZERKESZTHETŐ ALAPÉRTELMEZÉSEK ==========
DEFAULT_SUBREDDITS = ["hikingHungary", "RealHungary"]  # több is lehet
DEFAULT_OUTDIR = "./reddit_dump"                       # alap mentési könyvtár

# ========== Exportált mezők ==========
SUB_FIELDS = [
    "id","title","author","subreddit","created_utc","selftext","url",
    "permalink","num_comments","score","over_18","spoiler","locked","stickied"
]
CMT_FIELDS = [
    "id","author","subreddit","created_utc","body","score",
    "parent_id","link_id","permalink","is_submitter"
]

# ========== Segédfüggvények ==========
def to_epoch(dt: Optional[str]) -> Optional[int]:
    """
    dt lehet:
      - None
      - '2025-08-01' (UTC 00:00:00)
      - '2025-08-01T14:30:00' (UTC)
      - epoch string (pl. '1722575400')
    """
    if dt is None:
        return None
    try:
        return int(float(dt))  # ha epoch
    except ValueError:
        pass
    if "T" in dt:
        return int(datetime.fromisoformat(dt).replace(tzinfo=timezone.utc).timestamp())
    return int(datetime.fromisoformat(dt + "T00:00:00").replace(tzinfo=timezone.utc).timestamp())

def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)

def ndjson_append(path: str, items):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def last_submission_ts(path: str) -> Optional[int]:
    """Visszaadja a fájlban talált legöregebb created_utc értéket (egyszerű resume‑hoz)."""
    if not os.path.exists(path):
        return None
    last_ts = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
                cu = int(d.get("created_utc", 0))
                if last_ts is None or cu < last_ts:
                    last_ts = cu
            except Exception:
                continue
    return last_ts

def normalize(obj: dict, fields: list[str]) -> dict:
    out = {}
    for k in fields:
        v = obj.get(k)
        if k == "author" and v is not None and not isinstance(v, str):
            v = getattr(v, "name", None)
        if k == "subreddit" and v is not None and not isinstance(v, str):
            # PRAW Subreddit objektum -> display_name
            v = str(getattr(v, "display_name", v))
        out[k] = v
    return out

# ========== Reddit init (DUAL AUTH) ==========
def init_reddit() -> praw.Reddit:
    load_dotenv()
    cid  = os.getenv("REDDIT_CLIENT_ID","").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET","").strip()
    ua   = os.getenv("REDDIT_USER_AGENT","").strip()
    user = os.getenv("REDDIT_USERNAME","").strip()
    pwd  = os.getenv("REDDIT_PASSWORD","").strip()

    if not ua:
        raise RuntimeError("Hiányzik a REDDIT_USER_AGENT.")

    def smoke_test(r):
        # minimális read scope próba
        next(iter(r.subreddit("popular").hot(limit=1)))

    # --- 1) App-only (client_credentials) ---
    if cid and csec:
        try:
            r = praw.Reddit(
                client_id=cid,
                client_secret=csec,
                user_agent=ua,
                ratelimit_seconds=5,
            )
            r.read_only = True
            smoke_test(r)
            print("[auth] OK: app-only (client_credentials)")
            return r
        except Exception as e:
            print("[auth] FAIL app-only:", repr(e))

    # --- 2) Password grant fallback (script tulaj + app password, ha kell) ---
    if cid and csec and user and pwd:
        try:
            r = praw.Reddit(
                client_id=cid,
                client_secret=csec,
                user_agent=ua,
                username=user,
                password=pwd,  # 2FA esetén: App Password!
                ratelimit_seconds=5,
            )
            # read_only itt nem kell; user-context
            smoke_test(r)
            print("[auth] OK: password grant (script)")
            return r
        except Exception as e:
            print("[auth] FAIL password:", repr(e))

    # --- Részletes hibaüzenet ---
    msg = [
        "Autentikációs hiba – egyik módszer sem sikerült.",
        f"cid_len={len(cid) if cid else 0}, csec_len={len(csec) if csec else 0}, ua_len={len(ua) if ua else 0}, "
        f"user_set={'yes' if user else 'no'}, pwd_set={'yes' if pwd else 'no'}",
        "Ellenőrizd:",
        "- Script (personal use script) típus-e az app.",
        "- Secret friss és helyes (regenerálás után frissítve a .env-ben).",
        "- .env sorvégek/whitespace (nincs rejtett szóköz).",
        "- Password grantnél App Password kellhet (2FA esetén).",
        "- USER_AGENT egyedi (pl. myredditdl/1.0 by u/Levincorp).",
    ]
    raise RuntimeError("\n".join(msg))

# ========== Letöltés ==========
def iter_new_until(subreddit, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable[Submission]:
    """
    A 'new' feedet olvassuk visszafelé. Megállunk, ha created_utc < after vagy elértük a limitet.
    """
    count = 0
    for s in subreddit.new(limit=None):
        cu = int(getattr(s, "created_utc", 0))
        if before is not None and cu > before:
            # túl friss -> lépjünk tovább (a feed desc)
            continue
        if after is not None and cu < after:
            break
        yield s
        count += 1
        if hard_limit and count >= hard_limit:
            break

def download_submissions_and_comments(
    reddit: praw.Reddit,
    subreddit_name: str,
    out_dir: str,
    after: Optional[int],
    before: Optional[int],
    limit_posts: Optional[int],
    sleep_s: float = 0.5,
    include_comments: bool = True,
):
    sr = reddit.subreddit(subreddit_name)

    ensure_dir(out_dir)
    sub_path = os.path.join(out_dir, f"{subreddit_name}.submissions.ndjson")
    cmt_path = os.path.join(out_dir, f"{subreddit_name}.comments.ndjson")

    # (Opcionális) resume infó – jelenleg csak tájékoztató
    if after is None:
        _existing_oldest = last_submission_ts(sub_path)
        # ha szeretnéd, itt lehetne after = _existing_oldest, és így keményen folytatna

    subs_saved = 0
    cmts_saved = 0
    submissions_buf = []
    comments_buf = []

    pbar = tqdm(desc=f"Submissions r/{subreddit_name}", unit="post")
    for s in iter_new_until(sr, before=before, after=after, hard_limit=limit_posts):
        s_dict = {
            "id": s.id,
            "title": s.title,
            "author": s.author,
            "subreddit": s.subreddit,
            "created_utc": int(getattr(s, "created_utc", 0)),
            "selftext": getattr(s, "selftext", None),
            "url": s.url,
            "permalink": s.permalink,
            "num_comments": s.num_comments,
            "score": s.score,
            "over_18": s.over_18,
            "spoiler": getattr(s, "spoiler", False),
            "locked": s.locked,
            "stickied": s.stickied,
        }
        submissions_buf.append(normalize(s_dict, SUB_FIELDS))
        subs_saved += 1

        # Kommentek kibontása (teljes fa)
        if include_comments and s.num_comments:
            s.comments.replace_more(limit=None)
            for c in s.comments.list():
                c_dict = {
                    "id": c.id,
                    "author": c.author,
                    "subreddit": c.subreddit,
                    "created_utc": int(getattr(c, "created_utc", 0)),
                    "body": c.body,
                    "score": c.score,
                    "parent_id": c.parent_id,
                    "link_id": c.link_id,
                    "permalink": c.permalink,
                    "is_submitter": getattr(c, "is_submitter", None),
                }
                comments_buf.append(normalize(c_dict, CMT_FIELDS))
                cmts_saved += 1

        # Időnként flush-olunk, hogy nagy fájl esetén se vesszen el semmi
        if len(submissions_buf) >= 50:
            ndjson_append(sub_path, submissions_buf); submissions_buf.clear()
        if len(comments_buf) >= 200:
            ndjson_append(cmt_path, comments_buf); comments_buf.clear()

        pbar.update(1)
        time.sleep(sleep_s)  # legyünk kíméletesek

    pbar.close()
    if submissions_buf:
        ndjson_append(sub_path, submissions_buf)
    if comments_buf:
        ndjson_append(cmt_path, comments_buf)

    print(f"[✓] Mentve: {subs_saved} submission -> {sub_path}")
    if include_comments:
        print(f"[✓] Mentve: {cmts_saved} comment -> {cmt_path}")

# ========== CLI ==========
def main():
    ap = argparse.ArgumentParser(description="Reddit subreddit downloader (posts + comments, Reddit API/PRAW)")
    ap.add_argument("subreddit", nargs="*", help="pl. RealHungary vagy hikingHungary (több is lehet szóközzel elválasztva)")
    ap.add_argument("--out", default=None, help="kimeneti könyvtár (alapértelmezés: DEFAULT_OUTDIR)")
    ap.add_argument("--after", help="alsó időhatár (epoch vagy ISO pl. 2024-01-01)", default=None)
    ap.add_argument("--before", help="felső időhatár (epoch vagy ISO)", default=None)
    ap.add_argument("--limit", type=int, help="max poszt darabszám (None = amennyit enged)", default=None)
    ap.add_argument("--no-comments", action="store_true", help="kommentek kihagyása")
    ap.add_argument("--sleep", type=float, default=0.5, help="várakozás posztok között (másodperc)")
    ap.add_argument("--auth-test", action="store_true", help="csak az autentikációt teszteli és kilép")

    args = ap.parse_args()
    after = to_epoch(args.after)
    before = to_epoch(args.before)

    # Auth próba (külön kapcsolóval)
    reddit = init_reddit()
    if args.auth_test:
        print("[auth] sikeres smoke test – kilépés (--auth-test)")
        return

    subreddits = args.subreddit if args.subreddit else DEFAULT_SUBREDDITS
    outdir = args.out if args.out else DEFAULT_OUTDIR

    for sr in subreddits:
        download_submissions_and_comments(
            reddit=reddit,
            subreddit_name=sr,
            out_dir=outdir,
            after=after,
            before=before,
            limit_posts=args.limit,
            sleep_s=args.sleep,
            include_comments=(not args.no_comments),
        )

if __name__ == "__main__":
    main()
