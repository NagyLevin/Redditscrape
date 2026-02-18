import os
import time
import argparse
import pathlib
import re
import shutil
from datetime import datetime, timezone
from typing import Optional, Iterable

import praw
from praw.models import Submission
from dotenv import load_dotenv
from tqdm import tqdm
from prawcore import Redirect, NotFound, Forbidden

# ---- Defaults ----
DEFAULT_SUBREDDITS = ["hikingHungary", "RealHungary"]
DEFAULT_OUTDIR = "/home/szabol/SavedFromReddit_3"

VISITED_FILE = pathlib.Path("./visited.txt")
TIMEOUTS_FILE = pathlib.Path("./timeouts.txt")

# TXT header format:
# === r/<subreddit> === visited: YYYY.MM.DD
HDR_RE = re.compile(
    r"^=== r/(?P<name>[^ ]+) ===(?: visited: (?P<date>\d{4}\.\d{2}\.\d{2}))?\s*$"
)

# ---- visited.txt / timeouts.txt (one-shot mode) ----
def add_to_visited(name: str) -> None:
    VISITED_FILE.touch(exist_ok=True)
    cur = set(x.strip() for x in VISITED_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    if name not in cur:
        with VISITED_FILE.open("a", encoding="utf-8") as f:
            f.write(name + "\n")

def is_visited(name: str) -> bool:
    VISITED_FILE.touch(exist_ok=True)
    return name in {x.strip() for x in VISITED_FILE.read_text(encoding="utf-8").splitlines() if x.strip()}

def add_to_timeouts(name: str) -> None:
    TIMEOUTS_FILE.touch(exist_ok=True)
    cur = set(x.strip() for x in TIMEOUTS_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    if name not in cur:
        with TIMEOUTS_FILE.open("a", encoding="utf-8") as f:
            f.write(name + "\n")

# ---- Utils ----
def ensure_dir(p: str) -> None:
    if p:
        os.makedirs(p, exist_ok=True)

def to_epoch(dt: Optional[str]) -> Optional[int]:
    """
    Accepts:
      - None
      - '2026-02-18' (UTC 00:00:00)
      - '2026-02-18T14:30:00' (UTC)
      - epoch string (e.g. '1708214400')
    Returns epoch seconds (UTC) or None.
    """
    if dt is None:
        return None
    try:
        return int(float(dt))
    except ValueError:
        pass

    if "T" in dt:
        return int(datetime.fromisoformat(dt).replace(tzinfo=timezone.utc).timestamp())

    return int(datetime.fromisoformat(dt + "T00:00:00").replace(tzinfo=timezone.utc).timestamp())

def today_str_yyyy_mm_dd() -> str:
    # csak dátum kell
    return datetime.now(timezone.utc).strftime("%Y.%m.%d")

def epoch_to_date_str(epoch_s: int) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y.%m.%d")

# ---- Reddit ----
def init_reddit() -> praw.Reddit:
    load_dotenv()
    cid  = os.getenv("REDDIT_CLIENT_ID","").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET","").strip()
    ua   = os.getenv("REDDIT_USER_AGENT","").strip()

    if not ua:
        raise RuntimeError("Missing REDDIT_USER_AGENT.")

    def smoke_test(r):
        next(iter(r.subreddit("popular").hot(limit=1)))

    if cid and csec:
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

    raise RuntimeError("Authentication error")

def resolve_subreddit(reddit: praw.Reddit, name: str):
    name = name.strip()
    if not name:
        return None
    if name.startswith("r/"):
        name = name[2:]
    sr = reddit.subreddit(name)
    try:
        sr._fetch()
        if getattr(sr, "quarantine", False):
            print(f"[skip] r/{name} is quarantined (requires opt-in).")
            return None
        return sr
    except Redirect:
        print(f"[skip] r/{name} not found (redirect).")
    except NotFound:
        print(f"[skip] r/{name} not found/banned.")
    except Forbidden:
        print(f"[skip] r/{name} is private/quarantined (403).")
    except Exception as e:
        print(f"[skip] r/{name} unknown error: {e!r}")
    return None

def load_subreddits_from_file(path: str) -> list[str]:
    seen = set()
    subs: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("r/"):
                line = line[2:]
            line = line.split()[0]
            if line and line not in seen:
                seen.add(line)
                subs.append(line)
    if not subs:
        raise RuntimeError(f"No subreddits found in file: {path}")
    return subs

# ---- TXT formatting helpers (same structure as your example) ----
def _fallback_author(a) -> str:
    return a if a else "[deleted]"

def _safe_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    lines = s.split("\n")
    return ("\n      ").join(lines)

def txt_write_post_block(f, post_author: str, title: str, selftext: str, comments: list[tuple[str,str]]):
    f.write("Post:\n")
    f.write(f"by {post_author}: {title}\n")

    body = _safe_text(selftext)
    if body:
        f.write("  body:\n")
        f.write(f"    {body}\n")

    for (cauthor, cbody) in comments:
        f.write("  comment:\n")
        f.write(f"    {cauthor}: {_safe_text(cbody)}\n")

    f.write("\n")

# ---- Header visited handling ----
def read_txt_visited_date(txt_path: str, subreddit_name: str) -> Optional[str]:
    if not os.path.exists(txt_path):
        return None
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            first = f.readline().rstrip("\n")
        m = HDR_RE.match(first)
        if not m:
            return None
        if (m.group("name") or "").lower() != subreddit_name.lower():
            return None
        return m.group("date")  # can be None
    except Exception:
        return None

def stamp_txt_header_visited(txt_path: str, subreddit_name: str, visited_date: str) -> None:
    """
    Ensures the first line is:
      === r/<subreddit> === visited: YYYY.MM.DD
    If existing first line is a header for the same subreddit (with or without visited), it gets replaced.
    Otherwise the header is inserted at the top.
    Streaming rewrite (no full memory load).
    """
    ensure_dir(os.path.dirname(txt_path) or ".")
    new_header_line = f"=== r/{subreddit_name} === visited: {visited_date}\n"

    if not os.path.exists(txt_path):
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(new_header_line)
            f.write("\n")
        return

    tmp = txt_path + ".tmp"
    with open(txt_path, "r", encoding="utf-8") as fin, open(tmp, "w", encoding="utf-8") as fout:
        first = fin.readline()
        first_stripped = first.rstrip("\n")
        m = HDR_RE.match(first_stripped)

        if m and (m.group("name") or "").lower() == subreddit_name.lower():
            fout.write(new_header_line)
            # a régi header sorát eldobjuk, a többit visszaírjuk
        else:
            fout.write(new_header_line)
            fout.write("\n")
            fout.write(first)

        shutil.copyfileobj(fin, fout)

    os.replace(tmp, txt_path)

# ---- Iteration ----
def iter_new_until(subreddit, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable[Submission]:
    """
    Reads 'new' feed in descending order.
    Stops when created_utc < after.
    Includes submissions with created_utc >= after.
    """
    count = 0
    for s in subreddit.new(limit=None):
        cu = int(getattr(s, "created_utc", 0))
        if before is not None and cu > before:
            continue
        if after is not None and cu < after:
            break
        yield s
        count += 1
        if hard_limit and count >= hard_limit:
            break

# ---- Main downloader (TXT ONLY) ----
def download_subreddit_txt(
    reddit: praw.Reddit,
    subreddit_name: str,
    out_dir: str,
    after: Optional[int],
    before: Optional[int],
    limit_posts: Optional[int],
    sleep_s: float,
    include_comments: bool,
    append_mode: bool,
    visited_stamp: str,
):
    sr = resolve_subreddit(reddit, subreddit_name)
    if sr is None:
        return

    ensure_dir(out_dir)
    txt_path = os.path.join(out_dir, f"{subreddit_name}.txt")

    # If we are NOT appending -> write into .part then replace (so failed download won't leave a "visited" file)
    use_part = (not append_mode) or (append_mode and not os.path.exists(txt_path))
    target_path = (txt_path + ".part") if use_part else txt_path

    subs_saved = 0
    cmts_saved = 0

    # Open file
    f = open(target_path, "a" if (append_mode and not use_part) else "w", encoding="utf-8")

    try:
        # When creating a new file (part file), write a basic header line (no visited yet)
        if use_part:
            f.write(f"=== r/{subreddit_name} ===\n\n")

        pbar = tqdm(desc=f"Submissions r/{subreddit_name}", unit="post")

        for s in iter_new_until(sr, before=before, after=after, hard_limit=limit_posts):
            post_author = _fallback_author(getattr(s.author, "name", None) if s.author else None)
            title = s.title or ""
            selftext = getattr(s, "selftext", None) or ""

            comments_out: list[tuple[str,str]] = []
            if include_comments and getattr(s, "num_comments", 0):
                s.comments.replace_more(limit=None)
                for c in s.comments.list():
                    cauthor = _fallback_author(getattr(c.author, "name", None) if c.author else None)
                    cbody = getattr(c, "body", "") or ""
                    comments_out.append((cauthor, cbody))

            txt_write_post_block(f, post_author, title, selftext, comments_out)

            subs_saved += 1
            cmts_saved += len(comments_out)

            pbar.update(1)
            time.sleep(sleep_s)

        pbar.close()

        f.flush()
        f.close()

        # Stamp header visited (on the file we actually wrote)
        stamp_txt_header_visited(target_path, subreddit_name, visited_stamp)

        # If .part was used, replace final
        if use_part:
            os.replace(target_path, txt_path)

    except (Redirect, NotFound, Forbidden) as e:
        try:
            f.close()
        except Exception:
            pass
        if use_part:
            try:
                os.remove(target_path)
            except Exception:
                pass
        print(f"[skip] r/{subreddit_name} access error: {e.__class__.__name__}")
        return

    except Exception:
        try:
            f.close()
        except Exception:
            pass
        if use_part:
            try:
                os.remove(target_path)
            except Exception:
                pass
        raise

    print(f"[✓] Saved: {subs_saved} posts -> {txt_path}")
    if include_comments:
        print(f"[✓] Saved: {cmts_saved} comments -> (embedded in TXT)")

# ---- CLI ----
def main():
    ap = argparse.ArgumentParser(description="Reddit subreddit downloader (TXT: posts + comments)")
    ap.add_argument("subreddit", nargs="*", help="e.g. RealHungary (multiple allowed)")
    ap.add_argument("--out", default=None, help=f"output directory (default: {DEFAULT_OUTDIR})")
    ap.add_argument("--after", help="lower time bound (epoch or ISO e.g., 2024-01-01)", default=None)
    ap.add_argument("--before", help="upper time bound (epoch or ISO)", default=None)
    ap.add_argument("--limit", type=int, help="max number of posts", default=None)
    ap.add_argument("--no-comments", action="store_true", help="skip comments")
    ap.add_argument("--sleep", type=float, default=0.5, help="sleep between posts (seconds)")
    ap.add_argument("--auth-test", action="store_true", help="only test authentication and exit")
    ap.add_argument("--inputfile", help="path to a text file listing subreddits (one per line)", default=None)

    args = ap.parse_args()

    after_epoch = to_epoch(args.after)
    before_epoch = to_epoch(args.before)

    reddit = init_reddit()
    if args.auth_test:
        print("[auth] smoke test successful – exiting (--auth-test)")
        return

    # subreddit list
    if args.inputfile:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        inpath = args.inputfile
        if not os.path.isabs(inpath):
            inpath = os.path.join(base_dir, inpath)
        subreddits = load_subreddits_from_file(inpath)
        print(f"[info] Loaded {len(subreddits)} subreddits from {inpath}")
    else:
        subreddits = args.subreddit if args.subreddit else DEFAULT_SUBREDDITS

    outdir = args.out if args.out else DEFAULT_OUTDIR
    include_comments = (not args.no_comments)

    # Update mode if --after is used
    update_mode = (args.after is not None)

    # visited stamp:
    # - update mode: stamp = day-of --after in YYYY.MM.DD (so equality check works)
    # - normal mode: stamp = today's date
    visited_stamp = epoch_to_date_str(after_epoch) if (update_mode and after_epoch is not None) else today_str_yyyy_mm_dd()

    for sr_name in subreddits:
        txt_path = os.path.join(outdir, f"{sr_name}.txt")

        if update_mode:
            prev = read_txt_visited_date(txt_path, sr_name)

            if prev == visited_stamp:
                print(f"[up-to-date] r/{sr_name} visited: {prev} == --after ({visited_stamp})")
                continue
            if prev is not None and prev > visited_stamp:
                print(f"[up-to-date] r/{sr_name} visited: {prev} newer than --after ({visited_stamp})")
                continue

            # append new posts since --after
            try:
                download_subreddit_txt(
                    reddit=reddit,
                    subreddit_name=sr_name,
                    out_dir=outdir,
                    after=after_epoch,
                    before=before_epoch,
                    limit_posts=args.limit,
                    sleep_s=args.sleep,
                    include_comments=include_comments,
                    append_mode=True,
                    visited_stamp=visited_stamp,
                )
            except Exception as e:
                print(f"[ABORT FILE] {sr_name} due to failure: {e}")
                add_to_timeouts(sr_name)
                continue

        else:
            # one-shot behavior (like before) using visited.txt
            if is_visited(sr_name):
                print(f"Already processed {sr_name}")
                continue

            try:
                download_subreddit_txt(
                    reddit=reddit,
                    subreddit_name=sr_name,
                    out_dir=outdir,
                    after=None,
                    before=before_epoch,
                    limit_posts=args.limit,
                    sleep_s=args.sleep,
                    include_comments=include_comments,
                    append_mode=False,          # overwrite (new full download)
                    visited_stamp=visited_stamp # today
                )
                add_to_visited(sr_name)
            except Exception as e:
                print(f"[ABORT FILE] {sr_name} due to failure: {e}")
                add_to_timeouts(sr_name)
                continue

if __name__ == "__main__":
    main()
