import os
import time
import json
import argparse
import re
import pathlib
from datetime import datetime, timezone
from typing import Optional, Iterable

import praw
from praw.models import Submission
from dotenv import load_dotenv
from tqdm import tqdm
from prawcore import Redirect, NotFound, Forbidden

"""
Default values if the user gives none
- Fallback subreddit list and output directory used by the CLI when no args are provided.
"""

DEFAULT_SUBREDDITS = ["hikingHungary", "RealHungary"]  # add more here if you want
DEFAULT_OUTDIR = "/home/szabol/SavedFromReddit_2"      # base output directory

VISITED_FILE   = pathlib.Path("./visited.txt")   # one-shot mode: names of processed subreddits
TIMEOUTS_FILE  = pathlib.Path("./timeouts.txt")  # names of aborted files (relative keys)

"""
Fields used
- Exact set of keys we keep from PRAW objects when normalizing to plain dicts.
"""

SUB_FIELDS = [
    "id", "title", "author", "subreddit", "created_utc", "selftext", "url",
    "permalink", "num_comments", "score", "over_18", "spoiler", "locked", "stickied"
]
CMT_FIELDS = [
    "id", "author", "subreddit", "created_utc", "body", "score",
    "parent_id", "link_id", "permalink", "is_submitter"
]

# TXT header format:
# === r/<subreddit> === visited: YYYY.MM.DD
HDR_RE = re.compile(
    r"^=== r/(?P<name>[^ ]+) ===(?: visited: (?P<date>\d{4}\.\d{2}\.\d{2}))?\s*$"
)


# ---------------------------
# visited.txt / timeouts.txt
# ---------------------------

def add_to_visited(name: str) -> None:
    VISITED_FILE.touch(exist_ok=True)
    cur = set(x.strip() for x in VISITED_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    if name not in cur:
        with VISITED_FILE.open("a", encoding="utf-8") as f:
            f.write(name + "\n")


def is_visited(name: str) -> bool:
    """
    Checks if the subreddit name is in the visited section (one-shot mode).
    """
    VISITED_FILE.touch(exist_ok=True)
    return name in {x.strip() for x in VISITED_FILE.read_text(encoding="utf-8").splitlines() if x.strip()}


def add_to_timeouts(name: str) -> None:
    """
    If the file processing is halted, its name will get added to timeouts
    """
    TIMEOUTS_FILE.touch(exist_ok=True)
    cur = set(x.strip() for x in TIMEOUTS_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    if name not in cur:
        with TIMEOUTS_FILE.open("a", encoding="utf-8") as f:
            f.write(name + "\n")


# ---------------------------
# Small utils
# ---------------------------

def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)


def to_epoch(dt: Optional[str]) -> Optional[int]:
    """
    dt can be:
      - None
      - '2025-08-01' (UTC 00:00:00)
      - '2025-08-01T14:30:00' (UTC)
      - epoch string (e.g. '1722575400')
    Returns epoch seconds (UTC) or None.
    """
    if dt is None:
        return None
    try:
        return int(float(dt))  # already epoch
    except ValueError:
        pass
    if "T" in dt:
        return int(datetime.fromisoformat(dt).replace(tzinfo=timezone.utc).timestamp())
    return int(datetime.fromisoformat(dt + "T00:00:00").replace(tzinfo=timezone.utc).timestamp())


def epoch_to_visited_date_str(epoch_s: int) -> str:
    """
    Convert epoch seconds to 'YYYY.MM.DD' in UTC.
    """
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y.%m.%d")


def now_utc_visited_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y.%m.%d")


def truncate_file(path: str) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8"):
        pass


# ---------------------------
# NDJSON write (append)
# ---------------------------

def ndjson_append(path: str, items) -> None:
    """
    Append a list of dicts to an NDJSON file (one JSON per line).
    """
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


# ---------------------------
# TXT header (visited stamp)
# ---------------------------

def read_txt_visited_date(txt_path: str, subreddit_name: str) -> Optional[str]:
    """
    Returns 'YYYY.MM.DD' from the first line if it matches header format for this subreddit, else None.
    """
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
        return m.group("date")
    except Exception:
        return None


def set_txt_header_visited(txt_path: str, subreddit_name: str, visited_date: str) -> None:
    """
    Ensure first line is:
      === r/<subreddit_name> === visited: YYYY.MM.DD
    If header exists -> replace/update.
    If not -> insert header line at top (preserving existing content).
    Tries in-place update if possible (same byte length); otherwise rewrites with a temp file.
    """
    ensure_dir(os.path.dirname(txt_path) or ".")
    new_header = f"=== r/{subreddit_name} === visited: {visited_date}"
    new_header_line = new_header + "\n"
    new_bytes = new_header_line.encode("utf-8")

    if not os.path.exists(txt_path):
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(new_header_line + "\n")
        return

    # Try in-place update (fast) if the first line already is a header and same length
    try:
        with open(txt_path, "r+b") as fb:
            first_bytes = fb.readline()  # includes newline
            try:
                first_line = first_bytes.decode("utf-8").rstrip("\n")
            except Exception:
                first_line = ""

            m = HDR_RE.match(first_line)
            if m and (m.group("name") or "").lower() == subreddit_name.lower():
                if len(first_bytes) == len(new_bytes):
                    fb.seek(0)
                    fb.write(new_bytes)
                    return
    except Exception:
        pass

    # Fallback: rewrite via temp file
    tmp = txt_path + ".tmp"
    with open(txt_path, "r", encoding="utf-8") as fin, open(tmp, "w", encoding="utf-8") as fout:
        first = fin.readline()
        rest = fin.read()

        first_stripped = first.rstrip("\n")
        m = HDR_RE.match(first_stripped)
        if m and (m.group("name") or "").lower() == subreddit_name.lower():
            # Replace existing header
            fout.write(new_header_line)
            fout.write(rest)
        else:
            # Insert header above existing content
            fout.write(new_header_line)
            fout.write(first)
            fout.write(rest)

    os.replace(tmp, txt_path)


# ---------------------------
# Meta for NDJSON mode
# ---------------------------

def meta_path(out_dir: str, subreddit_name: str) -> str:
    return os.path.join(out_dir, f"{subreddit_name}.meta.json")


def read_meta_visited_date(meta_file: str) -> Optional[str]:
    if not os.path.exists(meta_file):
        return None
    try:
        with open(meta_file, "r", encoding="utf-8") as f:
            d = json.load(f)
        v = d.get("visited")
        return v if isinstance(v, str) else None
    except Exception:
        return None


def write_meta(meta_file: str, subreddit_name: str, visited_date: str) -> None:
    ensure_dir(os.path.dirname(meta_file) or ".")
    payload = {
        "subreddit": subreddit_name,
        "visited": visited_date,
        "updated_utc": int(datetime.now(timezone.utc).timestamp()),
    }
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------
# Subreddit resolve
# ---------------------------

def resolve_subreddit(reddit: praw.Reddit, name: str):
    """
    Checks if the subreddit exists and also normalizes names.
    Return a PRAW Subreddit or None if not accessible/doesn't exist.
    """
    name = name.strip()
    if not name:
        return None
    if name.startswith("r/"):
        name = name[2:]
    sr = reddit.subreddit(name)
    try:
        sr._fetch()  # force fetch
        if getattr(sr, "quarantine", False):
            print(f"[skip] r/{name} is quarantined (requires opt-in, skip with app-only auth).")
            return None
        return sr
    except Redirect:
        print(f"[skip] r/{name} not found (redirected to search).")
    except NotFound:
        print(f"[skip] r/{name} not found/banned.")
    except Forbidden:
        print(f"[skip] r/{name} is private or quarantined (403).")
    except Exception as e:
        print(f"[skip] r/{name} unknown error: {e!r}")
    return None


# ---------------------------
# Load subreddits from file
# ---------------------------

def load_subreddits_from_file(path: str) -> list[str]:
    """
    Read subreddits from a text file, one per line.
    - Ignores empty lines and lines starting with '#'
    - Strips whitespace
    - Accepts optional leading 'r/' and removes it
    - De-duplicates while preserving order
    """
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


# ---------------------------
# Normalize PRAW objects
# ---------------------------

def normalize(obj: dict, fields: list[str]) -> dict:
    out = {}
    for k in fields:
        v = obj.get(k)
        if k == "author" and v is not None and not isinstance(v, str):
            v = getattr(v, "name", None)
        if k == "subreddit" and v is not None and not isinstance(v, str):
            v = str(getattr(v, "display_name", v))
        out[k] = v
    return out


# ---------------------------
# TXT output helpers
# ---------------------------

def _fallback_author(a):
    return a if a else "[deleted]"


def _safe_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    lines = s.split("\n")
    return ("\n      ").join(lines)


def txt_write_post_block(f, post: dict, comments: list[dict]):
    author = _fallback_author(post.get("author"))
    title = post.get("title") or ""
    f.write("Post:\n")
    f.write(f"by {author}: {title}\n")

    body = _safe_text(post.get("selftext"))
    if body:
        f.write("  body:\n")
        f.write(f"    {body}\n")

    for c in comments:
        cauthor = _fallback_author(c.get("author"))
        cbody = _safe_text(c.get("body"))
        f.write("  comment:\n")
        f.write(f"    {cauthor}: {cbody}\n")

    f.write("\n")


# ---------------------------
# Reddit init
# ---------------------------

def init_reddit() -> praw.Reddit:
    load_dotenv()
    cid = os.getenv("REDDIT_CLIENT_ID", "").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
    ua = os.getenv("REDDIT_USER_AGENT", "").strip()

    if not ua:
        raise RuntimeError("Missing REDDIT_USER_AGENT.")

    def smoke_test(r):
        next(iter(r.subreddit("popular").hot(limit=1)))

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

    raise RuntimeError("Authentication error")


# ---------------------------
# Iteration helper
# ---------------------------

def iter_new_until(subreddit, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable[Submission]:
    """
    We read the 'new' feed in descending order. Stop when created_utc < after or limit reached.
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


# ---------------------------
# Main download
# ---------------------------

def download_submissions_and_comments(
    reddit: praw.Reddit,
    subreddit_name: str,
    out_dir: str,
    after: Optional[int],
    before: Optional[int],
    limit_posts: Optional[int],
    sleep_s: float = 0.5,
    include_comments: bool = True,
    plain: bool = False,
    incremental: bool = False,
    visited_tag: Optional[str] = None,  # e.g. '2026.02.18'
):
    """
    - plain=True  -> one TXT file per subreddit (posts + comments embedded)
    - plain=False -> NDJSON submissions + NDJSON comments

    incremental=True is meant for --after usage:
    - append to existing files (do NOT truncate)
    - visited_tag is used to stamp "visited: YYYY.MM.DD"
    """

    sr = resolve_subreddit(reddit, subreddit_name)
    if sr is None:
        return

    ensure_dir(out_dir)

    sub_path = os.path.join(out_dir, f"{subreddit_name}.submissions.ndjson")
    cmt_path = os.path.join(out_dir, f"{subreddit_name}.comments.ndjson")
    txt_path = os.path.join(out_dir, f"{subreddit_name}.txt")
    meta_file = meta_path(out_dir, subreddit_name)

    # Snapshot mode: start from clean files for NDJSON
    if not plain and not incremental:
        truncate_file(sub_path)
        if include_comments:
            truncate_file(cmt_path)

    subs_saved = 0
    cmts_saved = 0
    submissions_buf = []
    comments_buf = []

    txt_file = None
    new_txt_created = False

    # Decide file mode for TXT
    if plain:
        if incremental:
            if not os.path.exists(txt_path):
                # new file -> create it, write content first; header will be inserted/stamped at end if needed
                txt_file = open(txt_path, "w", encoding="utf-8")
                new_txt_created = True
            else:
                txt_file = open(txt_path, "a", encoding="utf-8")
        else:
            # snapshot: overwrite
            txt_file = open(txt_path, "w", encoding="utf-8")
            new_txt_created = True
            # write header immediately in snapshot mode
            stamp = visited_tag or now_utc_visited_date_str()
            txt_file.write(f"=== r/{subreddit_name} === visited: {stamp}\n\n")

    try:
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
            s_norm = normalize(s_dict, SUB_FIELDS)

            current_comments = []
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
                    current_comments.append(normalize(c_dict, CMT_FIELDS))

            if plain:
                # Write immediately to TXT
                txt_write_post_block(txt_file, s_norm, current_comments)
                subs_saved += 1
                cmts_saved += len(current_comments)
            else:
                # NDJSON: buffer + flush append
                submissions_buf.append(s_norm)
                subs_saved += 1

                if current_comments:
                    comments_buf.extend(current_comments)
                    cmts_saved += len(current_comments)

                if len(submissions_buf) >= 50:
                    ndjson_append(sub_path, submissions_buf)
                    submissions_buf.clear()

                if include_comments and len(comments_buf) >= 200:
                    ndjson_append(cmt_path, comments_buf)
                    comments_buf.clear()

            pbar.update(1)
            time.sleep(sleep_s)

        pbar.close()

        # Flush remaining buffers
        if not plain:
            if submissions_buf:
                ndjson_append(sub_path, submissions_buf)
            if include_comments and comments_buf:
                ndjson_append(cmt_path, comments_buf)

        # After SUCCESS: stamp visited
        if visited_tag:
            if plain:
                # If snapshot mode header already written, this just updates the date.
                # If incremental and header missing, this will insert it at top.
                set_txt_header_visited(txt_path, subreddit_name, visited_tag)
            else:
                write_meta(meta_file, subreddit_name, visited_tag)

    except (Redirect, NotFound, Forbidden) as e:
        print(f" skipping because subreddit is private or no longer exists ({e.__class__.__name__})")
        # If we created a brand-new TXT in incremental and failed, remove it to avoid false "visited"
        if plain and incremental and new_txt_created:
            try:
                txt_file.close()
            except Exception:
                pass
            try:
                os.remove(txt_path)
            except Exception:
                pass
        return

    except Exception:
        # If we created a brand-new TXT in incremental and failed, remove it to avoid false "visited"
        if plain and incremental and new_txt_created:
            try:
                if txt_file:
                    txt_file.close()
            except Exception:
                pass
            try:
                os.remove(txt_path)
            except Exception:
                pass
        raise

    finally:
        if txt_file:
            try:
                txt_file.flush()
                txt_file.close()
            except Exception:
                pass

    if plain:
        print(f"[✓] Saved: {subs_saved} posts  -> {txt_path}")
        if include_comments:
            print(f"[✓] Saved: {cmts_saved} comments -> (embedded in TXT)")
    else:
        print(f"[✓] Saved: {subs_saved} posts  -> {sub_path}")
        if include_comments:
            print(f"[✓] Saved: {cmts_saved} comments -> {cmt_path}")


# ---------------------------
# CLI main
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Reddit subreddit downloader (posts + comments, Reddit API/PRAW)")
    ap.add_argument("subreddit", nargs="*", help="e.g. RealHungary or hikingHungary (you can pass multiple separated by space)")
    ap.add_argument("--out", default=None, help="output directory (default: DEFAULT_OUTDIR)")
    ap.add_argument("--after", help="lower time bound (epoch or ISO e.g., 2024-01-01)", default=None)
    ap.add_argument("--before", help="upper time bound (epoch or ISO)", default=None)
    ap.add_argument("--limit", type=int, help="max number of posts (None = as many as allowed)", default=None)
    ap.add_argument("--no-comments", action="store_true", help="skip comments")
    ap.add_argument("--sleep", type=float, default=0.5, help="sleep between posts (seconds)")
    ap.add_argument("--auth-test", action="store_true", help="only test authentication and exit")
    ap.add_argument("--plain", action="store_true", help="TXT output (one big file per subreddit)")
    ap.add_argument("--inputfile", help="path to a text file listing subreddits (one per line)", default=None)

    args = ap.parse_args()

    after_epoch = to_epoch(args.after)
    before_epoch = to_epoch(args.before)

    # incremental mode: if --after is used, we append + visited stamping + up-to-date check
    incremental = (args.after is not None)

    # visited stamp: in incremental mode the user-provided after date is the "target" visited date (YYYY.MM.DD)
    # in snapshot mode we stamp with today's UTC date
    if incremental:
        visited_tag = epoch_to_visited_date_str(after_epoch) if after_epoch is not None else now_utc_visited_date_str()
    else:
        visited_tag = now_utc_visited_date_str()

    reddit = init_reddit()

    if args.auth_test:
        print("[auth] smoke test successful – exiting (--auth-test)")
        return

    # Load subreddits
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

    for sr in subreddits:
        # one-shot mode (no --after): keep old visited.txt behavior
        if not incremental:
            if is_visited(sr):
                print(f"Already processed {sr}")
                continue

        # incremental mode: check "visited" inside the subreddit output
        if incremental:
            if args.plain:
                txt_path = os.path.join(outdir, f"{sr}.txt")
                prev = read_txt_visited_date(txt_path, sr)
            else:
                prev = read_meta_visited_date(meta_path(outdir, sr))

            if prev == visited_tag:
                print(f"[up-to-date] r/{sr} visited: {prev} == --after ({visited_tag})")
                continue
            if prev is not None and prev > visited_tag:
                print(f"[skip] r/{sr} has visited: {prev} which is newer than --after ({visited_tag})")
                continue

        try:
            download_submissions_and_comments(
                reddit=reddit,
                subreddit_name=sr,
                out_dir=outdir,
                after=after_epoch,
                before=before_epoch,
                limit_posts=args.limit,
                sleep_s=args.sleep,
                include_comments=(not args.no_comments),
                plain=args.plain,
                incremental=incremental,
                visited_tag=visited_tag,
            )

            if not incremental:
                add_to_visited(sr)

        except Exception as e:
            print(f"[ABORT FILE] {sr} due to failure: {e}")
            add_to_timeouts(sr)
            continue


if __name__ == "__main__":
    main()
