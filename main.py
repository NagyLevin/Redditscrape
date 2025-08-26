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
from prawcore import Redirect, NotFound, Forbidden


"""
Default values if the user gives none
- Fallback subreddit list and output directory used by the CLI when no args are provided.
"""

DEFAULT_SUBREDDITS = ["hikingHungary", "RealHungary"]  # can be more
DEFAULT_OUTDIR = "./reddit_dump"                       # base output directory


"""
Fields used
- Exact set of keys we keep from PRAW objects when normalizing to plain dicts.
"""

SUB_FIELDS = [
    "id","title","author","subreddit","created_utc","selftext","url",
    "permalink","num_comments","score","over_18","spoiler","locked","stickied"
]
CMT_FIELDS = [
    "id","author","subreddit","created_utc","body","score",
    "parent_id","link_id","permalink","is_submitter"
]


"""
Helping Functions
- Small utilities for parsing dates, filesystem safety, and (ND)JSON append.
"""



def resolve_subreddit(reddit: praw.Reddit, name: str):

    """
    Checks if the subbreddit exists and also normalizes names
    Return a PRAW Subreddit or None if not accessible/doesn't exist.
    
    """
    name = name.strip()
    if not name:
        return None
    if name.startswith("r/"):
        name = name[2:]
    sr = reddit.subreddit(name)
    try:
        # Force a fetch to validate existence & access
        sr._fetch()
        # Optionally warn about quarantined subs (app-only auth nem fog opt-inelni)
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

"""
loads subbreddit names from file
"""

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
            # keep only simple subreddit token (no spaces)
            line = line.split()[0]
            if line and line not in seen:
                seen.add(line)
                subs.append(line)
    if not subs:
        raise RuntimeError(f"No subreddits found in file: {path}")
    return subs

"""
Checks if date time is valid
- Accepts None, ISO-8601 (with optional time), or an epoch string.
- Returns epoch seconds (UTC) or None.
"""
def to_epoch(dt: Optional[str]) -> Optional[int]:
    """
    dt can be:
      - None
      - '2025-08-01' (UTC 00:00:00)
      - '2025-08-01T14:30:00' (UTC)
      - epoch string (e.g. '1722575400')
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


"""
Checks if directory exists
- Creates target directory recursively if missing.
"""
def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)


"""
Append a list of dicts to an NDJSON file
- Ensures the directory exists; writes one JSON object per line (UTF-8).
"""
def ndjson_append(path: str, items):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


"""
Scan an NDJSON submissions file and return the oldest created_utc
- Useful for simple "resume" behavior (deciding 'after').
"""
def last_submission_ts(path: str) -> Optional[int]:
    """Returns the oldest created_utc found in the file (for simple resume)."""
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


"""
Normalize PRAW objects to plain dicts using a whitelist of fields
- Converts complex types (author, subreddit) to simple strings where needed.
"""
def normalize(obj: dict, fields: list[str]) -> dict:
    out = {}
    for k in fields:
        v = obj.get(k)
        if k == "author" and v is not None and not isinstance(v, str):
            v = getattr(v, "name", None)
        if k == "subreddit" and v is not None and not isinstance(v, str):
            # PRAW Subreddit object -> display_name
            v = str(getattr(v, "display_name", v))
        out[k] = v
    return out


# ===== TXT output helpers =====

"""
Fallback author rendering
- Reddit returns None for deleted users so i made delted as a placeholder becase that stands in the app
"""
def _fallback_author(a):
    return a if a else "[deleted]"

"""
Sanitize and indent multiline text blocks for nice TXT formatting
- Keeps first line; subsequent lines are indented for readability.
"""
def _safe_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    lines = s.split("\n")
    # keep first line, indent the rest
    return ("\n      ").join(lines)

"""
Write a single post + its comments as a readable TXT block
- Simple, human-friendly structure for later reading/grepping.
"""
def txt_write_post_block(f, post: dict, comments: list[dict]):
    # Post header
    author = _fallback_author(post.get("author"))
    title  = post.get("title") or ""
    f.write("Post:\n")
    f.write(f"by {author}: {title}\n")
    # Optional selftext
    body = _safe_text(post.get("selftext"))
    if body:
        f.write("  body:\n")
        f.write(f"    {body}\n")
    # Comments
    for c in comments:
        cauthor = _fallback_author(c.get("author"))
        cbody   = _safe_text(c.get("body"))
        f.write("  comment:\n")
        f.write(f"    {cauthor}: {cbody}\n")
    f.write("\n")


"""
Initialize a PRAW Reddit client with auth
- Loads credentials from .env; performs a minimal "smoke test" to ensure read scope.
- Prints helpful diagnostics and raises a clear error if both methods fail.
"""
def init_reddit() -> praw.Reddit:
    load_dotenv()
    cid  = os.getenv("REDDIT_CLIENT_ID","").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET","").strip()
    ua   = os.getenv("REDDIT_USER_AGENT","").strip()
    user = os.getenv("REDDIT_USERNAME","").strip()
    pwd  = os.getenv("REDDIT_PASSWORD","").strip()

    if not ua:
        raise RuntimeError("Missing REDDIT_USER_AGENT.")

    def smoke_test(r):
        # minimal read scope probe
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

   
    # --- Detailed error message with hints ---
    msg = [
        "Authentication error"
    ]
    raise RuntimeError("\n".join(msg))


# ========== Download ==========

"""
Iterate the 'new' feed backwards with time-window and hard-limit controls
"""
def iter_new_until(subreddit, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable[Submission]:
    """
    We read the 'new' feed in descending order. Stop when created_utc < after or limit reached.
    """
    count = 0
    for s in subreddit.new(limit=None):
        cu = int(getattr(s, "created_utc", 0))
        if before is not None and cu > before:
            # too fresh -> continue (feed is descending)
            continue
        if after is not None and cu < after:
            break
        yield s
        count += 1
        if hard_limit and count >= hard_limit:
            break


"""
Download submissions + optional comments for one subreddit
- Supports NDJSON output (submissions/comments) or a single human-readable TXT file ("plain" mode).
- Applies time window (after/before), post count limit, and polite sleep between requests.
- Buffers NDJSON writes for efficiency; 
- writes TXT progressively in plain mode.
"""
def download_submissions_and_comments(
    reddit: praw.Reddit,
    subreddit_name: str,
    out_dir: str,
    after: Optional[int],
    before: Optional[int],
    limit_posts: Optional[int],
    sleep_s: float = 0.5,
    include_comments: bool = True,
    plain: bool = False,  # new param: enable TXT mode instead of json
):
    sr = reddit.subreddit(subreddit_name)
    
    if sr is None:
        return

    ensure_dir(out_dir)
    sub_path = os.path.join(out_dir, f"{subreddit_name}.submissions.ndjson")
    cmt_path = os.path.join(out_dir, f"{subreddit_name}.comments.ndjson")
    txt_path = os.path.join(out_dir, f"{subreddit_name}.txt")  # target for plain mode

    
    if after is None:
        _existing_oldest = last_submission_ts(sub_path)
        # If desired, you could set after = _existing_oldest here to strictly continue.

    subs_saved = 0
    cmts_saved = 0

    submissions_buf = []
    comments_buf = []

    # Plain mode: open TXT once and append each post immediately
    txt_file = open(txt_path, "a", encoding="utf-8") if plain else None
    if txt_file and os.path.getsize(txt_path) == 0:
        txt_file.write(f"=== r/{subreddit_name} ===\n\n")

    try:
        pbar = tqdm(desc=f"Submissions r/{subreddit_name}", unit="post")
        for s in iter_new_until(sr, before=before, after=after, hard_limit=limit_posts):
            # Build submission dict + normalize
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

            # Expand and collect comments (if requested)
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
                # NDJSON path (buffer + flush)
                submissions_buf.append(s_norm)
                subs_saved += 1
                if current_comments:
                    comments_buf.extend(current_comments)
                    cmts_saved += len(current_comments)

                # Periodic flush for efficiency
                if len(submissions_buf) >= 50:
                    ndjson_append(sub_path, submissions_buf); submissions_buf.clear()
                if len(comments_buf) >= 200:
                    ndjson_append(cmt_path, comments_buf); comments_buf.clear()

            pbar.update(1)
            time.sleep(sleep_s)  # be gentle to the API because you can get timeout if you spam

        pbar.close()

      
        if not plain:
            if submissions_buf:
                ndjson_append(sub_path, submissions_buf)
            if comments_buf:
                ndjson_append(cmt_path, comments_buf)

    except (Redirect, NotFound, Forbidden) as e:
        print(f" skipping because we subbredit is private or no longer exists {e.__class__.__name__} downloaded:")
        return
    
    finally:
        if txt_file:
            txt_file.flush()
            txt_file.close()

    print(f"[✓] Saved: {subs_saved} posts  -> {'{}/{}.txt'.format(out_dir, subreddit_name) if plain else sub_path}")
    if include_comments:
        print(f"[✓] Saved: {cmts_saved} comments -> {'(embedded in TXT)' if plain else cmt_path}")



"""
Command-line entry point
downloads one or more subreddits using the selected mode and filters.
"""
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
    after = to_epoch(args.after)
    before = to_epoch(args.before)

    # Auth check (optional flag)
    reddit = init_reddit()
    if args.auth_test:
        print("[auth] smoke test successful – exiting (--auth-test)")
        return

    #If there is an imput file we will read the subreddit names from there.

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
        download_submissions_and_comments(
            reddit=reddit,
            subreddit_name=sr,
            out_dir=outdir,
            after=after,
            before=before,
            limit_posts=args.limit,
            sleep_s=args.sleep,
            include_comments=(not args.no_comments),
            plain=args.plain,  # pass through TXT mode
        )


if __name__ == "__main__":
    main()
