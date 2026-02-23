import argparse
import os
import re
from datetime import date


# Példa sor:
# === r/szekesfehervar === visited: 2025.08.28
PATTERN = re.compile(r"^(===\s*r\/[^\s]+\s*===\s*visited:\s*)(\d{4}\.\d{2}\.\d{2})(\s*)$")


def split_line_ending(line: str):
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    if line.endswith("\r"):
        return line[:-1], "\r"
    return line, ""


def process_file(path: str, today_str: str) -> str:
    # Visszatér: "updated" | "not_found"
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return "not_found"

    first_raw = lines[0]
    first_stripped, eol = split_line_ending(first_raw)

    m = PATTERN.match(first_stripped)
    if not m:
        return "not_found"

    prefix, old_date, suffix = m.groups()
    new_first = f"{prefix}{today_str}{suffix}{eol}"
    lines[0] = new_first

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    return "updated"


def main():
    parser = argparse.ArgumentParser(
        description="TXT fájlok első sorában a 'visited: YYYY.MM.DD' dátum cseréje a mai dátumra."
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default=".",
        help="A mappa útvonala (alapértelmezett: aktuális mappa).",
    )
    args = parser.parse_args()

    today_str = date.today().strftime("%Y.%m.%d")
    folder = args.folder

    for root, _, files in os.walk(folder):
        for name in files:
            if not name.lower().endswith(".txt"):
                continue

            path = os.path.join(root, name)

            try:
                result = process_file(path, today_str)
                if result == "updated":
                    print(f"feldolgozás: {path} dátum cserélve a maira ({today_str})")
                else:
                    print(f"feldolgozás: {path} sikertelen nem találtam meg a megfelelő sort")
            except Exception as e:
                print(f"feldolgozás: {path} sikertelen: {e}")


if __name__ == "__main__":
    main()

#breakod a mappába a datumok melle es lecsereli a datumokat a mai datumra, csak debugra hasznaltam