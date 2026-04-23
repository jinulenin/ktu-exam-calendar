import os
import json
import hashlib
import tempfile
import re
import warnings
from datetime import datetime, timezone
from pathlib import Path

import requests
import pdfplumber
from google import genai
from playwright.sync_api import sync_playwright

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

TIMETABLE_URL = "https://ktu.edu.in/exam/timetable"
ROOT = Path(__file__).parent.parent
DATA_FILE = ROOT / "data" / "exams.json"
HASHES_FILE = ROOT / "data" / "pdf_hashes.json"


def fetch_pdf_links():
    """Use a headless browser to render the page and collect all PDF links."""
    links = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("  Opening KTU timetable page in headless browser...")
        page.goto(TIMETABLE_URL, timeout=60000, wait_until="networkidle")

        # Wait a bit more for any lazy-loaded content
        page.wait_for_timeout(3000)

        # Grab every anchor whose href ends with .pdf
        anchors = page.eval_on_selector_all(
            "a",
            """els => els
                .filter(el => el.href && el.href.toLowerCase().includes('.pdf'))
                .map(el => ({ url: el.href, name: el.innerText.trim() || el.href.split('/').pop() }))
            """
        )
        links.extend(anchors)

        # Also scan for links inside iframes if any
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                frame_anchors = frame.eval_on_selector_all(
                    "a",
                    """els => els
                        .filter(el => el.href && el.href.toLowerCase().includes('.pdf'))
                        .map(el => ({ url: el.href, name: el.innerText.trim() || el.href.split('/').pop() }))
                    """
                )
                links.extend(frame_anchors)
            except Exception:
                pass

        browser.close()

    # Deduplicate by URL
    seen = set()
    unique = []
    for link in links:
        if link["url"] not in seen:
            seen.add(link["url"])
            unique.append(link)
    return unique


def download_pdf(url):
    resp = requests.get(url, verify=False, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_text_from_pdf(pdf_bytes):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(pdf_bytes)
        tmp_path = f.name
    try:
        text = ""
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text.strip()
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def parse_with_gemini(client, text, source_name):
    prompt = f"""You are extracting exam timetable data from a KTU (Kerala Technological University) official notification.

Extract every exam entry and return a JSON array. Each object must have:
- "course_code": string (e.g. "CS301") or null
- "course_name": string (e.g. "Data Structures") or null
- "date": string in YYYY-MM-DD format or null
- "day": string (e.g. "Monday") or null
- "time": string (e.g. "10:00 AM") or null
- "semester": string (e.g. "S6") or null
- "branch": string (e.g. "CSE" or "All Branches") or null
- "notes": string (any rescheduling notice or special instruction) or null

Rules:
- Return ONLY a valid JSON array, no explanation or markdown fences.
- If a field is not mentioned, use null.
- If the document mentions a rescheduled exam, include it with a note.

Document title: {source_name}

Text:
{text[:10000]}"""

    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt,
    )
    raw = response.text.strip()
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if match:
        return json.loads(match.group())
    return []


def load_json(path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    gemini = genai.Client(api_key=api_key)

    print("Fetching PDF links from KTU timetable page...")
    links = fetch_pdf_links()
    print(f"Found {len(links)} PDF link(s)")

    if not links:
        print("No PDF links found. The page structure may have changed.")
        print("Saving empty data file so the calendar still loads cleanly.")
        existing = load_json(DATA_FILE, {"last_updated": None, "sources": [], "exams": []})
        existing["last_updated"] = datetime.now(timezone.utc).isoformat()
        save_json(DATA_FILE, existing)
        return

    hashes = load_json(HASHES_FILE, {})
    existing_data = load_json(DATA_FILE, {"last_updated": None, "sources": [], "exams": []})

    existing_by_source = {}
    for exam in existing_data.get("exams", []):
        src = exam.get("source_url")
        existing_by_source.setdefault(src, []).append(exam)

    all_exams = []
    sources = []
    any_changed = False

    for link in links:
        url = link["url"]
        name = link["name"] or url.split("/")[-1]
        print(f"\nChecking: {name}")

        try:
            pdf_bytes = download_pdf(url)
            pdf_hash = hashlib.md5(pdf_bytes).hexdigest()

            if hashes.get(url) == pdf_hash:
                print("  Unchanged — reusing cached data")
                all_exams.extend(existing_by_source.get(url, []))
                sources.append({"url": url, "name": name, "hash": pdf_hash})
                continue

            print("  New or updated PDF — extracting text...")
            text = extract_text_from_pdf(pdf_bytes)

            if not text:
                print("  No text extracted (possibly a scanned image PDF — skipping)")
                sources.append({"url": url, "name": name, "hash": pdf_hash, "warning": "scanned_pdf"})
                hashes[url] = pdf_hash
                any_changed = True
                continue

            print("  Parsing with Gemini...")
            exams = parse_with_gemini(gemini, text, name)

            for exam in exams:
                exam["source_url"] = url
                exam["source_name"] = name

            all_exams.extend(exams)
            hashes[url] = pdf_hash
            sources.append({"url": url, "name": name, "hash": pdf_hash})
            any_changed = True
            print(f"  Extracted {len(exams)} exam entries")

        except Exception as e:
            print(f"  ERROR: {e}")
            all_exams.extend(existing_by_source.get(url, []))

    if any_changed:
        new_data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "exams": all_exams,
        }
        save_json(DATA_FILE, new_data)
        save_json(HASHES_FILE, hashes)
        print(f"\nSaved {len(all_exams)} total exam entries to data/exams.json")
    else:
        print("\nNo changes detected — data/exams.json unchanged")


if __name__ == "__main__":
    main()
