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
import google.generativeai as genai
from bs4 import BeautifulSoup

# Suppress SSL warnings since KTU's certificate has issues
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

TIMETABLE_URL = "https://ktu.edu.in/exam/timetable"
BASE_URL = "https://ktu.edu.in"
ROOT = Path(__file__).parent.parent
DATA_FILE = ROOT / "data" / "exams.json"
HASHES_FILE = ROOT / "data" / "pdf_hashes.json"


def fetch_pdf_links():
    resp = requests.get(TIMETABLE_URL, verify=False, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            if not href.startswith("http"):
                href = BASE_URL + href
            label = a.get_text(strip=True) or href.split("/")[-1]
            links.append({"url": href, "name": label})
    return links


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


def parse_with_gemini(text, source_name):
    model = genai.GenerativeModel("gemini-1.5-flash")
    prompt = f"""You are extracting exam timetable data from a KTU (Kerala Technological University) official notification.

Extract every exam entry and return a JSON array. Each object must have:
- "course_code": string (e.g. "CS301") or null
- "course_name": string (e.g. "Data Structures") or null
- "date": string in YYYY-MM-DD format or null
- "day": string (e.g. "Monday") or null
- "time": string (e.g. "10:00 AM") or null
- "semester": string (e.g. "S6") or null
- "branch": string (e.g. "CSE" or "All Branches") or null
- "notes": string (any special instruction, rescheduling notice, etc.) or null

Rules:
- Return ONLY a valid JSON array, no explanation or markdown.
- If a field is not mentioned, set it to null.
- If the document mentions a rescheduled exam, include it with a note.
- If there are multiple branches or semesters in one row, create separate entries.

Document title: {source_name}

Text:
{text[:10000]}"""

    response = model.generate_content(prompt)
    raw = response.text.strip()
    # Strip markdown code fences if present
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
    genai.configure(api_key=api_key)

    print("Fetching PDF links from KTU timetable page...")
    links = fetch_pdf_links()
    print(f"Found {len(links)} PDF link(s)")

    hashes = load_json(HASHES_FILE, {})
    existing_data = load_json(DATA_FILE, {"last_updated": None, "sources": [], "exams": []})

    # Build a lookup of existing exams by source URL for unchanged PDFs
    existing_by_source = {}
    for exam in existing_data.get("exams", []):
        src = exam.get("source_url")
        existing_by_source.setdefault(src, []).append(exam)

    all_exams = []
    sources = []
    any_changed = False

    for link in links:
        url = link["url"]
        name = link["name"]
        print(f"\nChecking: {name}")
        print(f"  URL: {url}")

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
            exams = parse_with_gemini(text, name)

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
            # Keep existing data for this source on error
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
