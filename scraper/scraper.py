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
KTU_API = "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable"
ROOT = Path(__file__).parent.parent
DATA_FILE = ROOT / "data" / "exams.json"
HASHES_FILE = ROOT / "data" / "pdf_hashes.json"

# URL patterns to test for PDF downloads (in order of likelihood)
PDF_URL_PATTERNS = [
    ("encryptId",    "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable/getAttachment/{}"),
    ("encryptId",    "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable/download/{}"),
    ("encryptId",    "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable/attachment/{}"),
    ("encryptId",    "https://api.ktu.edu.in/ktu-web-portal-api/anon/attachment/{}"),
    ("attachmentId", "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable/getAttachment/{}"),
    ("attachmentId", "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable/download/{}"),
    ("attachmentId", "https://api.ktu.edu.in/ktu-web-portal-api/anon/attachment/{}"),
]


def fetch_all_timetable_pdfs():
    """
    Opens KTU timetable in a headless browser, fetches ALL pages via the API
    using the browser's authenticated session, and downloads all PDFs.
    Returns list of {url, name, fileName, pdf_bytes, meta}.
    """
    results = []
    first_page_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        def handle_response(response):
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and "anon/timetable" in response.url and "Weblogs" not in response.url:
                    data = response.json()
                    first_page_data.update(data)
                    print(f"  Page 1 captured: {len(data.get('content', []))} entries, totalPages={data.get('totalPages')}")
            except Exception:
                pass

        page.on("response", handle_response)

        print("  Loading KTU timetable page in browser...")
        try:
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="domcontentloaded")
        except Exception as e:
            print(f"  Retrying page load: {e}")
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="load")
        page.wait_for_timeout(6000)

        if not first_page_data:
            print("  No API data captured — page may not have loaded properly")
            browser.close()
            return []

        # Collect all entries across all pages
        all_entries = list(first_page_data.get("content", []))
        total_pages = first_page_data.get("totalPages", 1)
        print(f"  Total pages: {total_pages} — fetching all...")

        for page_num in range(1, total_pages):
            try:
                # Use context.request — same session as the browser, fully authenticated
                resp = context.request.get(f"{KTU_API}?page={page_num}", timeout=30000)
                if resp.ok:
                    data = resp.json()
                    entries = data.get("content", [])
                    all_entries.extend(entries)
                    print(f"  Page {page_num + 1}/{total_pages}: +{len(entries)} entries")
                else:
                    print(f"  Page {page_num + 1} failed with status {resp.status} — stopping pagination")
                    break
            except Exception as e:
                print(f"  Page {page_num + 1} error: {e} — stopping pagination")
                break

        print(f"  Total entries fetched: {len(all_entries)}")

        if not all_entries:
            browser.close()
            return []

        # Detect working PDF download URL using the first entry as a test
        first = all_entries[0]
        working_key = None
        working_template = None

        print(f"  Testing PDF URL patterns for: {first.get('fileName')}")
        for key_field, template in PDF_URL_PATTERNS:
            key_val = first.get(key_field, "")
            if not key_val:
                continue
            test_url = template.format(key_val)
            try:
                resp = context.request.get(test_url, timeout=15000)
                ct = resp.headers.get("content-type", "")
                print(f"  {test_url} → {resp.status} [{ct[:50]}]")
                if resp.ok and "pdf" in ct.lower():
                    working_key = key_field
                    working_template = template
                    print(f"  ✓ Working URL pattern: {template}")
                    break
            except Exception as e:
                print(f"  {test_url} → ERROR: {e}")

        if not working_template:
            print("  Could not determine PDF download URL pattern")
            browser.close()
            return []

        # Download all PDFs using the confirmed pattern
        print(f"\n  Downloading {len(all_entries)} PDFs...")
        for entry in all_entries:
            key_val = entry.get(working_key, "")
            if not key_val:
                continue
            url = working_template.format(key_val)
            name = entry.get("timeTableTitle") or entry.get("title") or entry.get("fileName", "")
            file_name = entry.get("fileName", "")

            try:
                resp = context.request.get(url, timeout=60000)
                ct = resp.headers.get("content-type", "")
                if resp.ok and "pdf" in ct.lower():
                    results.append({
                        "url": url,
                        "name": name,
                        "fileName": file_name,
                        "pdf_bytes": resp.body(),
                        "meta": entry,
                    })
                    print(f"  ✓ {file_name}")
                else:
                    print(f"  ✗ {file_name} — {resp.status} {ct[:40]}")
            except Exception as e:
                print(f"  ✗ {file_name} — {e}")

        browser.close()

    return results


def extract_text_from_pdf(pdf_bytes):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(pdf_bytes)
        tmp_path = f.name
    try:
        text = ""
        with pdfplumber.open(tmp_path) as pdf:
            for pg in pdf.pages:
                page_text = pg.extract_text()
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
- Include rescheduled exams with a note explaining the change.

Document title: {source_name}

Text:
{text[:10000]}"""

    response = client.models.generate_content(model="gemini-1.5-flash", contents=prompt)
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


def push_file_to_github(file_path, content_str, token, repo):
    import base64
    api_url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    resp = requests.get(api_url, headers=headers, timeout=15)
    sha = resp.json().get("sha", "") if resp.status_code == 200 else ""
    payload = {
        "message": f"chore: update {file_path}",
        "content": base64.b64encode(content_str.encode("utf-8")).decode("ascii"),
        "sha": sha,
    }
    put_resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
    if put_resp.status_code not in (200, 201):
        print(f"  WARNING: GitHub push failed for {file_path}: {put_resp.status_code}")
    else:
        print(f"  Pushed {file_path} to GitHub")


def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    gemini = genai.Client(api_key=api_key)

    print("Fetching KTU timetable PDFs (all pages)...")
    pdf_items = fetch_all_timetable_pdfs()
    print(f"\nTotal downloadable PDFs: {len(pdf_items)}")

    if not pdf_items:
        print("No PDFs available. Exiting.")
        return

    hashes = load_json(HASHES_FILE, {})
    existing_data = load_json(DATA_FILE, {"last_updated": None, "sources": [], "exams": []})
    existing_by_url = {}
    for exam in existing_data.get("exams", []):
        existing_by_url.setdefault(exam.get("source_url"), []).append(exam)

    all_exams = []
    sources = []
    any_changed = False

    for item in pdf_items:
        url = item["url"]
        name = item["name"]
        file_name = item["fileName"]
        pdf_bytes = item["pdf_bytes"]
        pdf_hash = hashlib.md5(pdf_bytes).hexdigest()

        if hashes.get(url) == pdf_hash:
            print(f"Unchanged: {file_name}")
            all_exams.extend(existing_by_url.get(url, []))
            sources.append({"url": url, "name": name, "hash": pdf_hash})
            continue

        print(f"New/updated: {file_name}")
        text = extract_text_from_pdf(pdf_bytes)

        if not text:
            print("  No text extracted (scanned PDF?) — skipping")
            sources.append({"url": url, "name": name, "hash": pdf_hash, "warning": "scanned_pdf"})
            hashes[url] = pdf_hash
            any_changed = True
            continue

        exams = parse_with_gemini(gemini, text, name)
        for exam in exams:
            exam["source_url"] = url
            exam["source_name"] = name
        all_exams.extend(exams)
        hashes[url] = pdf_hash
        sources.append({"url": url, "name": name, "hash": pdf_hash})
        any_changed = True
        print(f"  Extracted {len(exams)} exam entries")

    if any_changed:
        new_data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "exams": all_exams,
        }
        save_json(DATA_FILE, new_data)
        save_json(HASHES_FILE, hashes)
        print(f"\nSaved {len(all_exams)} total exam entries to data/exams.json")

        gh_token = os.environ.get("GITHUB_TOKEN")
        gh_repo = os.environ.get("GITHUB_REPOSITORY")
        if gh_token and gh_repo:
            print("Pushing updated files to GitHub...")
            push_file_to_github("data/exams.json",
                                 json.dumps(new_data, indent=2, ensure_ascii=False),
                                 gh_token, gh_repo)
            push_file_to_github("data/pdf_hashes.json",
                                 json.dumps(hashes, indent=2, ensure_ascii=False),
                                 gh_token, gh_repo)
    else:
        print("No changes — data/exams.json unchanged")


if __name__ == "__main__":
    main()
