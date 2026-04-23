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


KTU_API = "https://api.ktu.edu.in/ktu-web-portal-api/anon/timetable"


def fetch_pdf_links_from_api():
    """Call the KTU timetable API directly and extract PDF download URLs."""
    resp = requests.get(KTU_API, verify=False, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Print raw structure so we can see the shape of the data
    print(f"  API returned {len(data) if isinstance(data, list) else type(data).__name__}")
    if isinstance(data, list) and data:
        print(f"  First entry keys: {list(data[0].keys()) if isinstance(data[0], dict) else data[0]}")
        print(f"  First entry: {json.dumps(data[0], indent=2)[:500]}")
    elif isinstance(data, dict):
        print(f"  Response keys: {list(data.keys())}")
        print(f"  Response: {json.dumps(data, indent=2)[:500]}")

    links = []
    entries = data if isinstance(data, list) else data.get("data", data.get("results", data.get("content", [])))

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # Try common field names for the file URL
        url = (entry.get("attachmentUrl") or entry.get("fileUrl") or
               entry.get("file_url") or entry.get("url") or
               entry.get("pdfUrl") or entry.get("pdf_url") or
               entry.get("link") or entry.get("documentUrl"))
        # Try common field names for the filename
        filename = (entry.get("attachmentName") or entry.get("fileName") or
                    entry.get("file_name") or entry.get("name") or
                    entry.get("title") or "")
        if url:
            if not url.startswith("http"):
                url = f"https://api.ktu.edu.in/{url.lstrip('/')}"
            links.append({"url": url, "name": filename})

    return links


def fetch_pdf_links():
    """Use a headless browser with network interception to collect PDF links."""
    links = []
    network_pdfs = []
    api_calls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        timetable_json = []

        # Intercept every network response to catch PDFs loaded via API
        def handle_response(response):
            url = response.url
            try:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type and "ktu.edu.in" in url:
                    api_calls.append(url)
                    # Specifically capture the timetable endpoint response
                    if "anon/timetable" in url and "Weblogs" not in url:
                        body = response.text()
                        print(f"\n  === TIMETABLE API RESPONSE ===")
                        print(f"  URL: {url}")
                        print(f"  First 1500 chars: {body[:1500]}")
                        print(f"  ==============================\n")
                        try:
                            data = response.json()
                            if isinstance(data, list):
                                timetable_json.extend(data)
                            elif isinstance(data, dict):
                                timetable_json.append(data)
                        except Exception:
                            pass
            except Exception:
                pass

        page.on("response", handle_response)

        print("  Opening KTU timetable page in headless browser...")
        try:
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="domcontentloaded")
        except Exception as e:
            print(f"  First load attempt failed ({e}), retrying...")
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="load")

        # Wait for dynamic content to load
        page.wait_for_timeout(8000)

        # Scroll down to trigger any lazy-loaded content
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(3000)

        print(f"  Page title: {page.title()}")
        print(f"  API calls intercepted: {len(api_calls)}")
        print(f"  Timetable entries captured: {len(timetable_json)}")

        # Build links from the captured timetable JSON
        if timetable_json:
            print(f"  First entry keys: {list(timetable_json[0].keys()) if isinstance(timetable_json[0], dict) else timetable_json[0]}")
            for entry in timetable_json:
                if not isinstance(entry, dict):
                    continue
                # Try every possible field name that could hold a URL or filename
                for key, val in entry.items():
                    if isinstance(val, str) and val.lower().endswith(".pdf"):
                        url = val if val.startswith("http") else f"https://api.ktu.edu.in/ktu-web-portal-api/{val.lstrip('/')}"
                        name = entry.get("title") or entry.get("name") or entry.get("subject") or val.split("/")[-1]
                        links.append({"url": url, "name": name, "entry": entry})
                        print(f"  Found PDF field '{key}': {url}")

        # Also grab PDF links directly from DOM as fallback
        anchors = page.eval_on_selector_all(
            "a",
            """els => els
                .filter(el => el.href && el.href.toLowerCase().includes('.pdf'))
                .map(el => ({ url: el.href, name: el.innerText.trim() || el.href.split('/').pop() }))
            """
        )
        if anchors:
            print(f"  PDF links in DOM: {len(anchors)}")
            links.extend(anchors)

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


def push_file_to_github(file_path, content_str, token, repo):
    """Push a file to GitHub via the Contents API — no git required."""
    import base64
    api_url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }
    # Get current SHA (required for update)
    resp = requests.get(api_url, headers=headers, timeout=15)
    sha = resp.json().get("sha", "") if resp.status_code == 200 else ""

    payload = {
        "message": f"chore: update {file_path}",
        "content": base64.b64encode(content_str.encode("utf-8")).decode("ascii"),
        "sha": sha,
    }
    put_resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
    if put_resp.status_code not in (200, 201):
        print(f"  WARNING: GitHub API push failed for {file_path}: {put_resp.status_code} {put_resp.text[:200]}")
    else:
        print(f"  Pushed {file_path} to GitHub via API")


def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    gemini = genai.Client(api_key=api_key)

    print("Fetching PDF links from KTU API directly...")
    try:
        links = fetch_pdf_links_from_api()
        print(f"Found {len(links)} PDF link(s) via API")
    except Exception as e:
        print(f"Direct API failed ({e}), falling back to browser scrape...")
        links = fetch_pdf_links()
        print(f"Found {len(links)} PDF link(s) via browser")

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

        # Push via GitHub API if running inside GitHub Actions
        gh_token = os.environ.get("GITHUB_TOKEN")
        gh_repo = os.environ.get("GITHUB_REPOSITORY")
        if gh_token and gh_repo:
            print("Pushing updated data files to GitHub via API...")
            exams_str = json.dumps(new_data, indent=2, ensure_ascii=False)
            hashes_str = json.dumps(hashes, indent=2, ensure_ascii=False)
            push_file_to_github("data/exams.json", exams_str, gh_token, gh_repo)
            push_file_to_github("data/pdf_hashes.json", hashes_str, gh_token, gh_repo)
        else:
            print("(Not in GitHub Actions — skipping API push)")
    else:
        print("\nNo changes detected — data/exams.json unchanged")


if __name__ == "__main__":
    main()
