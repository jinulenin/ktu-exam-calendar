import os
import json
import hashlib
import tempfile
import re
import warnings
import base64
import time
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

# Only process notifications from this date onwards (YYYY-MM-DD)
CUTOFF_DATE = "2026-01-01"

# Only include these courses (case-insensitive, checked against entry title)
INCLUDE_COURSES = ["b.tech", "btech", "bca"]

# Exclude these even if an include keyword appears
EXCLUDE_COURSES = ["mba", "b.arch", "barch", "b.des", "bdes", "bhmct",
                   "m.tech", "mtech", "mca", "phd", "m.sc", "msc"]


def is_relevant(entry):
    """Return True if this timetable entry should be downloaded and parsed."""
    # Date filter — skip anything before the cutoff
    created = entry.get("createdDate", "")
    if created and created[:10] < CUTOFF_DATE:
        return False

    # Course filter — must mention B.Tech or BCA, must not be another course only
    text = (
        (entry.get("timeTableTitle") or "") + " " +
        (entry.get("title") or "") + " " +
        (entry.get("details") or "")
    ).lower()

    if any(kw in text for kw in EXCLUDE_COURSES):
        return False
    if not any(kw in text for kw in INCLUDE_COURSES):
        return False

    return True


def browser_fetch_json(page, url, extra_headers=None):
    """Run fetch() inside the browser JS context with captured auth headers."""
    headers_js = json.dumps(extra_headers or {})
    result = page.evaluate(f"""async () => {{
        try {{
            const resp = await fetch("{url}", {{
                credentials: "include",
                headers: {headers_js}
            }});
            if (!resp.ok) {{
                console.log("fetch failed", resp.status, "{url}");
                return null;
            }}
            return await resp.json();
        }} catch(e) {{ console.log("fetch error", e.message); return null; }}
    }}""")
    return result


def browser_fetch_pdf(page, url, extra_headers=None):
    """Download a PDF as base64 via browser JS fetch, with debug output."""
    headers_js = json.dumps(extra_headers or {})
    result = page.evaluate(f"""async () => {{
        try {{
            const resp = await fetch("{url}", {{
                credentials: "include",
                headers: {headers_js}
            }});
            const ct = resp.headers.get("content-type") || "";
            if (!resp.ok || !ct.toLowerCase().includes("pdf")) {{
                return {{error: resp.status + " " + ct}};
            }}
            const buf = await resp.arrayBuffer();
            const bytes = new Uint8Array(buf);
            let bin = "";
            bytes.forEach(b => bin += String.fromCharCode(b));
            return {{data: btoa(bin)}};
        }} catch(e) {{ return {{error: e.message}}; }}
    }}""")
    if isinstance(result, dict):
        if "data" in result:
            return base64.b64decode(result["data"])
        print(f"    fetch result: {result.get('error', 'unknown')}")
    return None


def fetch_all_timetable_pdfs():
    """
    Opens KTU timetable page, clicks through pages and download buttons,
    and captures PDFs via browser interaction.
    Returns list of {url, name, fileName, pdf_bytes, meta}.
    """
    results = []
    first_page_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True, accept_downloads=True)
        page = context.new_page()

        all_entries = []
        pdf_requests_captured = []

        def handle_response(response):
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and "anon/timetable" in response.url and "Weblogs" not in response.url:
                    data = response.json()
                    entries = data.get("content", [])
                    all_entries.extend(entries)
                    if not first_page_data:
                        first_page_data.update(data)
                    print(f"  API response: +{len(entries)} entries (total now {len(all_entries)}), totalPages={data.get('totalPages')}")
            except Exception:
                pass

        def handle_request(request):
            url = request.url
            if ("pdf" in url.lower() or "download" in url.lower() or "attachment" in url.lower()) and "Weblogs" not in url:
                pdf_requests_captured.append(url)
                print(f"  >>> PDF request captured: {url}")

        page.on("response", handle_response)
        page.on("request", handle_request)

        print("  Loading KTU timetable page...")
        try:
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="domcontentloaded")
        except Exception as e:
            print(f"  Retrying: {e}")
            page.goto(TIMETABLE_URL, timeout=90000, wait_until="load")
        page.wait_for_timeout(6000)

        if not all_entries:
            print("  No entries captured from page load")
            browser.close()
            return []

        total_pages = first_page_data.get("totalPages", 1)
        print(f"  Page 1 loaded. Total pages: {total_pages}")
        page.wait_for_timeout(2000)

        # Collect entries from pages — stop early once we hit old entries
        current_page = 1
        stop_early = False
        while current_page < total_pages and not stop_early:
            next_link = page.locator("a[aria-label='Next page']")
            if next_link.count() == 0:
                break
            entries_before = len(all_entries)
            next_link.first.click()
            for _ in range(20):
                page.wait_for_timeout(500)
                if len(all_entries) > entries_before:
                    break
            current_page += 1
            new_entries = all_entries[entries_before:]
            # Stop if ALL entries on this page are before the cutoff
            if new_entries and all(
                (e.get("createdDate") or "9999")[:10] < CUTOFF_DATE
                for e in new_entries
            ):
                print(f"  Page {current_page}: all entries before {CUTOFF_DATE} — stopping pagination")
                stop_early = True
            else:
                print(f"  Page {current_page}: +{len(new_entries)} entries (total {len(all_entries)})")

        # Apply filters
        relevant = [e for e in all_entries if is_relevant(e)]
        print(f"\n  Total entries: {len(all_entries)} → after filtering: {len(relevant)} relevant")
        print(f"  (date ≥ {CUTOFF_DATE}, courses: B.Tech / BCA only)")

        # Save a snapshot of all entries before re-navigation
        # (re-navigating would trigger more API responses and duplicate all_entries)
        entry_snapshot = list(all_entries)
        all_entries.clear()

        # Navigate back to page 1 to start clicking download buttons
        page.goto(TIMETABLE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)

        BUTTON_SEL = "button.btn-md.bg-light.border"
        downloaded_urls = set()
        page_num = 1

        while True:
            buttons = page.locator(BUTTON_SEL)
            count = buttons.count()
            print(f"\n  Page {page_num}: {count} buttons")

            for i in range(count):
                btn = buttons.nth(i)
                btn_text_full = btn.text_content().strip()

                # Match to API entry by page/button position for relevance check
                # (button text is often generic — don't rely on it for filtering)
                entry_index = (page_num - 1) * 10 + i
                entry = entry_snapshot[entry_index] if entry_index < len(entry_snapshot) else {}
                if not is_relevant(entry):
                    title = (entry.get("timeTableTitle") or entry.get("title") or btn_text_full)[:60]
                    print(f"  Skip (not relevant): '{title}'")
                    continue

                btn_text = btn_text_full[:60]
                print(f"  Clicking: '{btn_text}'")

                try:
                    with page.expect_download(timeout=15000) as dl_info:
                        btn.click()
                    dl = dl_info.value
                    dl.save_as(dl.suggested_filename)
                    pdf_b = Path(dl.suggested_filename).read_bytes()
                    Path(dl.suggested_filename).unlink(missing_ok=True)
                    url = dl.url

                    if url in downloaded_urls:
                        print(f"    Already downloaded, skipping")
                        continue
                    downloaded_urls.add(url)

                    results.append({
                        "url": url,
                        "name": btn_text,
                        "fileName": entry.get("fileName", dl.suggested_filename),
                        "pdf_bytes": pdf_b,
                        "meta": entry,
                    })
                    print(f"    ✓ {dl.suggested_filename} ({len(pdf_b):,} bytes)")

                except Exception as e:
                    print(f"    ✗ Download failed: {e}")

            # Stop paginating if all entries on this page predate the cutoff
            last_entries_on_page = entry_snapshot[(page_num - 1) * 10: page_num * 10]
            if last_entries_on_page and all(
                (e.get("createdDate") or "9999")[:10] < CUTOFF_DATE
                for e in last_entries_on_page
            ):
                print(f"  All entries on page {page_num} are before {CUTOFF_DATE} — done")
                break

            next_link = page.locator("a[aria-label='Next page']")
            if next_link.count() == 0 or page_num >= total_pages:
                break
            next_link.first.click()
            page.wait_for_timeout(3000)
            page_num += 1

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

    for attempt in range(3):
        try:
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            raw = response.text.strip()
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            return json.loads(match.group()) if match else []
        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                # Daily quota exhausted — no point retrying, raise so main() can stop
                if "limit: 0" in err or attempt == 2:
                    raise RuntimeError("QUOTA_EXHAUSTED") from e
                # Per-minute limit — short wait then retry
                print(f"  Rate limited — waiting 65s (attempt {attempt + 1}/3)...")
                time.sleep(65)
            else:
                print(f"  Gemini error: {e}")
                return []
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

        time.sleep(5)  # Stay under 15 requests/minute free tier limit
        try:
            exams = parse_with_gemini(gemini, text, name)
        except RuntimeError as quota_err:
            if "QUOTA_EXHAUSTED" in str(quota_err):
                print("  Daily Gemini quota exhausted — saving progress and stopping.")
                any_changed = True
                break
            raise

        for exam in exams:
            exam["source_url"] = url
            exam["source_name"] = name
        all_exams.extend(exams)
        hashes[url] = pdf_hash
        sources.append({"url": url, "name": name, "hash": pdf_hash})
        any_changed = True
        print(f"  Extracted {len(exams)} exam entries")

        # Save progress every 5 PDFs so reruns skip already-processed ones
        if len(sources) % 5 == 0:
            partial = {
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "sources": sources,
                "exams": all_exams,
            }
            save_json(DATA_FILE, partial)
            save_json(HASHES_FILE, hashes)
            print(f"  [checkpoint] saved {len(all_exams)} exams so far")

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
