import os
import json
import hashlib
import tempfile
import re
import warnings
import base64
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
        context = browser.new_context(ignore_https_errors=True)
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

        # Wait for the timetable table to actually render in DOM
        print("  Waiting for table to render...")
        for selector in ["table", "mat-table", "tr", ".cdk-row", "[mat-row]", "tbody tr", "mat-row"]:
            try:
                page.wait_for_selector(selector, timeout=8000)
                print(f"  Table found via selector: '{selector}'")
                break
            except Exception:
                pass

        # Scroll down to ensure content is visible
        page.evaluate("window.scrollTo(0, 600)")
        page.wait_for_timeout(2000)

        # Print ALL elements to understand full page structure
        elements = page.eval_on_selector_all(
            "button, a, mat-icon, [role='button'], td, th, mat-cell, mat-header-cell",
            "els => els.map(el => ({tag: el.tagName, text: el.textContent.trim().slice(0,50), cls: el.className.slice(0,70), aria: el.getAttribute('aria-label')||''}))"
        )
        print(f"\n  ALL interactive/table elements ({len(elements)} total):")
        for el in elements:
            print(f"    {el['tag']} | text='{el['text']}' | aria='{el['aria']}' | class='{el['cls'][:50]}'")

        # Try clicking what looks like a download/view button for the first row
        print("\n  Trying to click download/view buttons...")
        for selector in [
            "button[aria-label='View']", "button[aria-label='Download']",
            "button[aria-label='view']", "button[aria-label='download']",
            "mat-icon:text('visibility')", "mat-icon:text('get_app')",
            "mat-icon:text('download')", "mat-icon:text('picture_as_pdf')",
            "td button", "tr button", ".mat-icon-button", "button.mat-mdc-icon-button",
        ]:
            try:
                loc = page.locator(selector)
                count = loc.count()
                if count > 0:
                    print(f"  Found {count} elements for '{selector}' — clicking first")
                    before = len(pdf_requests_captured)
                    loc.first.click()
                    page.wait_for_timeout(3000)
                    if len(pdf_requests_captured) > before:
                        print(f"  ✓ Click triggered PDF request!")
                        break
            except Exception as e:
                pass

        print(f"\n  Total PDF requests captured via clicks: {len(pdf_requests_captured)}")
        print(f"  PDF request URLs: {pdf_requests_captured}")

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
