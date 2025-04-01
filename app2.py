# app.py
import os
import sys
import requests
import subprocess
import tempfile
import threading
import zipfile
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# Attempt to import BeautifulSoup; if not found, exit with a clear message.
try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    # Uncomment the following lines to auto-install in a development environment:
    # subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4"])
    # from bs4 import BeautifulSoup
    sys.exit("Error: 'beautifulsoup4' is not installed. Please install it (e.g., pip install beautifulsoup4) "
             "and ensure you are running this script in the correct environment.")

from flask import Flask, request, render_template_string, send_file, url_for
from waitress import serve

# Constants and configuration
HEADERS = {
    'User-Agent': 'Mzansi EDGAR Viewer (support@example.com)'
}

CHROME_PATH = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}

# Create a global requests session for connection pooling
session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 10  # seconds

app = Flask(__name__)
# Ensure sensitive keys are provided via environment variables in production
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "replace_this_with_a_secret_key")
# In production, consider using persistent storage instead of in-memory storage
app.config['FILE_STORAGE'] = {}
# A lock to protect shared file storage state (for concurrency)
file_storage_lock = threading.Lock()

FORM_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Mzansi</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f0f0f0; }
        .container { background: white; padding: 20px; border-radius: 8px; max-width: 700px; margin: auto; }
        label { display: block; margin-top: 10px; }
        input, select { width: 100%; padding: 8px; margin-top: 5px; }
        .checkbox { width: auto; }
        .button { margin-top: 20px; padding: 10px 20px; }
        .log { background: #e8e8e8; padding: 10px; margin-top: 20px; white-space: pre-wrap; font-family: Consolas, monospace; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Mzansi</h2>
        <form id="mainForm">
            <label for="cik">Company CIK (numbers only):</label>
            <input type="text" id="cik" name="cik" required>

            <label for="ticker">Ticker (optional):</label>
            <input type="text" id="ticker" name="ticker">

            <label for="fy_month">Fiscal Year-End Month:</label>
            <select id="fy_month" name="fy_month">
                {% for num, month in months %}
                <option value="{{ num }}">{{ month }}</option>
                {% endfor %}
            </select>

            <label for="fy_adjust">Fiscal Year Basis:</label>
            <select id="fy_adjust" name="fy_adjust">
                <option value="Same Year">Same Year</option>
                <option value="Previous Year">Previous Year</option>
            </select>

            <label>
                <input type="checkbox" name="cleanup" class="checkbox">
                Delete HTML and assets after PDF conversion
            </label>

            <button type="button" class="button" onclick="handleSubmit()">Fetch Filing</button>
        </form>

        <div id="log" class="log"></div>

        <script>
            async function handleSubmit() {
                const logDiv = document.getElementById('log');
                logDiv.innerHTML = 'Initializing download sequence...';
                
                try {
                    // Request directory access with explicit write permission
                    const dirHandle = await window.showDirectoryPicker({
                        mode: 'readwrite'
                    });
                    
                    // Verify user gesture is still valid
                    if (!navigator.userActivation.isActive) {
                        throw new Error('User interaction required - please click the button again');
                    }

                    const formData = {
                        cik: document.getElementById('cik').value,
                        ticker: document.getElementById('ticker').value,
                        fy_month: document.getElementById('fy_month').value,
                        fy_adjust: document.getElementById('fy_adjust').value,
                        cleanup: document.querySelector('[name="cleanup"]').checked
                    };

                    logDiv.innerHTML += '\\nFetching filing data...';
                    const response = await fetch('/fetch', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(formData)
                    });

                    const results = await response.json();
                    
                    if (!results.success) {
                        throw new Error(results.message);
                    }

                    logDiv.innerHTML += `\\nFound ${results.files.length} file(s) to download`;
                    
                    // Process files sequentially with error handling
                    for (const file of results.files) {
                        try {
                            logDiv.innerHTML += `\\n[${new Date().toLocaleTimeString()}] Starting ${file.filename}`;
                            
                            const fileResponse = await fetch(file.url);
                            if (!fileResponse.ok) throw new Error(`HTTP ${fileResponse.status}`);
                            
                            const fileData = await fileResponse.blob();
                            
                            const fileHandle = await dirHandle.getFileHandle(
                                file.filename, 
                                { create: true }
                            );
                            const writable = await fileHandle.createWritable();
                            await writable.write(fileData);
                            await writable.close();
                            
                            logDiv.innerHTML += `\\n[${new Date().toLocaleTimeString()}] Saved: ${file.filename}`;
                        } catch (fileError) {
                            logDiv.innerHTML += `\\n[ERROR] ${file.filename}: ${fileError.message}`;
                        }
                    }

                    logDiv.innerHTML += "\\n\\nOperation completed - check selected folder for files";
                } catch (error) {
                    logDiv.innerHTML += `\\n\\nCRITICAL ERROR: ${error.message}`;
                }
            }
        </script>
    </div>
</body>
</html>
"""

def get_chrome_path():
    import platform
    system = platform.system().lower()
    return CHROME_PATH.get(system)

def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
    """
    Calculate the filing period based on the filing date, fiscal year end,
    and the chosen Fiscal Year Basis.
    """
    if fiscal_year_end_month != 12:
        reported_year = filing_date.year if filing_date.month > fiscal_year_end_month else filing_date.year - 1
        if fy_adjust == "Previous Year":
            reported_year -= 1
        if form == "10-K":
            return f"FY{reported_year % 100:02d}"
        elif form == "10-Q":
            if fiscal_year_end_month == 3:
                if 4 <= filing_date.month <= 6:
                    quarter = 4
                    year = reported_year
                elif 7 <= filing_date.month <= 9:
                    quarter = 1
                    year = reported_year + 1
                elif 10 <= filing_date.month <= 12:
                    quarter = 2
                    year = reported_year + 1
                elif 1 <= filing_date.month <= 3:
                    quarter = 3
                    year = reported_year + 1
                return f"{quarter}Q{year % 100:02d}"
            else:
                quarter = ((filing_date.month - fiscal_year_end_month - 1) % 12) // 3 + 1
                year = reported_year + (filing_date.month < fiscal_year_end_month)
                return f"{quarter}Q{year % 100:02d}"
        else:
            return f"FY{reported_year % 100:02d}"
    else:
        # Special handling for December year end
        if form == "10-K":
            fiscal_year = filing_date.year if filing_date.month > 3 else filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"
        elif form == "10-Q":
            month = filing_date.month
            if month in [1, 2, 3]:
                fiscal_year = filing_date.year - 1
                quarter = 4
            elif month in [4, 5, 6]:
                fiscal_year = filing_date.year
                quarter = 1
            elif month in [7, 8, 9]:
                fiscal_year = filing_date.year
                quarter = 2
            elif month in [10, 11, 12]:
                fiscal_year = filing_date.year
                quarter = 3
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"{quarter}Q{fiscal_year % 100:02d}"
        else:
            fiscal_year = filing_date.year if filing_date.month > 3 else filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"

def download_assets(soup, base_url, output_dir, log_lines):
    downloaded_assets = []
    for tag in soup.find_all(['img', 'link', 'script']):
        url_attr = 'src' if tag.name in ['img', 'script'] else 'href'
        asset_url = tag.get(url_attr)
        if not asset_url:
            continue
        absolute_url = urljoin(base_url, asset_url)
        try:
            parsed = urlparse(absolute_url)
            if parsed.scheme not in ['http', 'https']:
                continue
            r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            filename = os.path.basename(parsed.path)
            if not filename:
                continue
            local_path = os.path.join(output_dir, filename)
            with open(local_path, 'wb') as f:
                f.write(r.content)
            tag[url_attr] = filename
            downloaded_assets.append(filename)
            log_lines.append(f"Downloaded asset: {filename}")
        except Exception as e:
            log_lines.append(f"Asset error for {absolute_url}: {str(e)}")
    return downloaded_assets

def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    chrome_path = get_chrome_path()
    if not chrome_path or not os.path.exists(chrome_path):
        log_lines.append("Chrome not found - PDF conversion skipped")
        return None
    try:
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        fiscal_year_end_month = int(fy_month_idx)
        period = get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        pdf_filename = f"{base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        cmd = [
            chrome_path,
            "--headless",
            "--disable-gpu",
            f"--print-to-pdf={pdf_path}",
            "--no-sandbox",
            f"file://{os.path.abspath(html_path)}"
        ]
        log_lines.append(f"Converting to PDF: {pdf_filename}")
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if os.path.exists(pdf_path):
            log_lines.append("PDF created successfully")
            return pdf_path
        else:
            log_lines.append("PDF conversion failed - no output file")
            return None
    except subprocess.TimeoutExpired:
        log_lines.append("PDF conversion timed out")
        return None
    except subprocess.CalledProcessError as e:
        log_lines.append(f"PDF conversion failed: {e.stderr.decode()}")
        return None
    except Exception as e:
        log_lines.append(f"Conversion error: {str(e)}")
        return None

def cleanup_files(html_path, assets, output_dir, log_lines):
    try:
        if os.path.exists(html_path):
            os.remove(html_path)
            log_lines.append("Cleaned HTML file")
        for asset in assets:
            asset_path = os.path.join(output_dir, asset)
            if os.path.exists(asset_path):
                os.remove(asset_path)
                log_lines.append(f"Cleaned asset: {asset}")
    except Exception as e:
        log_lines.append(f"Cleanup error: {str(e)}")

def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, output_dir):
    try:
        log_lines.append(f"Processing {form} filing")
        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        html_filename = f"{cik}_{form}_{date}_{accession}.html"
        html_path = os.path.join(output_dir, html_filename)
        decoded_text = r.content.decode('utf-8', errors='replace')
        replacements = {
            "â€": "\"",
            "â€œ": "\"",
            "â€™": "'",
            "â€˜": "'",
            "â€“": "-",
            "â€”": "-"
        }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)
        soup = BeautifulSoup(decoded_text, 'html.parser')
        if not soup.find('meta', charset=True):
            meta = soup.new_tag('meta', charset='UTF-8')
            if soup.head:
                soup.head.insert(0, meta)
            else:
                head = soup.new_tag('head')
                head.append(meta)
                if soup.html:
                    soup.html.insert(0, head)
                else:
                    soup.insert(0, head)
        downloaded_assets = download_assets(soup, doc_url, output_dir, log_lines)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        log_lines.append("HTML processed")
        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)
        if pdf_path and cleanup_flag:
            cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
        return (form, pdf_path)
    except requests.exceptions.RequestException as e:
        log_lines.append(f"Download error: {str(e)}")
    except Exception as e:
        log_lines.append(f"Processing error: {str(e)}")
    return (form, None)

def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    pdf_files = {"10-K": [], "10-Q": []}
    if not cik.isdigit():
        log_lines.append("Invalid CIK format")
        return pdf_files
    cik_padded = cik.zfill(10)
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    log_lines.append("Accessing SEC database...")
    try:
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        submissions = r.json()
    except Exception as e:
        log_lines.append(f"Connection error: {str(e)}")
        return pdf_files

    try:
        filings = submissions['filings']['recent']
    except KeyError as e:
        log_lines.append(f"Data format error: {str(e)}")
        return pdf_files

    fiscal_year_end_month = int(fy_month)
    valid_forms = ['10-K', '10-Q']
    tasks = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx in range(len(filings['accessionNumber'])):
            form = filings['form'][idx]
            if form not in valid_forms:
                continue
            try:
                filing_date_str = filings['filingDate'][idx]
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                period = get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust)
                # Filtering: Only require filings from FY17 and newer.
                if form == "10-K":
                    if period.startswith("FY"):
                        year = int(period[2:])
                        if year < 17:
                            break  # Stop processing further filings
                    else:
                        continue
                elif form == "10-Q":
                    if "Q" in period:
                        year = int(period.split("Q")[-1])
                        if year <= 17:
                            break  # Stop processing further filings
                    else:
                        continue
                accession = filings['accessionNumber'][idx].replace('-', '')
                doc_file = filings['primaryDocument'][idx]
                doc_url = f"{base_url}{accession}/{doc_file}"
                tasks.append(executor.submit(
                    download_and_process,
                    doc_url, cik_padded, form, filing_date_str, accession,
                    ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir
                ))
            except Exception as e:
                log_lines.append(f"Skipped filing {idx+1}: {str(e)}")
        for future in as_completed(tasks):
            form, pdf_path = future.result()
            if pdf_path:
                pdf_files[form].append(pdf_path)
    return pdf_files

def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    zip_filename = f"{cik}_filings.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for form in pdf_files:
                folder = form  # Using form name (10-K or 10-Q) as folder name in zip
                for pdf_file in pdf_files[form]:
                    arcname = os.path.join(folder, os.path.basename(pdf_file))
                    zipf.write(pdf_file, arcname=arcname)
                    log_lines.append(f"Added {os.path.basename(pdf_file)} to {folder}/")
        log_lines.append("ZIP archive created successfully")
        return zip_path
    except Exception as e:
        log_lines.append(f"ZIP archive creation failed: {str(e)}")
        return None

@app.route("/", methods=["GET"])
def index():
    months = [(str(i), datetime(2000, i, 1).strftime('%B')) for i in range(1, 13)]
    return render_template_string(FORM_TEMPLATE, months=months, log=None, pdf_url=None)

@app.route("/fetch", methods=["POST"])
def fetch_filing():
    data = request.json
    cik = data.get("cik", "").strip()
    ticker = data.get("ticker", "").strip()
    fy_month = data.get("fy_month", "12")
    fy_adjust = data.get("fy_adjust", "Same Year")
    cleanup_flag = data.get("cleanup", False)
    
    log_lines = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_files = process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir)
        if not any(pdf_files.values()):
            return {
                "success": False,
                "message": "No valid filings found for this criteria"
            }
        zip_path = create_zip_archive(pdf_files, cik.zfill(10), log_lines, tmp_dir)
        if not zip_path:
            return {
                "success": False,
                "message": "Failed to create ZIP archive"
            }
        filename = os.path.basename(zip_path)
        with open(zip_path, 'rb') as f:
            zip_data = f.read()
        with file_storage_lock:
            app.config['FILE_STORAGE'][filename] = zip_data
        file_urls = [{
            "filename": filename,
            "url": url_for("download_file", filename=filename, _external=True)
        }]
        return {
            "success": True,
            "files": file_urls
        }

@app.route("/download/<filename>")
def download_file(filename):
    with file_storage_lock:
        file_data = app.config['FILE_STORAGE'].get(filename)
    if not file_data:
        return "File not found", 404
    mimetype = 'application/zip' if filename.lower().endswith('.zip') else 'application/pdf'
    return send_file(
        BytesIO(file_data),
        mimetype=mimetype,
        as_attachment=True,
        download_name=filename
    )

@app.route("/share", methods=["GET"])
def share_files():
    with file_storage_lock:
        files = [{"filename": k, "url": url_for("download_file", filename=k, _external=True)}
                 for k in app.config['FILE_STORAGE']]
    share_template = """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Shared Filings</title>
    </head>
    <body>
        <h2>Available Shared Files</h2>
        <ul>
            {% for file in files %}
            <li><a href="{{ file.url }}">{{ file.filename }}</a></li>
            {% endfor %}
        </ul>
    </body>
    </html>
    """
    return render_template_string(share_template, files=files)

if __name__ == "__main__":
    try:
        import streamlit as st
        import threading

        def run_flask():
            serve(app, host="0.0.0.0", port=5000, threads=2, url_prefix="/mzansi")

        # Start the Flask app in a background thread if running via Streamlit
        if "server_started" not in st.session_state:
            st.session_state.server_started = True
            threading.Thread(target=run_flask, daemon=True).start()
        st.write("Mzansi app is running. Visit [http://localhost:5000/mzansi](http://localhost:5000/mzansi) to access the application.")
    except ImportError:
        # Fallback: run normally if Streamlit is not available.
        serve(app, host="0.0.0.0", port=5000, threads=2, url_prefix="/mzansi")
