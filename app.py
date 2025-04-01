# app.py
import os
import sys
import requests
import subprocess
import tempfile
import zipfile
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Error: Missing required package. Install with: pip install beautifulsoup4")

# Configuration constants
HEADERS = {'User-Agent': 'Mzansi EDGAR Viewer (support@example.com)'}
CHROME_PATH = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}
DEFAULT_TIMEOUT = 10
session = requests.Session()
session.headers.update(HEADERS)

def get_chrome_path():
    import platform
    system = platform.system().lower()
    return CHROME_PATH.get(system)

# Core processing functions
def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
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
            else:
                fiscal_year = filing_date.year
                quarter = (month - 1) // 3
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"{quarter + 1}Q{fiscal_year % 100:02d}"

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
            response = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            filename = os.path.basename(parsed.path)
            if not filename:
                continue
            local_path = os.path.join(output_dir, filename)
            with open(local_path, 'wb') as f:
                f.write(response.content)
            tag[url_attr] = filename
            downloaded_assets.append(filename)
            log_lines.append(f"Downloaded asset: {filename}")
        except Exception as e:
            log_lines.append(f"Asset error: {str(e)}")
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
        
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
        
        if os.path.exists(pdf_path):
            log_lines.append("PDF created successfully")
            return pdf_path
        log_lines.append("PDF conversion failed")
        return None
    except Exception as e:
        log_lines.append(f"Conversion error: {str(e)}")
        return None

def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    pdf_files = {"10-K": [], "10-Q": []}
    if not cik.isdigit():
        log_lines.append("Invalid CIK format")
        return pdf_files
    
    cik_padded = cik.zfill(10)
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    
    try:
        response = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        submissions = response.json()
        filings = submissions['filings']['recent']
    except Exception as e:
        log_lines.append(f"SEC API error: {str(e)}")
        return pdf_files

    valid_forms = ['10-K', '10-Q']
    tasks = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx in range(len(filings['accessionNumber'])):
            form = filings['form'][idx]
            if form not in valid_forms:
                continue
            
            try:
                filing_date = datetime.strptime(filings['filingDate'][idx], "%Y-%m-%d")
                accession = filings['accessionNumber'][idx].replace('-', '')
                doc_file = filings['primaryDocument'][idx]
                doc_url = f"{base_url}{accession}/{doc_file}"
                
                tasks.append(executor.submit(
                    self._process_document,
                    doc_url, cik_padded, form, filings['filingDate'][idx],
                    accession, ticker, fy_month, fy_adjust, cleanup_flag,
                    log_lines, tmp_dir
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
                for pdf_file in pdf_files[form]:
                    arcname = os.path.join(form, os.path.basename(pdf_file))
                    zipf.write(pdf_file, arcname=arcname)
                    log_lines.append(f"Added {os.path.basename(pdf_file)} to archive")
        return zip_path
    except Exception as e:
        log_lines.append(f"ZIP error: {str(e)}")
        return None

# Streamlit UI
def main():
    import streamlit as st
    
    st.set_page_config(
        page_title="Mzansi SEC Filing Processor",
        page_icon="📈",
        layout="wide"
    )
    
    st.title("SEC Filing Processor")
    st.write("Fetch and process SEC filings as PDF documents")
    
    with st.form("filing_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            cik = st.text_input("Company CIK Number", placeholder="Enter 10-digit CIK")
            ticker = st.text_input("Company Ticker (Optional)", placeholder="e.g., AAPL")
            fy_month = st.selectbox(
                "Fiscal Year-End Month",
                options=list(range(1, 13)),
                format_func=lambda x: datetime(2000, x, 1).strftime('%B')
            )
        
        with col2:
            fy_adjust = st.selectbox(
                "Fiscal Year Basis",
                ["Same Year", "Previous Year"],
                help="Adjust reporting year for non-calendar fiscal years"
            )
            cleanup_flag = st.checkbox(
                "Cleanup temporary files",
                value=True,
                help="Remove intermediate HTML and assets after PDF conversion"
            )
        
        submitted = st.form_submit_button("Process Filings")
    
    if submitted:
        if not cik or not cik.isdigit():
            st.error("Please enter a valid numeric CIK")
            return
        
        with st.spinner("Processing SEC filings..."):
            log_lines = []
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    pdf_files = process_filing(
                        cik, ticker, str(fy_month), fy_adjust,
                        cleanup_flag, log_lines, tmp_dir
                    )
                    
                    if not any(pdf_files.values()):
                        st.warning("No matching filings found")
                        return
                    
                    zip_path = create_zip_archive(
                        pdf_files, cik.zfill(10), log_lines, tmp_dir
                    )
                    
                    if zip_path and os.path.exists(zip_path):
                        with open(zip_path, "rb") as f:
                            zip_data = f.read()
                        
                        st.success("Processing completed!")
                        st.download_button(
                            label="Download ZIP Archive",
                            data=zip_data,
                            file_name=os.path.basename(zip_path),
                            mime="application/zip"
                        )
                    else:
                        st.error("Failed to create output package")
                
                except Exception as e:
                    st.error(f"Processing failed: {str(e)}")
                finally:
                    st.subheader("Processing Log")
                    st.code("\n".join(log_lines))

# Flask implementation (legacy)
def create_flask_app():
    from flask import Flask, request, render_template_string, send_file
    from waitress import serve

    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-key-123")
    app.config['FILE_STORAGE'] = {}
    file_storage_lock = threading.Lock()

    # ... [Keep original Flask routes and template here] ...

    return app

if __name__ == "__main__":
    if "streamlit" in sys.modules:
        main()
    else:
        flask_app = create_flask_app()
        serve(flask_app, host="0.0.0.0", port=5000, threads=4)
