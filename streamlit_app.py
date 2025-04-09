# streamlit_app.py
import os
import time
import requests
import subprocess
import tempfile
import zipfile
import threading
import platform
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from bs4 import BeautifulSoup

# -------------------------
# Configuration
# -------------------------
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
}

CHROME_PATHS = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 20
SEC_RATE_LIMIT = 10  # SEC's required 10-second delay between requests
last_request_time = 0
lock = threading.Lock()

# -------------------------
# Core Functions
# -------------------------
def enforce_sec_rate_limit():
    """Enforce SEC's 10-second rate limit between requests"""
    global last_request_time
    with lock:
        elapsed = time.time() - last_request_time
        if elapsed < SEC_RATE_LIMIT:
            wait_time = SEC_RATE_LIMIT - elapsed
            time.sleep(wait_time)
        last_request_time = time.time()

def get_chrome_status():
    """Check Chrome availability and return (path, available)"""
    system = platform.system().lower()
    chrome_path = CHROME_PATHS.get(system)
    if chrome_path and os.path.exists(chrome_path):
        return (chrome_path, True)
    return (None, False)

def fetch_sec_data(url):
    """Fetch data from SEC with rate limiting"""
    try:
        enforce_sec_rate_limit()
        response = session.get(url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"SEC API Error: {str(e)}")
        return None

def process_filings(cik_padded):
    """Retrieve and process all filings for a CIK"""
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    submissions = fetch_sec_data(submissions_url)
    if not submissions:
        return []
    
    filings = []
    # Process recent filings
    recent = submissions.get('filings', {}).get('recent', {})
    if recent:
        for i in range(len(recent.get('accessionNumber', []))):
            filing = {k: recent[k][i] for k in recent if isinstance(recent[k], list)}
            filings.append(filing)
    
    # Process historical filings
    for file_info in submissions.get('files', []):
        if file_info['name'].endswith('.json'):
            hist_url = f"https://data.sec.gov/submissions/{cik_padded}/{file_info['name']}"
            hist_data = fetch_sec_data(hist_url)
            if hist_data:
                filings.extend(hist_data.get('filings', []))
    
    return filings

def download_asset(asset_url, base_url, output_dir):
    """Download a single asset with SEC rate limiting"""
    try:
        absolute_url = urljoin(base_url, asset_url)
        if 'sec.gov' in absolute_url:
            enforce_sec_rate_limit()
        
        response = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        
        filename = os.path.basename(urlparse(absolute_url).path)
        if not filename:
            return None
        
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(response.content)
        return filename
    except Exception as e:
        return None

def convert_html_to_pdf(html_path, output_dir):
    """Convert HTML file to PDF using Chrome"""
    chrome_path, chrome_available = get_chrome_status()
    if not chrome_available:
        return None
    
    try:
        pdf_filename = os.path.basename(html_path).replace('.html', '.pdf')
        pdf_path = os.path.join(output_dir, pdf_filename)
        
        cmd = [
            chrome_path,
            "--headless",
            "--disable-gpu",
            f"--print-to-pdf={pdf_path}",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            f"file://{os.path.abspath(html_path)}"
        ]
        
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
        
        if os.path.exists(pdf_path):
            return pdf_path
        return None
    except Exception:
        return None

def process_filing(filing, cik_padded, ticker, output_dir):
    """Process a single filing"""
    result = {
        'html': None,
        'pdf': None,
        'assets': [],
        'error': None
    }
    
    try:
        form = filing.get('form', '')
        if not form or form.split('/')[0] not in ['10-K', '10-Q']:
            return result
        
        accession = filing.get('accessionNumber', '').replace('-', '')
        filing_date = filing.get('filingDate', '')
        primary_doc = filing.get('primaryDocument', '')
        
        if not all([accession, filing_date, primary_doc]):
            return result
        
        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/{primary_doc}"
        
        # Download main document
        enforce_sec_rate_limit()
        response = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        
        # Process HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Create HTML file
        html_filename = f"{ticker or cik_padded}_{form}_{filing_date}.html"
        html_path = os.path.join(output_dir, html_filename)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
        result['html'] = html_path
        
        # Download assets
        assets = []
        for tag in soup.find_all(['img', 'link', 'script']):
            url_attr = 'src' if tag.name in ['img', 'script'] else 'href'
            asset_url = tag.get(url_attr)
            if not asset_url:
                continue
            
            asset_filename = download_asset(asset_url, doc_url, output_dir)
            if asset_filename:
                assets.append(asset_filename)
                tag[url_attr] = asset_filename
        
        result['assets'] = assets
        
        # Update HTML with local asset paths
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
        
        # Convert to PDF
        pdf_path = convert_html_to_pdf(html_path, output_dir)
        result['pdf'] = pdf_path
        
        return result
    except Exception as e:
        result['error'] = str(e)
        return result

# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(
    page_title="SEC Filing Processor",
    page_icon="📈",
    layout="wide"
)

st.title("SEC Filing Processor")
st.markdown("""
Fetch and convert SEC filings to PDF with automatic rate limiting and Chrome fallback handling.
""")

# System status
chrome_path, chrome_available = get_chrome_status()
with st.expander("System Status", expanded=True):
    st.markdown(f"""
    - **PDF Conversion**: {'Available' if chrome_available else 'Disabled (Chrome not found)'}
    - **SEC Rate Limit**: {SEC_RATE_LIMIT} seconds between requests
    - **Max Workers**: 2 concurrent processes
    """)

# User inputs
with st.form("filing_config"):
    col1, col2 = st.columns(2)
    with col1:
        cik = st.text_input("CIK Number:", placeholder="0000320193 (Apple)")
        ticker = st.text_input("Ticker (optional):")
    with col2:
        max_filings = st.slider("Maximum filings to process", 1, 20, 5)
        fy_month = st.selectbox(
            "Fiscal Year End Month:",
            options=list(range(1, 13)),
            format_func=lambda x: datetime(2000, x, 1).strftime('%B'),
            index=11
        )
    
    submitted = st.form_submit_button("Process Filings")

if submitted:
    if not cik or not cik.isdigit():
        st.error("Please enter a valid 10-digit CIK number")
    else:
        with st.spinner(f"Processing up to {max_filings} filings (this may take several minutes)..."):
            cik_padded = cik.strip().zfill(10)
            process_log = []
            results = []
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    # Retrieve and process filings
                    filings = process_filings(cik_padded)
                    process_log.append(f"Found {len(filings)} potential filings")
                    
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = []
                        for filing in filings[:max_filings]:
                            futures.append(executor.submit(
                                process_filing,
                                filing,
                                cik_padded,
                                ticker.strip(),
                                tmp_dir
                            ))
                        
                        for future in as_completed(futures):
                            result = future.result()
                            results.append(result)
                            if result['error']:
                                process_log.append(f"Error: {result['error']}")
                            else:
                                process_log.append(f"Processed {os.path.basename(result['html'])}")
                    
                    # Create ZIP archive
                    zip_filename = f"{cik_padded}_filings.zip"
                    zip_path = os.path.join(tmp_dir, zip_filename)
                    
                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for result in results:
                            if result['html']:
                                zipf.write(
                                    result['html'],
                                    arcname=os.path.basename(result['html'])
                                )
                            if result['pdf']:
                                zipf.write(
                                    result['pdf'],
                                    arcname=os.path.basename(result['pdf'])
                                )
                            for asset in result['assets']:
                                zipf.write(
                                    os.path.join(tmp_dir, asset),
                                    arcname=asset
                                )
                    
                    # Display results
                    st.success(f"Processed {len([r for r in results if r['html']])} filings successfully!")
                    
                    with open(zip_path, "rb") as f:
                        st.download_button(
                            label="Download All Filings",
                            data=f,
                            file_name=zip_filename,
                            mime="application/zip"
                        )
                
                except Exception as e:
                    st.error(f"Critical error: {str(e)}")
                    process_log.append(f"CRITICAL ERROR: {str(e)}")
                
                # Show process log
                with st.expander("Processing Details"):
                    st.code("\n".join(process_log))

st.markdown("---")
st.caption("""
Note: Processing times vary based on SEC rate limits. 
HTML files are always included; PDFs require Chrome/Chromium installation.
""")
