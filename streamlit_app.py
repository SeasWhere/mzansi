# streamlit_app.py
import os
import time
import requests
import subprocess
import tempfile
import zipfile
import threading
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from bs4 import BeautifulSoup

# -------------------------
# Configuration
# -------------------------
HEADERS = {'User-Agent': 'CompanyName AppName contact@company.com'}
CHROME_PATHS = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}
session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 15
SEC_RATE_LIMIT = 10
last_request_time = 0
lock = threading.Lock()

# -------------------------
# Chrome Handling
# -------------------------
def get_chrome_status():
    """Check Chrome availability and return (path, status_message)"""
    import platform
    system = platform.system().lower()
    chrome_path = CHROME_PATHS.get(system)
    
    if chrome_path and os.path.exists(chrome_path):
        return (chrome_path, "Chrome available for PDF conversion")
    return (None, "PDF conversion disabled (Chrome not found)")

# -------------------------
# Core Functions
# -------------------------
def sec_get(url, timeout=DEFAULT_TIMEOUT):
    global last_request_time
    with lock:
        elapsed = time.time() - last_request_time
        if elapsed < SEC_RATE_LIMIT:
            time.sleep(SEC_RATE_LIMIT - elapsed)
        response = session.get(url, timeout=timeout)
        last_request_time = time.time()
        return response

def process_document(doc_url, cik, form, date, ticker, log_lines, tmp_dir):
    result = {'html': None, 'pdf': None, 'assets': []}
    
    try:
        # Fetch document
        response = sec_get(doc_url) if 'sec.gov' in doc_url else session.get(doc_url)
        response.raise_for_status()
        
        # Process HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        html_filename = f"{ticker or cik}_{form}_{date}.html"
        html_path = os.path.join(tmp_dir, html_filename)
        
        # Save HTML
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
        result['html'] = html_path
        log_lines.append(f"Saved HTML: {html_filename}")
        
        # Download assets
        assets = []
        for tag in soup.find_all(['img', 'link', 'script']):
            url_attr = 'src' if tag.name in ['img', 'script'] else 'href'
            asset_url = tag.get(url_attr)
            if not asset_url:
                continue
            
            try:
                absolute_url = urljoin(doc_url, asset_url)
                if 'sec.gov' in absolute_url:
                    asset_response = sec_get(absolute_url)
                else:
                    asset_response = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                
                asset_filename = os.path.basename(urlparse(absolute_url).path)
                asset_path = os.path.join(tmp_dir, asset_filename)
                
                with open(asset_path, 'wb') as f:
                    f.write(asset_response.content)
                tag[url_attr] = asset_filename
                assets.append(asset_filename)
                log_lines.append(f"Downloaded asset: {asset_filename}")
            except Exception as e:
                log_lines.append(f"Asset error: {str(e)}")
        
        result['assets'] = assets
        
        # PDF Conversion (if Chrome available)
        chrome_path, _ = get_chrome_status()
        if chrome_path:
            try:
                pdf_filename = f"{ticker or cik}_{form}_{date}.pdf"
                pdf_path = os.path.join(tmp_dir, pdf_filename)
                cmd = [
                    chrome_path,
                    "--headless",
                    "--disable-gpu",
                    f"--print-to-pdf={pdf_path}",
                    "--no-sandbox",
                    f"file://{html_path}"
                ]
                subprocess.run(cmd, check=True, capture_output=True, timeout=30)
                result['pdf'] = pdf_path
                log_lines.append(f"Generated PDF: {pdf_filename}")
            except Exception as e:
                log_lines.append(f"PDF conversion failed: {str(e)}")
        
        return result
    
    except Exception as e:
        log_lines.append(f"Document processing failed: {str(e)}")
        return result

# -------------------------
# Streamlit Interface
# -------------------------
st.set_page_config(page_title="SEC Filer Pro", layout="wide")
st.title("SEC Filing Processor")

# Display Chrome status
chrome_path, chrome_status = get_chrome_status()
with st.expander("System Status", expanded=True):
    st.markdown(f"""
    - **PDF Conversion**: {chrome_status}
    - **SEC Rate Limit**: {SEC_RATE_LIMIT} seconds between requests
    """)

# User inputs
with st.expander("Configuration"):
    col1, col2 = st.columns(2)
    with col1:
        cik = st.text_input("CIK Number:", placeholder="0000320193 (Apple)")
        ticker = st.text_input("Ticker (optional):")
    with col2:
        fy_month = st.selectbox("Fiscal Month:", 
                               options=list(range(1,13)),
                               format_func=lambda x: datetime(2000, x, 1).strftime('%B'),
                               index=11)
        max_filings = st.slider("Max filings to process", 1, 20, 5)

if st.button("Process Filings", type="primary"):
    if not cik or not cik.isdigit():
        st.error("Invalid CIK format")
    else:
        with st.spinner(f"Processing up to {max_filings} filings..."):
            log = []
            results = []
            cik_padded = cik.zfill(10)
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    # Fetch company data
                    subs_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
                    response = sec_get(subs_url)
                    subs_data = response.json()
                    
                    # Process filings
                    filings = []
                    recent = subs_data.get('filings', {}).get('recent', {})
                    if recent:
                        for i in range(len(recent.get('accessionNumber', []))):
                            filings.append({k: recent[k][i] for k in recent if isinstance(recent[k], list)})
                    
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = []
                        for filing in filings[:max_filings]:
                            form = filing.get('form', '').split('/')[0].strip().upper()
                            if form not in {'10-K', '10-Q'}:
                                continue
                            
                            accession = filing.get('accessionNumber', '').replace('-', '')
                            filing_date = filing.get('filingDate', '')
                            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/{filing.get('primaryDocument', '')}"
                            
                            futures.append(executor.submit(
                                process_document,
                                doc_url, cik_padded, form, filing_date,
                                ticker, log, tmp_dir
                            ))
                        
                        for future in as_completed(futures):
                            results.append(future.result())
                    
                    # Create ZIP package
                    if results:
                        zip_filename = f"{cik_padded}_filings.zip"
                        zip_path = os.path.join(tmp_dir, zip_filename)
                        
                        with zipfile.ZipFile(zip_path, 'w') as zipf:
                            for result in results:
                                if result['html']:
                                    zipf.write(result['html'], arcname=os.path.basename(result['html']))
                                if result['pdf']:
                                    zipf.write(result['pdf'], arcname=os.path.basename(result['pdf']))
                                for asset in result['assets']:
                                    zipf.write(os.path.join(tmp_dir, asset), arcname=asset)
                        
                        with open(zip_path, "rb") as f:
                            st.success("Processing complete!")
                            st.download_button(
                                label="Download Package",
                                data=f,
                                file_name=zip_filename,
                                mime="application/zip"
                            )
                    else:
                        st.warning("No valid filings processed")
                
                except Exception as e:
                    st.error(f"Critical error: {str(e)}")
                    log.append(f"ERROR: {str(e)}")
                
                # Show logs
                with st.expander("Processing Log"):
                    st.code("\n".join(log))

st.markdown("---")
st.caption("Note: HTML files are always included. PDFs require Chrome availability.")
