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
CHROME_PATH = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}
session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 15
SEC_RATE_LIMIT = 10  # Seconds between SEC requests
last_request_time = 0
lock = threading.Lock()

# -------------------------
# Rate-Limited SEC Requests
# -------------------------
def sec_get(url, timeout=DEFAULT_TIMEOUT):
    """Enforce SEC's 10-second rate limit for all requests"""
    global last_request_time
    with lock:
        # Calculate required wait time
        elapsed = time.time() - last_request_time
        if elapsed < SEC_RATE_LIMIT:
            wait_time = SEC_RATE_LIMIT - elapsed
            time.sleep(wait_time)
        
        # Make request and update timer
        response = session.get(url, timeout=timeout)
        last_request_time = time.time()
        return response

# -------------------------
# Core Functions
# -------------------------
def get_chrome_path():
    import platform
    system = platform.system().lower()
    return CHROME_PATH.get(system)

def normalize_form(form_str):
    return form_str.split('/')[0].strip().upper()

def get_all_filings(submissions, cik_padded):
    filings = []
    try:
        # Get recent filings
        recent = submissions.get('filings', {}).get('recent', {})
        if recent:
            for i in range(len(recent.get('accessionNumber', []))):
                filing = {k: recent[k][i] for k in recent if isinstance(recent[k], list)}
                filings.append(filing)
        
        # Get historical filings with rate limiting
        for file_info in submissions.get('files', []):
            if file_info['name'].endswith('.json'):
                hist_url = f"https://data.sec.gov/submissions/{cik_padded}/{file_info['name']}"
                try:
                    response = sec_get(hist_url)
                    filings.extend(response.json().get('filings', []))
                except Exception:
                    continue
    except Exception as e:
        st.error(f"Filings processing error: {str(e)}")
    return filings

def download_assets(soup, base_url, output_dir, log_lines):
    downloaded_assets = []
    for tag in soup.find_all(['img', 'link', 'script']):
        url_attr = 'src' if tag.name in ['img', 'script'] else 'href'
        asset_url = tag.get(url_attr)
        if not asset_url:
            continue
        
        absolute_url = urljoin(base_url, asset_url)
        try:
            # Apply rate limiting for SEC assets
            if 'sec.gov' in absolute_url:
                response = sec_get(absolute_url)
            else:
                response = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
            
            response.raise_for_status()
            filename = os.path.basename(urlparse(absolute_url).path)
            if not filename:
                continue
            
            local_path = os.path.join(output_dir, filename)
            with open(local_path, 'wb') as f:
                f.write(response.content)
            tag[url_attr] = filename
            downloaded_assets.append(filename)
            log_lines.append(f"Downloaded: {filename}")
        except Exception as e:
            log_lines.append(f"Asset error: {str(e)}")
    return downloaded_assets

def convert_to_pdf(html_path, form, date, cik, ticker, log_lines):
    chrome_path = get_chrome_path()
    if not chrome_path or not os.path.exists(chrome_path):
        log_lines.append("Chrome not available")
        return None
    
    try:
        pdf_filename = f"{ticker or cik}_{form}_{date}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        cmd = [
            chrome_path,
            "--headless",
            "--disable-gpu",
            f"--print-to-pdf={pdf_path}",
            "--no-sandbox",
            f"file://{os.path.abspath(html_path)}"
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        return pdf_path
    except Exception as e:
        log_lines.append(f"PDF error: {str(e)}")
        return None

def process_document(doc_url, cik, form, date, ticker, cleanup_flag, log_lines, tmp_dir):
    try:
        log_lines.append(f"Starting: {form} {date}")
        
        # Fetch document with rate limiting
        response = sec_get(doc_url) if 'sec.gov' in doc_url else session.get(doc_url)
        response.raise_for_status()
        
        # Process HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        html_path = os.path.join(tmp_dir, f"{cik}_{form}_{date}.html")
        
        # Download assets
        assets = download_assets(soup, doc_url, tmp_dir, log_lines)
        
        # Save HTML
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
        
        # Convert to PDF
        pdf_path = convert_to_pdf(html_path, form, date, cik, ticker, log_lines)
        
        # Cleanup
        if cleanup_flag and pdf_path:
            for asset in assets:
                os.remove(os.path.join(tmp_dir, asset))
            os.remove(html_path)
        
        return pdf_path
    except Exception as e:
        log_lines.append(f"Failed: {str(e)}")
        return None

# -------------------------
# Streamlit Interface
# -------------------------
st.set_page_config(page_title="SEC Filer Pro", layout="wide")
st.title("SEC Filing Processor")
st.markdown("""
SEC-compliant filing processor with automatic rate limiting
""")

with st.expander("Configuration", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        cik = st.text_input("CIK Number:", placeholder="0000320193 (Apple)")
        ticker = st.text_input("Ticker (optional):")
    with col2:
        fy_month = st.selectbox("Fiscal Month:", 
                              options=list(range(1,13)),
                              format_func=lambda x: datetime(2000, x, 1).strftime('%B'),
                              index=11)
        cleanup = st.checkbox("Auto-clean temporary files", value=True)

if st.button("Process Filings", type="primary"):
    if not cik or not cik.isdigit():
        st.error("Invalid CIK format")
    else:
        with st.spinner("Processing (may take 2-5 minutes due to SEC limits)..."):
            log = []
            pdf_files = []
            cik_padded = cik.zfill(10)
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    # Initial SEC request with rate limiting
                    subs_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
                    response = sec_get(subs_url)
                    subs_data = response.json()
                    
                    # Get filings with rate limiting
                    filings = get_all_filings(subs_data, cik_padded)
                    valid_forms = {'10-K', '10-Q'}
                    
                    # Process filings with thread pool
                    with ThreadPoolExecutor(max_workers=2) as executor:  # Reduced workers
                        futures = []
                        for filing in filings[:15]:  # Increased limit with rate control
                            form = normalize_form(filing.get('form', ''))
                            if form not in valid_forms:
                                continue
                            
                            accession = filing.get('accessionNumber', '').replace('-', '')
                            filing_date = filing.get('filingDate', '')
                            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/{filing.get('primaryDocument', '')}"
                            
                            futures.append(executor.submit(
                                process_document,
                                doc_url, cik_padded, form, filing_date,
                                ticker, cleanup, log, tmp_dir
                            ))
                        
                        for future in as_completed(futures):
                            pdf_path = future.result()
                            if pdf_path and os.path.exists(pdf_path):
                                pdf_files.append(pdf_path)
                    
                    # Create ZIP package
                    if pdf_files:
                        zip_filename = f"{cik_padded}_filings.zip"
                        zip_path = os.path.join(tmp_dir, zip_filename)
                        with zipfile.ZipFile(zip_path, 'w') as zipf:
                            for pdf in pdf_files:
                                zipf.write(pdf, arcname=os.path.basename(pdf))
                        
                        with open(zip_path, "rb") as f:
                            st.success("Processing complete!")
                            st.download_button(
                                label="Download Package",
                                data=f,
                                file_name=zip_filename,
                                mime="application/zip"
                            )
                    else:
                        st.warning("No filings found. Possible issues:")
                        st.markdown("""
                        - Verify CIK is correct
                        - Check fiscal month selection
                        - Try again after 10 seconds
                        """)
                
                except Exception as e:
                    st.error(f"Critical error: {str(e)}")
                    log.append(f"ERROR: {str(e)}")
                
                # Show detailed log
                with st.expander("Technical Details"):
                    st.code("\n".join(log))

st.markdown("---")
st.caption("SEC data subject to rate limits (10 seconds between requests)")
