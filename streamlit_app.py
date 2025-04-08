# streamlit_app.py
import os
import requests
import subprocess
import tempfile
import zipfile
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from bs4 import BeautifulSoup

# -------------------------
# Configuration
# -------------------------
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
CHROME_PATH = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}
session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 15

# -------------------------
# Core Functions
# -------------------------
def get_chrome_path():
    import platform
    system = platform.system().lower()
    return CHROME_PATH.get(system)

def normalize_form(form_str):
    return form_str.split('/')[0].strip().upper()

def get_all_filings(submissions):
    filings = []
    recent = submissions.get('filings', {}).get('recent', {})
    if recent:
        for i in range(len(recent.get('accessionNumber', []))):
            filing = {k: recent[k][i] for k in recent if isinstance(recent[k], list)}
            filings.append(filing)
    for file_info in submissions.get('files', []):
        if file_info['name'].endswith('.json'):
            try:
                hist_url = f"https://data.sec.gov{submissions['cik']}/{file_info['name']}"
                response = session.get(hist_url, timeout=DEFAULT_TIMEOUT)
                filings.extend(response.json().get('filings', []))
            except Exception:
                continue
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

def convert_to_pdf(html_path, form, date, cik, ticker, log_lines):
    chrome_path = get_chrome_path()
    if not chrome_path or not os.path.exists(chrome_path):
        log_lines.append("Chrome not found - PDF conversion skipped")
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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if os.path.exists(pdf_path):
            log_lines.append(f"PDF created: {pdf_filename}")
            return pdf_path
        log_lines.append(f"PDF failed: {result.stderr}")
        return None
    except Exception as e:
        log_lines.append(f"Conversion error: {str(e)}")
        return None

def process_document(doc_url, cik, form, date, accession, ticker, cleanup_flag, log_lines, tmp_dir):
    try:
        log_lines.append(f"Processing {form} - {accession}")
        response = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        
        # Process HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        html_filename = f"{cik}_{form}_{date}.html"
        html_path = os.path.join(tmp_dir, html_filename)
        
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
        log_lines.append(f"Processing failed: {str(e)}")
        return None

def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    zip_filename = f"{cik}_filings.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    try:
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for pdf in pdf_files:
                if pdf and os.path.exists(pdf):
                    zipf.write(pdf, arcname=os.path.basename(pdf))
        return zip_path
    except Exception as e:
        log_lines.append(f"ZIP error: {str(e)}")
        return None

# -------------------------
# Streamlit Interface
# -------------------------
st.set_page_config(page_title="SEC Filer", page_icon="📈", layout="wide")
st.title("SEC Filing Processor")
st.markdown("""
Download SEC filings as PDF packages with automatic asset handling.
""")

with st.expander("Configuration", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        cik = st.text_input("CIK Number:", placeholder="0000320193 (Apple)", help="10-digit SEC company identifier")
        ticker = st.text_input("Ticker (optional):", placeholder="AAPL")
    with col2:
        fy_month = st.selectbox("Fiscal Year End:", 
                               options=list(range(1,13)),
                               format_func=lambda x: datetime(2000, x, 1).strftime('%B'))
        cleanup = st.checkbox("Clean temporary files", value=True)

if st.button("Process Filings", type="primary"):
    if not cik or not cik.isdigit():
        st.error("Please enter a valid 10-digit CIK number")
    else:
        with st.spinner("Processing filings..."):
            log = []
            pdf_files = []
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    # Retrieve company data
                    cik_padded = cik.zfill(10)
                    subs_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
                    response = session.get(subs_url, timeout=DEFAULT_TIMEOUT)
                    subs_data = response.json()
                    
                    # Process filings
                    filings = get_all_filings(subs_data)
                    valid_forms = {'10-K', '10-Q'}
                    
                    with ThreadPoolExecutor(max_workers=4) as executor:
                        futures = []
                        for filing in filings[:10]:  # Limit to 10 filings for demo
                            form = normalize_form(filing.get('form', ''))
                            if form not in valid_forms:
                                continue
                            
                            accession = filing.get('accessionNumber', '').replace('-', '')
                            filing_date = filing.get('filingDate', '')
                            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession}/{filing.get('primaryDocument', '')}"
                            
                            futures.append(executor.submit(
                                process_document,
                                doc_url, cik_padded, form, filing_date,
                                accession, ticker, cleanup, log, tmp_dir
                            ))
                        
                        for future in as_completed(futures):
                            result = future.result()
                            if result:
                                pdf_files.append(result)
                    
                    # Create ZIP package
                    if pdf_files:
                        zip_path = create_zip_archive(pdf_files, cik_padded, log, tmp_dir)
                        if zip_path:
                            with open(zip_path, "rb") as f:
                                st.success("Processing complete!")
                                st.download_button(
                                    label="Download Filings Package",
                                    data=f,
                                    file_name=f"{cik_padded}_filings.zip",
                                    mime="application/zip"
                                )
                    else:
                        st.warning("No valid filings found. Possible issues:")
                        st.markdown("""
                        - Incorrect CIK
                        - No 10-K/10-Q filings available
                        - SEC API limitations (wait 10 seconds and retry)
                        """)
                
                except Exception as e:
                    st.error(f"Critical error: {str(e)}")
                    log.append(f"CRITICAL ERROR: {str(e)}")
                
                # Show process log
                with st.expander("Process Details"):
                    st.code("\n".join(log))

st.markdown("---")
st.caption("Note: SEC data access is rate limited. Multiple requests may require waiting between attempts.")
