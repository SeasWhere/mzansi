# app.py
import os
import sys
import requests
import tempfile
import zipfile
from datetime import datetime
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Error: Install required packages: pip install beautifulsoup4")

# Configuration
HEADERS = {'User-Agent': 'SEC Filing Processor/1.0'}
DEFAULT_TIMEOUT = 15
session = requests.Session()
session.headers.update(HEADERS)

def get_filing_period(filing_date, fiscal_month, form):
    """Simplified period calculation without feature policies"""
    year = filing_date.year
    if filing_date.month <= fiscal_month:
        year -= 1
    return f"FY{year%100:02d}" if form == "10-K" else f"Q{(filing_date.month-1)//3+1}"

def process_document(url, output_dir):
    """Process individual SEC document without browser dependencies"""
    try:
        response = session.get(url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        base_name = os.path.basename(url).split('.')[0]
        html_path = os.path.join(output_dir, f"{base_name}.html")
        
        # Basic HTML cleanup
        for tag in soup(['script', 'link', 'img']):
            if tag.name == 'link' and tag.get('href', '').endswith('.css'):
                tag.decompose()
            elif tag.name in ['img', 'script']:
                tag.decompose()

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
            
        return html_path
    except Exception:
        return None

def fetch_filings(cik, tmp_dir):
    """Fetch and process filings with simplified logic"""
    cik = cik.zfill(10)
    try:
        response = session.get(
            f"https://data.sec.gov/submissions/CIK{cik}.json",
            timeout=DEFAULT_TIMEOUT
        )
        filings = response.json()['filings']['recent']
    except Exception:
        return []
    
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for idx in range(len(filings['accessionNumber'])):
            if filings['form'][idx] not in ['10-K', '10-Q']:
                continue
            
            try:
                url = urljoin(
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/",
                    f"{filings['accessionNumber'][idx].replace('-', '')}/"
                    f"{filings['primaryDocument'][idx]}"
                )
                futures.append(executor.submit(process_document, url, tmp_dir))
            except Exception:
                continue

        for future in as_completed(futures):
            if (path := future.result()):
                results.append(path)
    
    return results

# Streamlit UI Components
def main():
    import streamlit as st
    
    st.set_page_config(
        page_title="SEC Filing Processor",
        page_icon="📑",
        layout="centered",
        initial_sidebar_state="collapsed"
    )
    
    st.title("SEC Filing Processor")
    st.markdown("---")
    
    with st.form(key="main_form"):
        cik = st.text_input(
            "Company CIK Number",
            placeholder="Enter 10-digit CIK",
            help="Example: 0000320193 for Apple"
        )
        fiscal_month = st.selectbox(
            "Fiscal Year End Month",
            options=list(range(1, 13)),
            format_func=lambda x: datetime(2000, x, 1).strftime('%B')
        )
        submitted = st.form_submit_button("Process Filings")
    
    if submitted:
        if not cik.isdigit() or len(cik) != 10:
            st.error("Please enter a valid 10-digit CIK number")
            return
        
        with st.spinner("Fetching and processing SEC filings..."):
            with tempfile.TemporaryDirectory() as tmp_dir:
                try:
                    files = fetch_filings(cik, tmp_dir)
                    if not files:
                        st.warning("No filings found for this CIK")
                        return
                    
                    zip_buffer = BytesIO()
                    with zipfile.ZipFile(zip_buffer, 'w') as zf:
                        for file in files:
                            zf.write(file, arcname=os.path.basename(file))
                    
                    st.success("Processing completed!")
                    st.download_button(
                        label="Download Processed Filings",
                        data=zip_buffer.getvalue(),
                        file_name="sec_filings.zip",
                        mime="application/zip"
                    )
                except Exception as e:
                    st.error(f"Processing failed: {str(e)}")

if __name__ == "__main__":
    if "streamlit" in sys.modules:
        main()
    else:
        print("Run with: streamlit run app.py")
