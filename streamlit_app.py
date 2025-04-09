import os
import sys
import requests
import tempfile
import zipfile
import time
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from fpdf import FPDF

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    st.error("Please install beautifulsoup4: pip install beautifulsoup4")
    st.stop()

# -------------------------
# Global configuration
# -------------------------
HEADERS = {
    'User-Agent': 'Mzansi EDGAR Viewer (support@example.com)'
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 10  # seconds

# Make sure this .ttf file is in the same directory.
FONT_PATH = os.path.join(os.path.dirname(__file__), "DejaVuSans.ttf")

# -------------------------
# Helper Function for API Retries
# -------------------------
def fetch_json_with_retries(url, max_retries=3, delay=10):
    """
    Attempts to fetch a JSON object from the provided URL with retries
    if SEC API limitations occur.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            err_msg = str(e)
            st.write(f"Attempt {attempt+1} failed: {err_msg}")
            if "404" in err_msg or "SEC API limitations" in err_msg:
                st.write(f"SEC API limitation encountered. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
                attempt += 1
            else:
                raise e
        except Exception as ex:
            st.write(f"Unexpected error: {ex}")
            raise ex
    raise Exception("Failed to fetch JSON from SEC API after several retries.")

# -------------------------
# Backend Functions
# -------------------------
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
                    quarter = 4; year = reported_year
                elif 7 <= filing_date.month <= 9:
                    quarter = 1; year = reported_year + 1
                elif 10 <= filing_date.month <= 12:
                    quarter = 2; year = reported_year + 1
                elif 1 <= filing_date.month <= 3:
                    quarter = 3; year = reported_year + 1
                return f"{quarter}Q{year % 100:02d}"
            else:
                quarter = ((filing_date.month - fiscal_year_end_month - 1) % 12) // 3 + 1
                year = reported_year + (filing_date.month < fiscal_year_end_month)
                return f"{quarter}Q{year % 100:02d}"
        else:
            return f"FY{reported_year % 100:02d}"
    else:
        if form == "10-K":
            fiscal_year = filing_date.year if filing_date.month > 3 else filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"
        elif form == "10-Q":
            month = filing_date.month
            if month in [1, 2, 3]:
                fiscal_year = filing_date.year - 1; quarter = 4
            elif month in [4, 5, 6]:
                fiscal_year = filing_date.year; quarter = 1
            elif month in [7, 8, 9]:
                fiscal_year = filing_date.year; quarter = 2
            elif month in [10, 11, 12]:
                fiscal_year = filing_date.year; quarter = 3
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
    """
    Uses fpdf with a Unicode TrueType font (DejaVuSans) to convert HTML content to PDF.
    The HTML is read and its visible text is extracted and written into the PDF.
    """
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, "html.parser")
        text_content = soup.get_text(separator="\n")
        
        class CustomPDF(FPDF):
            def header(self):
                # Add a Unicode font supporting UTF-8; ensure DejaVuSans.ttf is in the same folder.
                self.add_font("DejaVu", "", FONT_PATH, uni=True)
                self.set_font("DejaVu", "", 12)
                title = f"{ticker or cik} - {form} Filing"
                self.cell(0, 10, title, ln=True, align="C")
                self.ln(5)
        
        pdf = CustomPDF()
        pdf.add_page()
        # Set the font to our Unicode font
        pdf.add_font("DejaVu", "", FONT_PATH, uni=True)
        pdf.set_font("DejaVu", "", 10)
        
        # Write text line-by-line
        for line in text_content.splitlines():
            if line.strip():
                pdf.multi_cell(0, 5, line.strip())
        
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, int(fy_month_idx), fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        pdf_filename = f"{base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        pdf.output(pdf_path)
        log_lines.append("PDF created successfully using fpdf with UTF-8 support")
        return pdf_path
    except Exception as e:
        log_lines.append(f"FPDF conversion error: {str(e)}")
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
            "â€": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "-"
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
        submissions = fetch_json_with_retries(submissions_url, max_retries=3, delay=10)
    except Exception as e:
        log_lines.append(f"Connection error: {str(e)}")
        return pdf_files
    try:
        filings = submissions['filings']['recent']
    except KeyError as e:
        log_lines.append(f"Data format error: {str(e)}")
        return pdf_files
    fiscal_year_end_month = int(fy_month)
    valid_forms = ['10-K','10-Q']
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
                if form == "10-K":
                    if period.startswith("FY"):
                        year = int(period[2:])
                        if year < 17:
                            break
                    else:
                        continue
                elif form == "10-Q":
                    if "Q" in period:
                        year = int(period.split("Q")[-1])
                        if year <= 17:
                            break
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
                folder = form
                for pdf_file in pdf_files[form]:
                    arcname = os.path.join(folder, os.path.basename(pdf_file))
                    zipf.write(pdf_file, arcname=arcname)
                    log_lines.append(f"Added {os.path.basename(pdf_file)} to {folder}/")
        log_lines.append("ZIP archive created successfully")
        return zip_path
    except Exception as e:
        log_lines.append(f"ZIP archive creation failed: {str(e)}")
        return None

# -------------------------
# Streamlit UI
# -------------------------
st.title("Mzansi EDGAR Fetcher")
st.write("Fetch SEC filings as PDFs and download them as a ZIP archive.")

# Input widgets
cik_input = st.text_input("Company CIK (numbers only):")
ticker_input = st.text_input("Ticker (optional):")
fy_month_input = st.selectbox(
    "Fiscal Year-End Month:",
    [str(i) for i in range(1, 13)],
    format_func=lambda x: datetime(2000, int(x), 1).strftime('%B')
)
fy_adjust_input = st.selectbox("Fiscal Year Basis:", ["Same Year", "Previous Year"])
cleanup_flag_input = st.checkbox("Delete HTML and assets after PDF conversion", value=False)

if st.button("Fetch Filing"):
    if not cik_input.strip().isdigit():
        st.error("CIK must be numeric.")
    else:
        # Changed message here:
        st.info("Processing filing... This may take several minutes.")
        process_log = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            pdf_files = process_filing(
                cik_input.strip(),
                ticker_input.strip(),
                fy_month_input,
                fy_adjust_input,
                cleanup_flag_input,
                process_log,
                tmp_dir
            )
            if not any(pdf_files.values()):
                st.error("No valid filings found for the given criteria.")
            else:
                zip_path = create_zip_archive(
                    pdf_files,
                    cik_input.strip().zfill(10),
                    process_log,
                    tmp_dir
                )
                if not zip_path:
                    st.error("Failed to create ZIP archive.")
                else:
                    with open(zip_path, "rb") as f:
                        zip_data = f.read()
                    st.success("ZIP archive created successfully!")
                    st.download_button(
                        label="Download Filings ZIP",
                        data=zip_data,
                        file_name=os.path.basename(zip_path),
                        mime="application/zip"
                    )
        st.subheader("Process Log")
        st.text("\n".join(process_log))
