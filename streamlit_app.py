# streamlit_app.py
import os
import sys
import requests
import tempfile
import zipfile
import time
import mimetypes
import traceback
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st

# Ensure necessary libraries are installed
try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    st.error("Please install beautifulsoup4: pip install beautifulsoup4")
    st.stop()

try:
    import xhtml2pdf.pisa as pisa
except ModuleNotFoundError:
    st.error("Please install xhtml2pdf: pip install xhtml2pdf")
    st.stop()

# -------------------------
# Global configuration
# -------------------------
HEADERS = {
    'User-Agent': 'Mzansi EDGAR Viewer v1.3 (support@example.com)'
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 20

EARLIEST_FISCAL_YEAR_SUFFIX = 17

# -------------------------
# Backend Functions
# -------------------------

def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
    try:
        fiscal_year_end_month = int(fiscal_year_end_month)
        if not 1 <= fiscal_year_end_month <= 12:
            raise ValueError("Month must be between 1 and 12")
    except (ValueError, TypeError):
        fiscal_year_end_month = 12

    if fiscal_year_end_month != 12:
        reported_year = filing_date.year if filing_date.month > fiscal_year_end_month else filing_date.year - 1
        if fy_adjust == "Previous Year":
            reported_year -= 1

        if form == "10-K":
            return f"FY{reported_year % 100:02d}"
        elif form == "10-Q":
            months_since_fye_start = (filing_date.month - fiscal_year_end_month - 1 + 12) % 12
            quarter = (months_since_fye_start // 3) + 1
            q_year = reported_year if filing_date.month <= fiscal_year_end_month else reported_year + 1
            return f"{quarter}Q{q_year % 100:02d}"
        else:
            return f"FY{reported_year % 100:02d}"
    else:
        if form == "10-K":
            fiscal_year = filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"
        elif form == "10-Q":
            month = filing_date.month
            if 1 <= month <= 3: quarter = 4; report_year = filing_date.year - 1
            elif 4 <= month <= 6: quarter = 1; report_year = filing_date.year
            elif 7 <= month <= 9: quarter = 2; report_year = filing_date.year
            elif 10 <= month <= 12: quarter = 3; report_year = filing_date.year
            else:
                return f"Q?{(filing_date.year -1) % 100:02d}"
            fiscal_year = report_year
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"{quarter}Q{fiscal_year % 100:02d}"
        else:
            fiscal_year = filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"

def download_assets(soup, base_url, output_dir, log_lines):
    downloaded_assets_filenames = set()
    processed_urls = set()

    tags_and_attrs = [('img', 'src'), ('link', 'href'), ('script', 'src')]

    for tag_name, url_attr in tags_and_attrs:
        for tag in soup.find_all(tag_name):
            if tag_name == 'link' and tag.get('rel') != ['stylesheet']:
                continue

            asset_url = tag.get(url_attr)
            if not asset_url or asset_url.startswith(('data:', 'javascript:')):
                continue

            try:
                absolute_url = urljoin(base_url, asset_url)
                parsed_url = urlparse(absolute_url)
            except ValueError:
                log_lines.append(f"Skipping invalid asset URL: {asset_url}")
                continue

            if parsed_url.scheme not in ['http', 'https'] or absolute_url in processed_urls:
                continue

            processed_urls.add(absolute_url)

            base_url_parsed = urlparse(base_url)
            asset_path = parsed_url.path
            
            base_path = base_url_parsed.path.rstrip('/') + '/'
            if not asset_path.startswith(base_path):
                log_lines.append(f"Skipping external asset: {absolute_url}")
                continue

            relative_path = asset_path[len(base_path):]

            sanitized_parts = []
            for part in relative_path.split('/'):
                if not part:
                    continue
                sanitized = "".join([c if c.isalnum() or c in ('.', '_', '-') else '_' for c in part])
                sanitized = sanitized.strip('._')
                if not sanitized:
                    sanitized = f"part{len(sanitized_parts)+1}"
                sanitized_parts.append(sanitized)

            if not sanitized_parts:
                log_lines.append(f"Skipping asset with empty path: {absolute_url}")
                continue

            sanitized_relative_path = os.path.join(*sanitized_parts)
            local_path = os.path.join(output_dir, sanitized_relative_path)

            try:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                if not os.path.exists(local_path):
                    time.sleep(0.11)
                    r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                    r.raise_for_status()

                    content_type = r.headers.get('content-type')
                    guessed_ext = mimetypes.guess_extension(content_type.split(';')[0]) if content_type else None
                    
                    if guessed_ext:
                        base, _ = os.path.splitext(sanitized_relative_path)
                        new_path = base + guessed_ext
                        if not os.path.exists(os.path.join(output_dir, new_path)):
                            sanitized_relative_path = new_path
                            local_path = os.path.join(output_dir, new_path)

                    with open(local_path, 'wb') as f:
                        f.write(r.content)

                tag[url_attr] = sanitized_relative_path
                downloaded_assets_filenames.add(sanitized_relative_path)

            except Exception as e:
                log_lines.append(f"Error processing {absolute_url}: {str(e)}")

    if downloaded_assets_filenames:
        log_lines.append(f"Processed {len(downloaded_assets_filenames)} assets")
    return list(downloaded_assets_filenames)

def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    pdf_path = None
    try:
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, fy_month_idx, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name).strip('._')
        if not safe_base_name: safe_base_name = f"{cik}_{accession}"
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        log_lines.append(f"Attempting PDF conversion: {pdf_filename}")

        with open(html_path, "r", encoding="utf-8") as source_html_file, \
             open(pdf_path, "w+b") as result_file:

            base_dir = os.path.dirname(html_path)
            def link_callback(uri, rel):
                potential_local_path = os.path.abspath(os.path.join(base_dir, uri))
                if potential_local_path.startswith(os.path.abspath(base_dir)):
                    if os.path.exists(potential_local_path):
                        return potential_local_path
                if urlparse(uri).scheme in ['http', 'https']:
                     log_lines.append(f"PDF Conversion: Passing web link: {uri}")
                     return uri
                return uri

            pisa_status = pisa.CreatePDF(
                src=source_html_file,
                dest=result_file,
                encoding='utf-8',
                link_callback=link_callback
            )

        if pisa_status.err:
            log_lines.append(f"ERROR: PDF conversion failed: {pisa_status.err}")
            if os.path.exists(pdf_path):
                try: os.remove(pdf_path)
                except OSError as e: log_lines.append(f"Cleanup error: {e}")
            return None
        elif os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
            log_lines.append(f"PDF created successfully: {pdf_filename}")
            return pdf_path
        else:
            log_lines.append(f"ERROR: Empty PDF file: {pdf_filename}")
            if os.path.exists(pdf_path):
                 try: os.remove(pdf_path)
                 except OSError: pass
            return None

    except Exception as e:
        log_lines.append(f"ERROR: PDF conversion error: {str(e)}")
        if pdf_path and os.path.exists(pdf_path):
            try: os.remove(pdf_path)
            except OSError as e: log_lines.append(f"Cleanup error: {e}")
        return None

def cleanup_files(html_path, assets, output_dir, log_lines):
    cleaned_count = 0
    try:
        if html_path and os.path.exists(html_path):
            os.remove(html_path)
            cleaned_count += 1

        for asset_filename in assets:
            asset_path = os.path.join(output_dir, asset_filename)
            if os.path.exists(asset_path):
                try:
                    os.remove(asset_path)
                    cleaned_count += 1
                except OSError as e:
                     log_lines.append(f"Cleanup error: {e}")

        if cleaned_count > 0:
             log_lines.append(f"Cleaned {cleaned_count} files")

    except Exception as e:
        log_lines.append(f"Cleanup error: {str(e)}")

def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, output_dir):
    html_path = None
    downloaded_assets = []
    pdf_path = None

    try:
        log_lines.append(f"Processing {form} {accession}...")

        time.sleep(0.11)
        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()

        base_html_filename = f"{cik}_{form}_{date}_{accession}.htm"
        html_path = os.path.join(output_dir, base_html_filename)

        try:
            decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                decoded_text = r.content.decode('latin-1')
            except UnicodeDecodeError:
                decoded_text = r.content.decode('utf-8', errors='replace')

        replacements = {
            "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—",
            "&nbsp;": " ", "\u00a0": " "
        }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)

        soup = BeautifulSoup(decoded_text, 'html.parser')

        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            head = soup.head
            if not head:
                 head = soup.new_tag('head')
                 if soup.html: soup.html.insert(0, head)
                 else: soup.insert(0, head)
            head.insert(0, meta_tag)

        doc_base_url = urljoin(doc_url, '.')
        downloaded_assets = download_assets(soup, doc_base_url, output_dir, log_lines)

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))

        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)

        return (form, pdf_path)

    except Exception as e:
        log_lines.append(f"Processing error: {str(e)}")
        log_lines.append(traceback.format_exc())
        return (form, None)
    finally:
        if cleanup_flag and pdf_path:
            cleanup_files(html_path, downloaded_assets, output_dir, log_lines)

def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    pdf_files = {"10-K": [], "10-Q": []}

    if not cik.isdigit():
        log_lines.append(f"Invalid CIK: {cik}")
        return pdf_files

    cik_padded = cik.zfill(10)
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"

    try:
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        submissions = r.json()

        filings_data = submissions.get('filings', {}).get('recent', {})
        accession_numbers = filings_data.get('accessionNumber', [])
        forms = filings_data.get('form', [])
        filing_dates = filings_data.get('filingDate', [])
        primary_documents = filings_data.get('primaryDocument', [])

        tasks_to_submit = []
        for i in range(len(accession_numbers)):
            form = forms[i]
            if form not in ["10-K", "10-Q"]:
                continue

            try:
                filing_date = datetime.strptime(filing_dates[i], "%Y-%m-%d")
                period = get_filing_period(form, filing_date, fy_month, fy_adjust)
                year_suffix = int(period.split("Q")[-1]) if "Q" in period else int(period[2:])

                if year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX:
                    break

                accession_clean = accession_numbers[i].replace('-', '')
                doc_url = f"{archive_base_url}{accession_clean}/{primary_documents[i]}"
                
                tasks_to_submit.append({
                    "doc_url": doc_url, "cik": cik_padded, "form": form,
                    "date": filing_dates[i], "accession": accession_clean,
                    "ticker": ticker, "fy_month": fy_month,
                    "fy_adjust": fy_adjust, "cleanup_flag": cleanup_flag,
                    "output_dir": tmp_dir
                })

            except Exception as e:
                log_lines.append(f"Skipping filing {accession_numbers[i]}: {str(e)}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(download_and_process, **task): task for task in tasks_to_submit}
            for future in as_completed(futures):
                try:
                    form_type, pdf_path = future.result()
                    if pdf_path:
                        pdf_files[form_type].append(pdf_path)
                except Exception as e:
                    log_lines.append(f"Processing failed: {str(e)}")

    except Exception as e:
        log_lines.append(f"Fatal error: {str(e)}")

    return pdf_files

def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs:
        return None

    zip_filename = f"{cik}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)

    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for form_type, paths in pdf_files.items():
                if not paths: continue
                for pdf_path in paths:
                    if pdf_path and os.path.exists(pdf_path):
                        arcname = os.path.join(form_type, os.path.basename(pdf_path))
                        zipf.write(pdf_path, arcname=arcname)
                        added_count += 1

        return zip_path if added_count > 0 else None

    except Exception as e:
        log_lines.append(f"ZIP creation failed: {str(e)}")
        if os.path.exists(zip_path):
            try: os.remove(zip_path)
            except OSError: pass
        return None

# -------------------------
# Streamlit UI (Unchanged)
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("📈 Mzansi EDGAR Fetcher")
st.write(f"Fetch SEC 10-K and 10-Q filings (FY{EARLIEST_FISCAL_YEAR_SUFFIX} onwards)")

with st.form("filing_form"):
    col1, col2 = st.columns(2)
    with col1:
        cik_input = st.text_input("Company CIK:", key="cik")
        ticker_input = st.text_input("Ticker (Optional):", key="ticker")
    with col2:
        month_options = {str(i): datetime(2000, i, 1).strftime('%B') for i in range(1, 13)}
        fy_month_input = st.selectbox(
            "Fiscal Year-End Month:",
            options=list(month_options.keys()),
            format_func=lambda x: f"{month_options[x]} ({x})",
            index=11,
            key="fy_month"
        )
        fy_adjust_input = st.selectbox(
            "Fiscal Year Basis:",
            ["Same Year", "Previous Year"],
            index=0,
            key="fy_adjust"
            )

    cleanup_flag_input = st.checkbox(
        "Cleanup intermediate files",
        value=False,
        key="cleanup"
        )

    submitted
