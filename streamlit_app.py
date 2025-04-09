import os
import sys
import requests
import tempfile
import zipfile
import time
import mimetypes  # For guessing asset types
import traceback  # For detailed error logging
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
    # User agent includes contact info as requested by SEC
    'User-Agent': 'Mzansi EDGAR Viewer v1.2 (support@example.com)'
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 20  # Increased timeout for larger filings/assets

# --- Scope Control ---
# Fiscal Year cutoff: Process filings from this year onwards.
# Filings *before* this fiscal year suffix will be skipped.
EARLIEST_FISCAL_YEAR_SUFFIX = 17

# -------------------------
# Backend Functions
# -------------------------

def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
    """
    Determines the fiscal period string (e.g., FY23, 1Q24) based on filing date and fiscal year end.
    Handles December and non-December fiscal year ends.
    """
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
            if 1 <= month <= 3:
                quarter = 4; report_year = filing_date.year - 1
            elif 4 <= month <= 6:
                quarter = 1; report_year = filing_date.year
            elif 7 <= month <= 9:
                quarter = 2; report_year = filing_date.year
            elif 10 <= month <= 12:
                quarter = 3; report_year = filing_date.year
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
    """
    Downloads assets (images, CSS, etc.) and updates asset links in the HTML.
    """
    downloaded_assets_filenames = set()
    processed_urls = set()

    tags_and_attrs = [('img', 'src'), ('link', 'href'), ('script', 'src')]

    for tag_name, url_attr in tags_and_attrs:
        for tag in soup.find_all(tag_name):
            if tag_name == 'link' and tag.get('rel') != ['stylesheet']:
                continue

            asset_url = tag.get(url_attr)
            if not asset_url:
                continue

            if asset_url.startswith(('data:', 'javascript:')):
                continue

            try:
                absolute_url = urljoin(base_url, asset_url)
                parsed_url = urlparse(absolute_url)
            except ValueError:
                log_lines.append(f"Skipping invalid asset URL format: {asset_url}")
                continue

            if parsed_url.scheme not in ['http', 'https']:
                continue
            if absolute_url in processed_urls:
                continue
            processed_urls.add(absolute_url)

            try:
                path_part = parsed_url.path
                filename_base = os.path.basename(path_part) or f"asset_{len(downloaded_assets_filenames)+1}"
                safe_filename = "".join(c if c.isalnum() or c in ['.', '_', '-'] else '_' for c in filename_base)[:100].strip('._')
                if not safe_filename:
                    safe_filename = f"asset_{len(downloaded_assets_filenames)+1}"
                _, ext = os.path.splitext(safe_filename)
                if not ext:
                    safe_filename += ".asset"

                local_path = os.path.join(output_dir, safe_filename)

                if not os.path.exists(local_path):
                    time.sleep(0.11)  # Respect rate limits
                    r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                    r.raise_for_status()
                    content_type = r.headers.get('content-type')
                    guessed_ext = None
                    if content_type:
                        guessed_ext = mimetypes.guess_extension(content_type.split(';')[0])
                    if guessed_ext and guessed_ext != ".asset" and not safe_filename.endswith(guessed_ext):
                        base, _ = os.path.splitext(safe_filename)
                        new_safe_filename = base + guessed_ext
                        new_local_path = os.path.join(output_dir, new_safe_filename)
                        if not os.path.exists(new_local_path):
                            safe_filename = new_safe_filename
                            local_path = new_local_path
                    with open(local_path, 'wb') as f:
                        f.write(r.content)
                tag[url_attr] = safe_filename
                downloaded_assets_filenames.add(safe_filename)
            except Exception as e:
                log_lines.append(f"Asset download error for {absolute_url}: {str(e)}")
    if downloaded_assets_filenames:
        log_lines.append(f"Processed {len(downloaded_assets_filenames)} unique asset file(s).")
    return list(downloaded_assets_filenames)


def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    """
    Converts the HTML (with its asset links updated) to PDF using xhtml2pdf.
    Uses a link_callback to resolve local asset paths.
    """
    pdf_path = None
    try:
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, fy_month_idx, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name).strip('._')
        if not safe_base_name:
            safe_base_name = f"{cik}_{accession}"
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        log_lines.append(f"Attempting PDF conversion: {pdf_filename}")

        with open(html_path, "r", encoding="utf-8") as source_html_file, \
             open(pdf_path, "w+b") as result_file:

            base_dir = os.path.dirname(html_path)
            def link_callback(uri, rel):
                potential_local_path = os.path.abspath(os.path.join(base_dir, uri))
                if potential_local_path.startswith(os.path.abspath(base_dir)) and os.path.exists(potential_local_path):
                    return potential_local_path
                if urlparse(uri).scheme in ['http', 'https']:
                    log_lines.append(f"Passing web link to xhtml2pdf: {uri}")
                    return uri
                return uri

            pisa_status = pisa.CreatePDF(
                src=source_html_file,
                dest=result_file,
                encoding='utf-8',
                link_callback=link_callback
            )

        if pisa_status.err:
            log_lines.append(f"ERROR: xhtml2pdf conversion failed for {pdf_filename}. Error code: {pisa_status.err}")
            if os.path.exists(pdf_path):
                try: os.remove(pdf_path)
                except OSError as e: log_lines.append(f"Could not remove failed PDF: {e}")
            return None
        elif os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
            log_lines.append(f"PDF created successfully: {pdf_filename}")
            return pdf_path
        else:
            log_lines.append(f"ERROR: xhtml2pdf produced missing/empty file: {pdf_filename}")
            if os.path.exists(pdf_path):
                 try: os.remove(pdf_path)
                 except OSError: pass
            return None

    except Exception as e:
        log_lines.append(f"ERROR: Unexpected error during PDF conversion ({os.path.basename(html_path)}): {str(e)}")
        log_lines.append(traceback.format_exc())
        if pdf_path and os.path.exists(pdf_path):
            try: os.remove(pdf_path)
            except OSError as e: log_lines.append(f"Could not remove failed PDF during cleanup: {e}")
        return None


def cleanup_files(html_path, assets, output_dir, log_lines):
    try:
        if html_path and os.path.exists(html_path):
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
    html_path = None
    downloaded_assets = []
    pdf_path = None
    try:
        log_lines.append(f"Processing {form} filing {accession} from {date}...")
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
                log_lines.append(f"Note: Used 'latin-1' fallback for {accession}.")
            except UnicodeDecodeError:
                decoded_text = r.content.decode('utf-8', errors='replace')
                log_lines.append(f"Warning: Used 'utf-8' with replacements for {accession}.")

        replacements = {
            "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—",
            "&nbsp;": " ", "\u00a0": " "
        }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)

        soup = BeautifulSoup(decoded_text, 'html.parser')
        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            head = soup.head if soup.head else soup.new_tag('head')
            head.insert(0, meta_tag)
            if not soup.head:
                if soup.html:
                    soup.html.insert(0, head)
                else:
                    soup.insert(0, head)

        downloaded_assets = download_assets(soup, urljoin(doc_url, '.'), output_dir, log_lines)
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
        log_lines.append(f"Processing error for {accession}: {str(e)}")
        log_lines.append(traceback.format_exc())
    finally:
        if cleanup_flag and not pdf_path:
            cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
    return (form, None)


def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    pdf_files = {"10-K": [], "10-Q": []}
    if not cik.isdigit():
        log_lines.append("Invalid CIK format")
        return pdf_files
    cik_padded = cik.zfill(10)
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    log_lines.append("Accessing SEC database...")
    try:
        submissions = fetch_json_with_retries(submissions_url, max_retries=3, delay=10)
    except Exception as e:
        log_lines.append(f"Connection error: {str(e)}")
        return pdf_files
    try:
        filings_data = submissions.get('filings', {}).get('recent', {})
        if not filings_data or 'accessionNumber' not in filings_data:
            log_lines.append("No recent filings found in submissions data.")
            return pdf_files

        accession_numbers = filings_data.get('accessionNumber', [])
        forms = filings_data.get('form', [])
        filing_dates = filings_data.get('filingDate', [])
        primary_documents = filings_data.get('primaryDocument', [])

        if not (len(accession_numbers) == len(forms) == len(filing_dates) == len(primary_documents)):
            log_lines.append("Filing data arrays have inconsistent lengths. Aborting processing.")
            return pdf_files

        log_lines.append(f"Found {len(accession_numbers)} recent filings. Filtering relevant ones from FY{EARLIEST_FISCAL_YEAR_SUFFIX} onwards...")

        tasks_to_submit = []
        for i in range(len(accession_numbers)):
            form = forms[i]
            if form not in ["10-K", "10-Q"]:
                continue
            try:
                filing_date_str = filing_dates[i]
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                period = get_filing_period(form, filing_date, fy_month, fy_adjust)
                year_suffix = -1
                if period.startswith("FY"):
                    year_suffix = int(period[2:])
                elif "Q" in period:
                    year_suffix = int(period.split("Q")[-1])
                if year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX:
                    continue  # Skip filings older than FY{EARLIEST_FISCAL_YEAR_SUFFIX}
                accession_clean = accession_numbers[i].replace('-', '')
                doc_file = primary_documents[i]
                doc_url = f"{archive_base_url}{accession_clean}/{doc_file}"
                tasks_to_submit.append({
                    "doc_url": doc_url, "cik": cik_padded, "form": form, "date": filing_date_str,
                    "accession": accession_clean, "ticker": ticker, "fy_month": fy_month,
                    "fy_adjust": fy_adjust, "cleanup_flag": cleanup_flag, "output_dir": tmp_dir
                })
            except Exception as e:
                log_lines.append(f"Skipping filing {accession_numbers[i]} due to error: {e}")
                continue

        log_lines.append(f"Queued {len(tasks_to_submit)} filings for processing.")
        if not tasks_to_submit:
            return pdf_files

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(download_and_process, log_lines=log_lines, **task): task for task in tasks_to_submit}
            for future in as_completed(futures):
                try:
                    form_type, pdf_path = future.result()
                    if pdf_path:
                        pdf_files[form_type].append(pdf_path)
                except Exception as e:
                    task_info = futures[future]
                    log_lines.append(f"ERROR: Task failed for {task_info.get('form')} {task_info.get('accession')}: {e}")

        log_lines.append(f"Processing complete. Generated {len(pdf_files['10-K'])} 10-K and {len(pdf_files['10-Q'])} 10-Q PDFs.")
        return pdf_files

    except KeyError as e:
        log_lines.append(f"Data format error: Missing key {e}.")
        return pdf_files
    except Exception as e:
        log_lines.append(f"Unexpected error during filing processing: {str(e)}")
        log_lines.append(traceback.format_exc())
        return pdf_files


def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs:
        log_lines.append("No PDFs were generated; skipping ZIP creation.")
        return None
    zip_filename = f"{cik}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    log_lines.append(f"Creating ZIP archive '{zip_filename}' with {total_pdfs} PDF(s)...")
    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for form_type, paths in pdf_files.items():
                for pdf_path in paths:
                    if pdf_path and os.path.exists(pdf_path):
                        arcname = os.path.join(form_type, os.path.basename(pdf_path))
                        zipf.write(pdf_path, arcname=arcname)
                        added_count += 1
                    else:
                        log_lines.append(f"Warning: Skipping missing PDF path: {pdf_path}")
        log_lines.append(f"ZIP archive '{zip_filename}' created with {added_count}/{total_pdfs} files.")
        return zip_path
    except Exception as e:
        log_lines.append(f"ZIP archive creation failed: {str(e)}")
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
            except OSError:
                pass
        return None

# -------------------------
# Streamlit UI (GUI remains unchanged)
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("Mzansi EDGAR Fetcher")
st.write("Fetch SEC filings as PDFs and download them as a ZIP archive.")

st.markdown("""
**Instructions:**
1. Enter the company's Central Index Key (CIK). [Find CIK here](https://www.sec.gov/edgar/searchedgar/cik).
2. (Optional) Enter the stock ticker.
3. Select the company's Fiscal Year-End Month.
4. Choose the Fiscal Year Basis.
5. Click "Fetch Filings". *Fetches FY17 10-K and all newer 10-Ks/10-Qs.*
6. (Optional) Check the box to delete intermediate files after conversion.
""")

with st.form("filing_form"):
    col1, col2 = st.columns(2)
    with col1:
        cik_input = st.text_input("Company CIK (e.g., 1018724 for NVIDIA):", key="cik")
        ticker_input = st.text_input("Ticker (Optional, e.g., NVDA):", key="ticker")
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
            key="fy_adjust",
            help="Choose 'Same Year' (standard) or 'Previous Year' if FY labels are off."
        )

    cleanup_flag_input = st.checkbox(
        "Delete intermediate HTML/asset files after PDF conversion",
        value=False,
        key="cleanup",
        help="Check to delete intermediate files."
    )

    submitted = st.form_submit_button("🚀 Fetch Filings")

if submitted:
    if not cik_input or not cik_input.strip().isdigit():
        st.error("CIK is required and must be numeric.")
    else:
        cik_clean = cik_input.strip()
        ticker_clean = ticker_input.strip().upper() if ticker_input else ""
        st.info("Processing filing... This may take several minutes.")
        log_container = st.expander("Show Process Log", expanded=False)
        log_lines = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_lines.append(f"Using temporary directory: {tmp_dir}")
            with st.spinner("Fetching filings and converting to PDFs..."):
                pdf_files_dict = process_filing(
                    cik=cik_clean,
                    ticker=ticker_clean,
                    fy_month=fy_month_input,
                    fy_adjust=fy_adjust_input,
                    cleanup_flag=cleanup_flag_input,
                    log_lines=log_lines,
                    tmp_dir=tmp_dir
                )
                if not any(pdf_files_dict.values()):
                    st.error("No valid filings found for the given criteria.")
                else:
                    zip_path = create_zip_archive(
                        pdf_files=pdf_files_dict,
                        cik=cik_clean,
                        log_lines=log_lines,
                        tmp_dir=tmp_dir
                    )
                    if not zip_path:
                        st.error("Failed to create ZIP archive.")
                    else:
                        with open(zip_path, "rb") as f:
                            zip_data = f.read()
                        st.success("ZIP archive created successfully!")
                        st.download_button(
                            label=f"⬇️ Download {os.path.basename(zip_path)}",
                            data=zip_data,
                            file_name=os.path.basename(zip_path),
                            mime="application/zip"
                        )
        with log_container:
            st.text_area("Log Output:", "\n".join(log_lines), height=400)

st.markdown("---")
st.caption("Mzansi EDGAR Fetcher v1.2 | Data from SEC EDGAR | Fetches FY17 10-K and newer filings.")
