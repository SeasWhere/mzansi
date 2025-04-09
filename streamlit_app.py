# streamlit_app.py
import os
import sys
import requests
import tempfile
import zipfile
import time
import mimetypes # For guessing asset types
import traceback # For detailed error logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st

# Ensure necessary libraries are installed
# These should be in your requirements.txt for Streamlit Cloud
try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    st.error("Error: `beautifulsoup4` not found. Please add it to requirements.txt")
    st.stop()

# --- Use WeasyPrint instead of xhtml2pdf ---
try:
    from weasyprint import HTML, CSS
    from weasyprint.logger import LOGGER as weasyprint_logger
    import logging
    # Optional: Set WeasyPrint logging level (e.g., to ERROR to reduce noise)
    weasyprint_logger.setLevel(logging.ERROR)
except ModuleNotFoundError:
    st.error("Error: `weasyprint` not found. Please add it to requirements.txt")
    st.stop()
except Exception as e:
    # Catch potential import errors if system dependencies are missing
    st.error(f"Error importing WeasyPrint: {e}. Ensure system dependencies are listed in packages.txt.")
    st.stop()
# --- End WeasyPrint Import ---


# -------------------------
# Global configuration
# -------------------------
HEADERS = {
    # User agent includes contact info as requested by SEC best practices
    'User-Agent': 'Mzansi EDGAR Viewer v2.3 (support@example.com)' # Version bump
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 20  # Timeout for individual HTTP requests in seconds

# --- Scope Control ---
# Fiscal Year cutoff: Process filings from this year onwards.
EARLIEST_FISCAL_YEAR_SUFFIX = 17
# --- Limit to Prevent Resource Exhaustion ---
MAX_FILINGS_TO_PROCESS = 5 # Limit the number of relevant filings processed (low for testing)
# ----------------------------------


# -------------------------
# Backend Functions
# -------------------------

# get_filing_period function remains the same
def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
    """
    Determines the fiscal period string (e.g., FY23, 1Q24) based on filing date and fiscal year end.
    Handles December and non-December fiscal year ends.
    """
    # Ensure fiscal_year_end_month is a valid integer (1-12)
    try:
        fiscal_year_end_month = int(fiscal_year_end_month)
        if not 1 <= fiscal_year_end_month <= 12:
            raise ValueError("Month must be between 1 and 12")
    except (ValueError, TypeError):
        # Default to December if input is invalid (e.g., None or non-numeric)
        fiscal_year_end_month = 12

    # --- Non-December Fiscal Year End ---
    if fiscal_year_end_month != 12:
        # Determine the reporting year based on when the filing occurs relative to the FYE month
        reported_year = filing_date.year if filing_date.month > fiscal_year_end_month else filing_date.year - 1
        # Apply adjustment if user selected "Previous Year" basis
        if fy_adjust == "Previous Year":
            reported_year -= 1

        if form == "10-K":
            # 10-K uses the reporting year directly
            return f"FY{reported_year % 100:02d}"
        elif form == "10-Q":
            # Calculate the quarter based on months passed since FYE
            # Add 12 to handle month wrap-around, then take modulo 12
            months_since_fye_start = (filing_date.month - fiscal_year_end_month - 1 + 12) % 12
            quarter = (months_since_fye_start // 3) + 1

            # Determine the fiscal year the quarter belongs to.
            # Quarters start *after* the FYE month.
            # If filing month is <= FYE month, it belongs to the FY that just ended (reported_year).
            # If filing month is > FYE month, it belongs to the *next* FY cycle (reported_year + 1).
            q_year = reported_year if filing_date.month <= fiscal_year_end_month else reported_year + 1

            return f"{quarter}Q{q_year % 100:02d}"
        else: # Default for other forms (e.g., 8-K - though not typically processed here)
             # This logic might need adjustment if other forms require specific period naming
             return f"FY{reported_year % 100:02d}"

    # --- Standard December Fiscal Year End ---
    else:
        if form == "10-K":
            # 10-K reports on the *previous* calendar year
            fiscal_year = filing_date.year - 1
            if fy_adjust == "Previous Year": # Adjust further back if requested
                 fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"
        elif form == "10-Q":
            month = filing_date.month
            # Determine quarter based on *calendar* quarter end dates approximated by filing month
            if 1 <= month <= 3: quarter = 4; report_year = filing_date.year - 1 # Q4 (Oct-Dec) filed Jan-Mar
            elif 4 <= month <= 6: quarter = 1; report_year = filing_date.year     # Q1 (Jan-Mar) filed Apr-Jun
            elif 7 <= month <= 9: quarter = 2; report_year = filing_date.year     # Q2 (Apr-Jun) filed Jul-Sep
            elif 10 <= month <= 12: quarter = 3; report_year = filing_date.year    # Q3 (Jul-Sep) filed Oct-Dec
            else: # Should not happen with valid months
                return f"Q?{(filing_date.year -1) % 100:02d}" # Fallback

            fiscal_year = report_year # For Dec FYE, fiscal year label matches calendar year of quarter end
            if fy_adjust == "Previous Year":
                 fiscal_year -= 1 # Adjust the label year back if requested

            return f"{quarter}Q{fiscal_year % 100:02d}"
        else: # Default for other forms
            fiscal_year = filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"

# --- MODIFIED download_assets function ---
def download_assets(soup, base_url, filing_output_dir, log_lines): # Accepts specific dir
    """
    Downloads assets (images, CSS) linked in the HTML, saves them into the
    specific filing's output directory, and updates links to relative paths.
    """
    downloaded_assets_filenames = set()
    processed_urls = set()
    tags_and_attrs = [('img', 'src'), ('link', 'href')]

    for tag_name, url_attr in tags_and_attrs:
        for tag in soup.find_all(tag_name):
            if tag_name == 'link':
                rel = tag.get('rel')
                if not rel or 'stylesheet' not in rel: continue
            asset_url = tag.get(url_attr)
            if not asset_url or asset_url.startswith(('data:', 'javascript:')): continue

            try:
                absolute_url = urljoin(base_url, asset_url)
                parsed_url = urlparse(absolute_url)
            except ValueError:
                log_lines.append(f"Warning: Skipping invalid asset URL format: {asset_url}")
                continue

            if parsed_url.scheme not in ['http', 'https']: continue
            if absolute_url in processed_urls: continue
            processed_urls.add(absolute_url)

            try:
                path_part = parsed_url.path
                filename_base = os.path.basename(path_part)
                if not filename_base:
                    segments = [s for s in path_part.split('/') if s]
                    filename_base = segments[-1] if segments else f"asset_{len(downloaded_assets_filenames) + 1}"

                safe_filename = "".join(c if c.isalnum() or c in ['.', '_', '-'] else '_' for c in filename_base)
                safe_filename = safe_filename[:100].strip('._')
                if not safe_filename: safe_filename = f"asset_{len(downloaded_assets_filenames) + 1}"

                _, ext = os.path.splitext(safe_filename)
                if not ext: safe_filename += ".asset"
                # --- Save asset in the specific filing's directory ---
                local_path = os.path.join(filing_output_dir, safe_filename)

                if not os.path.exists(local_path):
                    time.sleep(0.11)
                    r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                    r.raise_for_status()

                    content_type = r.headers.get('content-type')
                    guessed_ext = None
                    if content_type:
                        guessed_ext = mimetypes.guess_extension(content_type.split(';')[0])
                    if guessed_ext and guessed_ext != ".asset" and not safe_filename.lower().endswith(guessed_ext.lower()):
                         base, _ = os.path.splitext(safe_filename)
                         new_safe_filename = base + guessed_ext
                         new_local_path = os.path.join(filing_output_dir, new_safe_filename)
                         if not os.path.exists(new_local_path):
                              safe_filename = new_safe_filename
                              local_path = new_local_path

                    with open(local_path, 'wb') as f: f.write(r.content)

                # --- Update link to be relative filename ---
                tag[url_attr] = safe_filename
                downloaded_assets_filenames.add(safe_filename)

            except requests.exceptions.Timeout:
                 log_lines.append(f"Warning: Asset download timeout for {absolute_url}")
            except requests.exceptions.RequestException as e:
                log_lines.append(f"Warning: Asset download error for {absolute_url}: {str(e)}")
            except IOError as e:
                log_lines.append(f"Warning: Asset file write error for {safe_filename}: {str(e)}")
            except Exception as e:
                log_lines.append(f"Warning: General error processing asset {absolute_url}: {str(e)}")

    # if downloaded_assets_filenames: # Reduce log noise
    #     log_lines.append(f"Processed {len(downloaded_assets_filenames)} asset file(s).")
    return list(downloaded_assets_filenames)

# --- MODIFIED convert_to_pdf function ---
def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    """
    Converts the local HTML file to PDF using WeasyPrint.
    Applies custom CSS to control page margins, set EB Garamond font, and add page numbers.
    """
    pdf_path = None
    try:
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, fy_month_idx, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name).strip('._')
        if not safe_base_name: safe_base_name = f"{cik}_{accession}"
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        # log_lines.append(f"Attempting PDF conversion with WeasyPrint: {pdf_filename}")

        html_dir_url = 'file://' + os.path.dirname(os.path.abspath(html_path)) + '/'
        html = HTML(filename=html_path, base_url=html_dir_url)

        # --- Define CSS for PDF page margins, EB Garamond font, and page numbers ---
        # IMPORTANT: Assumes 'EBGaramond-Regular.ttf' (or similar) is in a 'fonts' subdirectory.
        #            Verify the filename and adjust the url() path if needed.
        styling_css_string = """
        /* Embed the EB Garamond font */
        @font-face {
            font-family: "EB Garamond";
            /* Verify this filename matches the font file you added */
            src: url('fonts/EBGaramond-Regular.ttf') format('truetype');
            font-weight: normal;
            font-style: normal;
        }
        /* You might need additional @font-face rules for Bold, Italic, etc. if used */
        /* e.g., src: url('fonts/EBGaramond-Bold.ttf'); font-weight: bold; */

        /* Define page layout */
        @page {
            margin-top: 0.8cm; /* Keep reduced top margin */
            margin-bottom: 1.5cm; /* Keep margin for footer */
            margin-left: 1cm;
            margin-right: 1cm;

            /* Add page number in the bottom center using EB Garamond */
            @bottom-center {
                content: "Page " counter(page) " of " counter(pages);
                font-family: "EB Garamond", serif; /* Use EB Garamond */
                font-size: 9pt;
                color: #555;
                vertical-align: top;
                padding-top: 5mm;
            }
        }

        /* Set base body font to EB Garamond */
        body {
            font-family: "EB Garamond", serif; /* Use EB Garamond, fallback to generic serif */
            font-size: 11pt;   /* Adjust base font size as needed */
            line-height: 1.3;
        }

        /* Optional: Basic table styling */
        table {
            border-collapse: collapse;
            width: 100%;
            margin-top: 0.5em;
            margin-bottom: 0.5em;
         }
        th, td {
            border: 1px solid #ccc;
            padding: 4px 6px;
            text-align: left;
            vertical-align: top;
        }
        th {
             background-color: #f2f2f2;
             font-weight: bold;
        }
        """
        styling_css = CSS(string=styling_css_string)
        # ----------------------------------------------------

        # Render the PDF, applying the custom CSS
        html.write_pdf(pdf_path, stylesheets=[styling_css])

        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 100:
            log_lines.append(f"PDF created: {pdf_filename}")
            return pdf_path
        else:
            log_lines.append(f"ERROR: WeasyPrint conversion resulted in missing or near-empty file: {pdf_filename}")
            if os.path.exists(pdf_path):
                 try: os.remove(pdf_path)
                 except OSError: pass
            return None

    except FileNotFoundError:
        log_lines.append(f"ERROR: HTML file not found for PDF conversion: {html_path}")
        return None
    except ValueError as e:
         log_lines.append(f"ERROR: Value error during PDF setup ({os.path.basename(html_path)}): {str(e)}")
         return None
    except Exception as e:
        log_lines.append(f"ERROR: WeasyPrint PDF conversion failed for {accession} ({os.path.basename(html_path)}): {str(e)}")
        # Check if it's a font loading error
        if "font" in str(e).lower() or "EBGaramond" in str(e):
             log_lines.append("Hint: Check if the font file ('fonts/EBGaramond-Regular.ttf') exists and the path/filename in the CSS is correct.")
        log_lines.append(traceback.format_exc(limit=1))
        if pdf_path and os.path.exists(pdf_path):
            try: os.remove(pdf_path)
            except OSError as e_clean: log_lines.append(f"Warning: Could not remove failed PDF {pdf_filename} during cleanup: {e_clean}")
        return None

# --- MODIFIED cleanup_files function ---
def cleanup_files(html_path, assets, filing_output_dir, log_lines): # Accepts specific dir
    """Removes the temporary HTML file and downloaded asset files from the specific filing directory."""
    cleaned_count = 0
    try:
        # Clean HTML file
        if html_path and os.path.exists(html_path):
            os.remove(html_path)
            cleaned_count += 1

        # Clean asset files from the specific directory
        for asset_filename in assets:
            asset_path = os.path.join(filing_output_dir, asset_filename) # Use filing_output_dir
            if os.path.exists(asset_path):
                try:
                    os.remove(asset_path)
                    cleaned_count += 1
                except OSError as e:
                     log_lines.append(f"Warning: Error cleaning asset {asset_filename}: {e}")

        if cleaned_count > 0:
            log_lines.append(f"Cleaned {cleaned_count} intermediate file(s) for this filing.")
    except Exception as e:
        log_lines.append(f"ERROR: Exception during file cleanup for {os.path.basename(filing_output_dir)}: {str(e)}")


# --- MODIFIED download_and_process function ---
def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, filing_output_dir): # Accepts specific dir
    """
    Worker function: Downloads HTML/assets into filing_output_dir, converts to PDF, optionally cleans up.
    Returns a tuple: (form_type, path_to_pdf or None).
    """
    html_path = None
    downloaded_assets = []
    pdf_path = None
    log_prefix = f"[{accession} {form}]"

    try:
        log_lines.append(f"{log_prefix} Starting processing in {os.path.basename(filing_output_dir)}...")
        # --- Download Primary HTML Document ---
        time.sleep(0.11)
        # log_lines.append(f"{log_prefix} Downloading main HTML...")
        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        # log_lines.append(f"{log_prefix} Download complete.")

        # --- Save HTML in the specific filing's directory ---
        base_html_filename = f"{cik}_{form}_{date}_{accession}.htm"
        html_path = os.path.join(filing_output_dir, base_html_filename) # Use filing_output_dir

        # --- Decode HTML Content ---
        try: decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
             try: decoded_text = r.content.decode('latin-1')
             except UnicodeDecodeError:
                 decoded_text = r.content.decode('utf-8', errors='replace')
                 log_lines.append(f"{log_prefix} Warning: Used 'utf-8' with error replacement.")

        # --- Pre-process & Parse HTML ---
        replacements = { "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—", "&nbsp;": " ", "\u00a0": " " }
        for wrong, correct in replacements.items(): decoded_text = decoded_text.replace(wrong, correct)
        soup = BeautifulSoup(decoded_text, 'html.parser')

        # Ensure UTF-8 meta tag
        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            head = soup.head or soup.new_tag('head')
            if not soup.head:
                 doc_root = soup.html or soup
                 doc_root.insert(0, head)
            head.insert(0, meta_tag)

        # --- Download Assets into the specific filing's directory ---
        doc_base_url = urljoin(doc_url, '.')
        downloaded_assets = download_assets(soup, doc_base_url, filing_output_dir, log_lines) # Pass filing_output_dir

        # --- Save Processed HTML ---
        with open(html_path, 'w', encoding='utf-8') as f: f.write(str(soup))

        # --- Convert to PDF ---
        log_lines.append(f"{log_prefix} Starting PDF conversion...")
        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)
        # PDF creation/failure logged within convert_to_pdf

        # --- Return PDF Path (or None) ---
        return (form, pdf_path)

    # --- Error Handling ---
    except requests.exceptions.Timeout:
         log_lines.append(f"{log_prefix} ERROR: Timeout downloading main document.")
    except requests.exceptions.RequestException as e:
        log_lines.append(f"{log_prefix} ERROR: Download failed: {str(e)}")
    except IOError as e:
         log_lines.append(f"{log_prefix} ERROR: File I/O error: {str(e)}")
    except Exception as e:
        log_lines.append(f"{log_prefix} ERROR: Unexpected processing error: {str(e)}")
        log_lines.append(traceback.format_exc(limit=2))

    # --- Cleanup ---
    finally:
        # Cleanup happens within the specific filing's directory
        if cleanup_flag:
            cleanup_files(html_path, downloaded_assets, filing_output_dir, log_lines) # Pass filing_output_dir
        # log_lines.append(f"{log_prefix} Processing finished.") # Reduce log noise

    return (form, None) # Return None if error occurred


# --- MODIFIED process_filing function (with fix for UnboundLocalError) ---
def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir): # tmp_dir is now base dir
    """
    Main orchestrator: Fetches EDGAR index, filters filings, creates subdirs,
    submits tasks to thread pool, collects results.
    """
    pdf_files = {"10-K": [], "10-Q": []}
    if not cik.isdigit():
        log_lines.append(f"ERROR: Invalid CIK '{cik}'. Must be numeric.")
        st.error(f"Invalid CIK provided: '{cik}'. Must be numeric.")
        return pdf_files
    cik_padded = cik.zfill(10)

    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    log_lines.append(f"Accessing EDGAR index for CIK: {cik_padded}...")
    try:
        time.sleep(0.11)
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        submissions = r.json()
        log_lines.append("Successfully retrieved submission data.")
        if not ticker and 'tickers' in submissions and submissions['tickers']:
             ticker = submissions['tickers'][0]
             log_lines.append(f"Note: Ticker not provided, using '{ticker}' from SEC data.")
    except Exception as e: # Catch all exceptions during fetch
         log_lines.append(f"ERROR: Failed to retrieve or process submission data for CIK {cik_padded}: {str(e)}")
         st.error(f"Failed to retrieve or process data for CIK {cik_padded}. Check CIK and network.")
         return pdf_files

    try:
        filings_data = submissions.get('filings', {}).get('recent', {})
        if not filings_data or 'accessionNumber' not in filings_data:
            log_lines.append("No recent filings found in submission data.")
            st.warning("No recent filings found for this CIK.")
            return pdf_files

        accession_numbers = filings_data.get('accessionNumber', [])
        forms = filings_data.get('form', [])
        filing_dates = filings_data.get('filingDate', [])
        primary_documents = filings_data.get('primaryDocument', [])
        list_len = len(accession_numbers)
        if not (list_len == len(forms) == len(filing_dates) == len(primary_documents)):
             log_lines.append("ERROR: Filing data lists have inconsistent lengths.")
             st.error("Inconsistent data received from SEC EDGAR.")
             return pdf_files

        log_lines.append(f"Found {list_len} recent filings entries. Filtering...")

        tasks_to_submit = []
        processed_relevant_count = 0

        # --- Filter Filings BEFORE Submitting to Threads ---
        for i in range(list_len):
            form = forms[i]
            accession_raw = accession_numbers[i]
            if form not in ["10-K", "10-Q"]: continue

            if processed_relevant_count >= MAX_FILINGS_TO_PROCESS:
                 log_lines.append(f"Reached processing limit ({MAX_FILINGS_TO_PROCESS} relevant filings). Stopping search.")
                 break

            # --- Initialize period before try block ---
            period = "N/A"
            try:
                filing_date_str = filing_dates[i]
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                period = get_filing_period(form, filing_date, fy_month, fy_adjust) # Assign period here

                year_suffix = -1
                if period.startswith("FY"): year_suffix = int(period[2:])
                elif "Q" in period: year_suffix = int(period.split("Q")[-1])

                if 0 <= year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX: continue
                if year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and form == "10-Q": continue

                processed_relevant_count += 1

                accession_clean = accession_raw.replace('-', '')
                doc_filename = primary_documents[i]
                if not doc_filename:
                    log_lines.append(f"Warning: Skipping filing {accession_raw} due to missing primary document name.")
                    processed_relevant_count -= 1
                    continue
                doc_url = f"{archive_base_url}{accession_clean}/{doc_filename}"

                # --- Create specific directory for this filing ---
                filing_output_dir = os.path.join(tmp_dir, f"filing_{accession_clean}")
                os.makedirs(filing_output_dir, exist_ok=True) # Create dir if needed

                tasks_to_submit.append({
                    "doc_url": doc_url, "cik": cik_padded, "form": form, "date": filing_date_str,
                    "accession": accession_clean, "ticker": ticker, "fy_month": fy_month,
                    "fy_adjust": fy_adjust, "cleanup_flag": cleanup_flag,
                    "filing_output_dir": filing_output_dir # Pass specific dir
                })

            except (ValueError, TypeError) as e:
                 # Now 'period' will be defined (either "N/A" or the calculated value if error occurred later)
                 log_lines.append(f"Warning: Skipping filing {accession_raw} due to parsing error (Period: {period}, Error: {e}).")
                 continue
            except Exception as e:
                 log_lines.append(f"Warning: Skipping filing {accession_raw} due to unexpected error during filtering: {e}.")
                 continue

        log_lines.append(f"Identified {len(tasks_to_submit)} filings matching criteria (up to limit of {MAX_FILINGS_TO_PROCESS}) to process.")
        if not tasks_to_submit:
            st.warning(f"No filings found matching the criteria (10-K/10-Q, from FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K onwards, within limit).")
            return pdf_files

        # --- Execute Tasks in Parallel ---
        processed_success_count = 0
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Pass log_lines list - append is generally thread-safe
            futures = {executor.submit(download_and_process, log_lines=log_lines, **task_details): task_details
                       for task_details in tasks_to_submit}

            for future in as_completed(futures):
                task_info = futures[future]
                acc = task_info.get('accession','N/A')
                frm = task_info.get('form','N/A')
                # log_lines.append(f"--- Attempting to get result for {frm} {acc} ---") # Reduce log noise
                try:
                    form_type, pdf_path = future.result()
                    if pdf_path and form_type in pdf_files:
                        # pdf_path is now the full path including the filing_output_dir
                        pdf_files[form_type].append(pdf_path)
                        processed_success_count += 1
                        # log_lines.append(f"--- Successfully processed {frm} {acc} ---") # Reduce log noise
                    # else: # Reduce log noise
                         # log_lines.append(f"--- Task completed for {frm} {acc} but no PDF generated ---")
                except Exception as e:
                    log_lines.append(f"--- ERROR retrieving result for {frm} {acc}: {str(e)} ---")

    except KeyError as e:
        log_lines.append(f"ERROR: Data format error in submissions JSON (Missing key: {e}).")
        st.error("Data format error from SEC EDGAR.")
    except Exception as e:
         log_lines.append(f"ERROR: Unexpected error during main filing processing: {str(e)}")
         log_lines.append(traceback.format_exc())
         st.error("An unexpected error occurred during processing.")

    total_generated = len(pdf_files['10-K']) + len(pdf_files['10-Q'])
    log_lines.append(f"Processing complete. Successfully generated {total_generated} PDF(s) ({len(pdf_files['10-K'])} 10-K, {len(pdf_files['10-Q'])} 10-Q).")
    return pdf_files


# --- MODIFIED create_zip_archive function ---
def create_zip_archive(pdf_files, cik, log_lines, tmp_dir): # tmp_dir is the base temp dir
    """
    Creates a ZIP archive named '<CIK>.zip' containing the generated PDF files
    from their respective subdirectories.
    """
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs:
        log_lines.append("No PDFs were generated, skipping ZIP creation.")
        return None

    zip_filename = f"{cik}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename) # Create zip in base temp dir
    log_lines.append(f"Creating ZIP archive '{zip_filename}' with {total_pdfs} PDF(s)...")
    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for form_type, paths in pdf_files.items():
                if not paths: continue
                for pdf_full_path in paths: # pdf_full_path includes the filing subdir
                    if pdf_full_path and os.path.exists(pdf_full_path):
                        # Create arcname relative to the filing type folder
                        # e.g., "10-K/NVDA_FY23.pdf"
                        arcname = os.path.join(form_type, os.path.basename(pdf_full_path))
                        zipf.write(pdf_full_path, arcname=arcname)
                        added_count += 1
                    else:
                         log_lines.append(f"Warning: Skipping missing/invalid PDF path during zipping: {pdf_full_path}")

        if added_count == total_pdfs:
             log_lines.append(f"ZIP archive '{zip_filename}' created successfully.")
        else:
             log_lines.append(f"Warning: ZIP archive '{zip_filename}' created, but added only {added_count}/{total_pdfs} files.")
        return zip_path

    except Exception as e:
        log_lines.append(f"ERROR: Failed to create ZIP archive '{zip_filename}': {str(e)}")
        if os.path.exists(zip_path):
            try: os.remove(zip_path)
            except OSError: pass
        return None

# -------------------------
# Streamlit UI (Layout and Widgets)
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("📈 Mzansi EDGAR Fetcher")

# Description mentioning the limit
st.markdown(f"""
    **Instructions:**
    1.  Enter the company's Central Index Key (CIK). [Find CIK here](https://www.sec.gov/edgar/searchedgar/cik).
    2.  (Optional) Enter the stock ticker (used for PDF filenames if provided).
    3.  Select the company's Fiscal Year-End Month.
    4.  Choose the Fiscal Year Basis (usually "Same Year").
    5.  Click "Fetch Filings". *Fetches up to {MAX_FILINGS_TO_PROCESS} filings: FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and all newer 10-Ks/10-Qs found.*
    6.  (Optional) Check the box to delete intermediate HTML files after conversion.
    7.  Check the process log for details, especially if PDF quality is unexpected or errors occur.
""")

# --- Input Form ---
with st.form("filing_form"):
    col1, col2 = st.columns(2)
    with col1:
        cik_input = st.text_input("Company CIK (e.g., 1018724 for NVIDIA):", key="cik")
        ticker_input = st.text_input("Ticker (Optional, e.g., NVDA):", key="ticker")
    with col2:
        # Month selection with names only
        month_options = {str(i): datetime(2000, i, 1).strftime('%B') for i in range(1, 13)}
        fy_month_input = st.selectbox(
            "Fiscal Year-End Month:",
            options=list(month_options.keys()),
            format_func=lambda x: month_options[x], # Show only month name
            index=11, # Default to December (12)
            key="fy_month"
        )
        # Fiscal year basis selection
        fy_adjust_input = st.selectbox(
            "Fiscal Year Basis:",
            ["Same Year", "Previous Year"],
            index=0, # Default to Same Year
            key="fy_adjust",
            help="Adjusts the calculated fiscal year label. 'Same Year' is standard."
            )

    # Cleanup option checkbox
    cleanup_flag_input = st.checkbox(
        "Delete intermediate HTML/asset files after PDF conversion",
        value=False, # Default to keeping files for debugging
        key="cleanup",
        help="Check to save space. Uncheck to keep intermediate files, useful if PDF conversion has issues."
        )

    # Submit button for the form
    submitted = st.form_submit_button("🚀 Fetch Filings")

# --- Processing Logic (Runs when form is submitted) ---
if submitted:
    # Validate CIK input
    if not cik_input or not cik_input.strip().isdigit():
        st.error("CIK is required and must be numeric.")
    else:
        cik_clean = cik_input.strip()
        ticker_clean = ticker_input.strip().upper() if ticker_input else "" # Standardize ticker

        st.info(f"Processing request for CIK: {cik_clean}...")
        # Expander to show logs, expanded by default
        log_container = st.expander("Show Process Log", expanded=True)
        log_lines = [] # Initialize log list for this specific run

        # Use a temporary directory for all intermediate files (HTML, assets, PDF, ZIP)
        with tempfile.TemporaryDirectory() as tmp_dir: # tmp_dir is the base temp directory
            log_lines.append(f"Using base temporary directory: {tmp_dir}")
            # Updated spinner text to reflect new limit
            with st.spinner(f"Fetching data (up to {MAX_FILINGS_TO_PROCESS}), converting files into PDF, and creating ZIP"):
                # --- Call the main processing function ---
                # tmp_dir is passed as the base directory for creating subdirectories
                pdf_files_dict = process_filing(
                    cik=cik_clean,
                    ticker=ticker_clean,
                    fy_month=fy_month_input,
                    fy_adjust=fy_adjust_input,
                    cleanup_flag=cleanup_flag_input,
                    log_lines=log_lines,
                    tmp_dir=tmp_dir
                )

                # --- Create and Offer ZIP Download if PDFs were generated ---
                if any(pdf_files_dict.values()): # Check if the dictionary contains any PDF paths
                    # Pass the base tmp_dir for creating the zip file
                    zip_path = create_zip_archive(
                        pdf_files=pdf_files_dict,
                        cik=cik_clean, # Pass CIK for the zip filename
                        log_lines=log_lines,
                        tmp_dir=tmp_dir
                    )

                    # If ZIP creation was successful, provide download button
                    if zip_path and os.path.exists(zip_path):
                        st.success("✅ Success! Filings processed and zipped.")
                        try:
                            # Read zip file data into memory for download
                            with open(zip_path, "rb") as f:
                                zip_data = f.read()
                            # Display download button
                            st.download_button(
                                label=f"⬇️ Download {os.path.basename(zip_path)}", # e.g., Download 1018724.zip
                                data=zip_data,
                                file_name=os.path.basename(zip_path), # Filename for user
                                mime="application/zip"
                            )
                        except Exception as e:
                             st.error(f"Error reading ZIP file for download: {e}")
                             log_lines.append(f"ERROR: Error reading ZIP file for download: {e}")
                    else:
                        # Log file should indicate why zip creation failed
                        st.error("❌ Failed to create the final ZIP archive.")
                else:
                    # Log file should indicate why no PDFs were generated
                    st.warning("⚠️ No relevant filings were successfully processed into PDFs based on the criteria.")

        # Display the collected log output inside the expander
        # Ensure log is updated even if spinner finishes early due to error
        with log_container:
            st.text_area("Log Output:", "\n".join(log_lines), height=400, key="log_output_area")

# --- Footer ---
st.markdown("---")
# Updated caption to mention limit
st.caption(f"Mzansi EDGAR Fetcher v2.3 | Data sourced from SEC EDGAR | Uses WeasyPrint | Fetches up to {MAX_FILINGS_TO_PROCESS} filings from FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K onwards.")

