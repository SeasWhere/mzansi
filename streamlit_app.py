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
    st.error(f"Error importing WeasyPrint: {e}. Ensure system dependencies are listed in packages.txt (see deployment instructions).")
    st.stop()
# --- End WeasyPrint Import ---


# -------------------------
# Global configuration
# -------------------------
HEADERS = {
    # User agent includes contact info as requested by SEC best practices
    'User-Agent': 'Mzansi EDGAR Viewer v1.4 (support@example.com)' # Version bump
}

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 20  # Timeout for individual HTTP requests in seconds

# --- Scope Control ---
# Fiscal Year cutoff: Process filings from this year onwards.
# Filings *before* this year (e.g., FY16 if set to 17) will be skipped.
EARLIEST_FISCAL_YEAR_SUFFIX = 17
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

# download_assets function remains the same
def download_assets(soup, base_url, output_dir, log_lines):
    """
    Downloads assets (images, CSS) linked in the HTML, saves them locally,
    and updates the links in the BeautifulSoup object to relative local paths.

    Args:
        soup (BeautifulSoup): The parsed HTML object.
        base_url (str): The absolute base URL of the *directory* containing the original HTML document.
        output_dir (str): The local temporary directory to save assets.
        log_lines (list): List to append log messages.

    Returns:
        list: A list of unique local filenames of the downloaded assets.
    """
    downloaded_assets_filenames = set() # Store unique *local filenames* added
    processed_urls = set() # Keep track of absolute URLs already processed

    # Find relevant tags and their URL attributes (primarily images and stylesheets)
    tags_and_attrs = [('img', 'src'), ('link', 'href')]

    for tag_name, url_attr in tags_and_attrs:
        for tag in soup.find_all(tag_name):
            # Special handling for <link> tags: only process stylesheets
            if tag_name == 'link':
                rel = tag.get('rel')
                # Check if rel attribute exists and contains 'stylesheet'
                if not rel or 'stylesheet' not in rel:
                    continue

            asset_url = tag.get(url_attr)
            if not asset_url: continue # Skip if tag lacks the specified attribute

            # Skip data URIs and javascript pseudo-URLs
            if asset_url.startswith(('data:', 'javascript:')): continue

            # --- Resolve URL ---
            try:
                # Create absolute URL using the base URL of the document's directory
                absolute_url = urljoin(base_url, asset_url)
                parsed_url = urlparse(absolute_url)
            except ValueError:
                log_lines.append(f"Warning: Skipping invalid asset URL format: {asset_url}")
                continue

            # Skip non-HTTP(S) URLs and already processed URLs
            if parsed_url.scheme not in ['http', 'https']: continue
            if absolute_url in processed_urls: continue
            processed_urls.add(absolute_url)

            try:
                # --- Generate Safe Local Filename ---
                path_part = parsed_url.path
                filename_base = os.path.basename(path_part)
                if not filename_base: # Handle URLs ending in '/'
                    segments = [s for s in path_part.split('/') if s]
                    filename_base = segments[-1] if segments else f"asset_{len(downloaded_assets_filenames) + 1}"

                # Sanitize: allow alphanumeric, dot, underscore, hyphen
                safe_filename = "".join(c if c.isalnum() or c in ['.', '_', '-'] else '_' for c in filename_base)
                safe_filename = safe_filename[:100].strip('._') # Limit length, strip leading/trailing dots/underscores
                if not safe_filename: safe_filename = f"asset_{len(downloaded_assets_filenames) + 1}" # Fallback

                # Ensure a file extension exists (use '.asset' if unknown)
                _, ext = os.path.splitext(safe_filename)
                if not ext: safe_filename += ".asset"

                local_path = os.path.join(output_dir, safe_filename)

                # --- Download Asset (if not already present locally) ---
                if not os.path.exists(local_path):
                    time.sleep(0.11) # Rate limit delay
                    r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                    r.raise_for_status() # Check for download errors

                    # Refine extension based on Content-Type if possible and useful
                    content_type = r.headers.get('content-type')
                    guessed_ext = None
                    if content_type:
                        guessed_ext = mimetypes.guess_extension(content_type.split(';')[0])
                    if guessed_ext and guessed_ext != ".asset" and not safe_filename.lower().endswith(guessed_ext.lower()):
                         base, _ = os.path.splitext(safe_filename)
                         new_safe_filename = base + guessed_ext
                         new_local_path = os.path.join(output_dir, new_safe_filename)
                         # Only use new name if it doesn't conflict with an existing file
                         if not os.path.exists(new_local_path):
                              safe_filename = new_safe_filename
                              local_path = new_local_path

                    # Save the downloaded content
                    with open(local_path, 'wb') as f: f.write(r.content)
                    # log_lines.append(f"Downloaded asset: {safe_filename}") # Can make logs very verbose

                # --- Update HTML Tag ---
                # Update the attribute to the *relative* local filename for WeasyPrint's base_url
                tag[url_attr] = safe_filename
                downloaded_assets_filenames.add(safe_filename)

            # --- Error Handling for Individual Assets ---
            except requests.exceptions.Timeout:
                 log_lines.append(f"Warning: Asset download timeout for {absolute_url}")
            except requests.exceptions.RequestException as e:
                log_lines.append(f"Warning: Asset download error for {absolute_url}: {str(e)}")
            except IOError as e:
                log_lines.append(f"Warning: Asset file write error for {safe_filename}: {str(e)}")
            except Exception as e:
                log_lines.append(f"Warning: General error processing asset {absolute_url}: {str(e)}")

    if downloaded_assets_filenames:
        log_lines.append(f"Processed {len(downloaded_assets_filenames)} unique local asset files for this filing.")
    return list(downloaded_assets_filenames)

# convert_to_pdf function remains the same (using WeasyPrint)
def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    """
    Converts the local HTML file (with updated asset links) to PDF using WeasyPrint.

    NOTE: WeasyPrint Limitations & Requirements:
    - Requires external C libraries (Pango, Cairo, etc.) installed via packages.txt on Streamlit Cloud.
    - Provides better CSS support than xhtml2pdf but is still not a full browser.
    - Complex layouts and JavaScript WILL NOT be perfectly rendered.
    - Check logs for errors if PDF generation fails.
    """
    pdf_path = None
    try:
        # --- Generate PDF Filename (same logic) ---
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, fy_month_idx, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name).strip('._')
        if not safe_base_name: safe_base_name = f"{cik}_{accession}"
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        log_lines.append(f"Attempting PDF conversion with WeasyPrint: {pdf_filename}")

        # --- Conversion using WeasyPrint ---
        # We need to provide the base_url for WeasyPrint to find relative assets (CSS, images)
        # The base_url should be the directory containing the HTML file.
        html_dir_url = 'file://' + os.path.dirname(os.path.abspath(html_path)) + '/'

        # Create WeasyPrint HTML object from the local file path
        html = HTML(filename=html_path, base_url=html_dir_url)

        # Render the PDF to the target path
        html.write_pdf(pdf_path)

        # --- Check Conversion Result ---
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 100: # Check exists and has reasonable size
            log_lines.append(f"PDF created successfully using WeasyPrint: {pdf_filename}")
            return pdf_path
        else:
            log_lines.append(f"ERROR: WeasyPrint conversion resulted in missing or near-empty file: {pdf_filename}")
            if os.path.exists(pdf_path): # Remove empty/failed file
                 try: os.remove(pdf_path)
                 except OSError: pass
            return None

    # --- Error Handling ---
    except FileNotFoundError:
        log_lines.append(f"ERROR: HTML file not found for PDF conversion: {html_path}")
        return None
    except ValueError as e: # Catch date parsing errors etc. in filename generation
         log_lines.append(f"ERROR: Value error during PDF setup ({os.path.basename(html_path)}): {str(e)}")
         return None
    except Exception as e:
        # Catch potential WeasyPrint errors (which can be varied)
        log_lines.append(f"ERROR: Unexpected error during WeasyPrint PDF conversion ({os.path.basename(html_path)}): {str(e)}")
        log_lines.append(traceback.format_exc()) # Log full traceback
        if pdf_path and os.path.exists(pdf_path): # Cleanup potentially corrupt file
            try: os.remove(pdf_path)
            except OSError as e_clean: log_lines.append(f"Warning: Could not remove failed PDF {pdf_filename} during cleanup: {e_clean}")
        return None

# cleanup_files function remains the same
def cleanup_files(html_path, assets, output_dir, log_lines):
    """Removes the temporary HTML file and downloaded asset files."""
    cleaned_count = 0
    try:
        # Clean HTML file
        if html_path and os.path.exists(html_path):
            os.remove(html_path)
            cleaned_count += 1

        # Clean asset files specified in the list
        for asset_filename in assets:
            asset_path = os.path.join(output_dir, asset_filename)
            if os.path.exists(asset_path):
                try:
                    os.remove(asset_path)
                    cleaned_count += 1
                except OSError as e:
                     log_lines.append(f"Warning: Error cleaning asset {asset_filename}: {e}")

        # if cleaned_count > 0:
        #      log_lines.append(f"Cleaned {cleaned_count} intermediate file(s).") # Less verbose log

    except Exception as e:
        log_lines.append(f"ERROR: Exception during file cleanup: {str(e)}")

# download_and_process function remains the same
def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, output_dir):
    """
    Worker function: Downloads HTML, downloads assets, updates links, converts to PDF, optionally cleans up.
    Returns a tuple: (form_type, path_to_pdf or None).
    """
    html_path = None
    downloaded_assets = []
    pdf_path = None # Initialize pdf_path for use in finally block

    try:
        log_lines.append(f"Processing {form} {date} ({accession})...") # Slightly more compact log

        # --- Download Primary HTML Document ---
        time.sleep(0.11) # Adhere to SEC rate limit (10 req/sec)
        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # Define local HTML path using specific details
        base_html_filename = f"{cik}_{form}_{date}_{accession}.htm"
        html_path = os.path.join(output_dir, base_html_filename)

        # --- Decode HTML Content (UTF-8 -> Latin-1 -> Replace) ---
        try:
             decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
             try:
                 decoded_text = r.content.decode('latin-1')
                 # log_lines.append(f"Note: Used 'latin-1' fallback decoding for {accession}.")
             except UnicodeDecodeError:
                 decoded_text = r.content.decode('utf-8', errors='replace')
                 log_lines.append(f"Warning: Used 'utf-8' with error replacement for {accession}.")

        # --- Pre-process & Parse HTML ---
        # Replace common problematic characters/entities before parsing
        replacements = { "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—", "&nbsp;": " ", "\u00a0": " " }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)

        soup = BeautifulSoup(decoded_text, 'html.parser')

        # Ensure UTF-8 meta tag is present for rendering consistency
        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            head = soup.head or soup.new_tag('head') # Get head or create it
            if not soup.head: # If head was created, insert it
                 doc_root = soup.html or soup # Find root element (html or soup itself)
                 doc_root.insert(0, head)
            head.insert(0, meta_tag) # Insert charset at beginning of head

        # --- Download Assets & Update Links in Soup ---
        doc_base_url = urljoin(doc_url, '.') # Base URL for resolving relative asset paths
        downloaded_assets = download_assets(soup, doc_base_url, output_dir, log_lines)

        # --- Save Processed HTML Locally ---
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))

        # --- Convert to PDF (Calls the updated function) ---
        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)

        # --- Return Result ---
        return (form, pdf_path) # pdf_path is None if conversion failed

    # --- Error Handling for the entire process ---
    except requests.exceptions.Timeout:
         log_lines.append(f"ERROR: Timeout downloading main document: {doc_url}")
    except requests.exceptions.RequestException as e:
        log_lines.append(f"ERROR: Download failed for {doc_url}: {str(e)}")
    except IOError as e:
         log_lines.append(f"ERROR: File I/O error during processing {accession}: {str(e)}")
    except Exception as e:
        log_lines.append(f"ERROR: Unexpected error processing {form} {accession}: {str(e)}")
        log_lines.append(traceback.format_exc()) # Log full traceback for unexpected errors

    # --- Cleanup (Always runs due to finally) ---
    finally:
        if cleanup_flag:
            # Only cleanup if flag is set. We don't check pdf_path here,
            # assuming user wants cleanup regardless of success if flag is True.
            cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
        # else: # Optional: Log if files are kept
            # log_lines.append(f"Cleanup flag OFF. Keeping intermediate files for {accession}.")

    # Return None for pdf_path if control reaches here (due to an exception)
    return (form, None)

# process_filing function remains the same (uses continue logic)
def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    """
    Main orchestrator: Fetches EDGAR index, filters filings based on year/form,
    submits tasks to thread pool, collects results.
    Ensures all filings in the index are checked against the criteria.
    """
    pdf_files = {"10-K": [], "10-Q": []}

    # --- Input Validation ---
    if not cik.isdigit():
        log_lines.append(f"ERROR: Invalid CIK '{cik}'. Must be numeric.")
        st.error(f"Invalid CIK provided: '{cik}'. Must be numeric.")
        return pdf_files
    cik_padded = cik.zfill(10)

    # --- Fetch Submissions Index ---
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    log_lines.append(f"Accessing EDGAR index for CIK: {cik_padded}...")
    try:
        time.sleep(0.11) # Rate limit
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        submissions = r.json()
        log_lines.append("Successfully retrieved submission data.")
        # Auto-fill ticker if empty and available
        if not ticker and 'tickers' in submissions and submissions['tickers']:
             ticker = submissions['tickers'][0]
             log_lines.append(f"Note: Ticker not provided, using '{ticker}' from SEC data.")

    # --- Error Handling for Index Fetch ---
    except requests.exceptions.Timeout:
         log_lines.append(f"ERROR: Timeout connecting to SEC submissions URL.")
         st.error("Timeout connecting to SEC EDGAR. Please try again later.")
         return pdf_files
    except requests.exceptions.RequestException as e:
        log_lines.append(f"ERROR: Could not retrieve submission data for CIK {cik_padded}: {str(e)}")
        err_msg = f"Could not retrieve data for CIK {cik_padded}. "
        if "404" in str(e): err_msg += "Please double-check the CIK."
        else: err_msg += "Check network or try again."
        st.error(err_msg)
        return pdf_files
    except Exception as e:
        log_lines.append(f"ERROR: Failed to process submission data: {str(e)}")
        st.error("Failed to process data from SEC EDGAR.")
        return pdf_files

    # --- Process Filings List ---
    try:
        filings_data = submissions.get('filings', {}).get('recent', {})
        if not filings_data or 'accessionNumber' not in filings_data:
            log_lines.append("No recent filings found in submission data.")
            st.warning("No recent filings found for this CIK.")
            return pdf_files

        # Extract required lists
        accession_numbers = filings_data.get('accessionNumber', [])
        forms = filings_data.get('form', [])
        filing_dates = filings_data.get('filingDate', [])
        primary_documents = filings_data.get('primaryDocument', [])

        # Validate data consistency
        list_len = len(accession_numbers)
        if not (list_len == len(forms) == len(filing_dates) == len(primary_documents)):
             log_lines.append("ERROR: Filing data lists have inconsistent lengths.")
             st.error("Inconsistent data received from SEC EDGAR.")
             return pdf_files

        log_lines.append(f"Found {list_len} recent filings entries. Filtering...")

        tasks_to_submit = [] # Holds dicts of arguments for valid tasks

        # --- Filter Filings BEFORE Submitting to Threads ---
        # Iterate through the *entire* list provided by the SEC
        for i in range(list_len):
            form = forms[i]
            # Filter 1: Relevant forms (10-K, 10-Q)
            if form not in ["10-K", "10-Q"]: continue

            try:
                filing_date_str = filing_dates[i]
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                # Calculate period to determine fiscal year
                period = get_filing_period(form, filing_date, fy_month, fy_adjust)

                # Extract year suffix (YY)
                year_suffix = -1
                if period.startswith("FY"): year_suffix = int(period[2:])
                elif "Q" in period: year_suffix = int(period.split("Q")[-1])

                # --- Filter Logic (Using continue) ---
                # Filter 2: Year Cutoff (Skip filings *before* target year)
                if 0 <= year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX:
                    continue # Skip this older filing and check the next one

                # Filter 3: Special FY17 Handling (Skip 10-Qs from FY17)
                if year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and form == "10-Q":
                    continue # Skip this specific filing

                # --- If all filters passed, add task details ---
                accession_raw = accession_numbers[i]
                accession_clean = accession_raw.replace('-', '')
                doc_filename = primary_documents[i]
                # Handle cases where primaryDocument might be missing/empty
                if not doc_filename:
                    log_lines.append(f"Warning: Skipping filing {accession_raw} due to missing primary document name.")
                    continue
                doc_url = f"{archive_base_url}{accession_clean}/{doc_filename}"

                tasks_to_submit.append({
                    "doc_url": doc_url, "cik": cik_padded, "form": form, "date": filing_date_str,
                    "accession": accession_clean, "ticker": ticker, "fy_month": fy_month,
                    "fy_adjust": fy_adjust, "cleanup_flag": cleanup_flag, "output_dir": tmp_dir
                })

            # --- Error handling during filtering loop ---
            except (ValueError, TypeError) as e:
                 log_lines.append(f"Warning: Skipping filing {accession_numbers[i]} due to parsing error (Period: {period}, Error: {e}).")
                 continue
            except Exception as e:
                 log_lines.append(f"Warning: Skipping filing {accession_numbers[i]} due to unexpected error during filtering: {e}.")
                 continue

        # --- Log filtering results and check if any tasks remain ---
        log_lines.append(f"Identified {len(tasks_to_submit)} filings matching criteria to process.")
        if not tasks_to_submit:
            st.warning(f"No filings found matching the criteria (10-K/10-Q, from FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K onwards).")
            return pdf_files

        # --- Execute Tasks in Parallel using ThreadPoolExecutor ---
        processed_success_count = 0
        # Using max_workers=4 as a balance between parallelism and resource usage/rate limits
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Create future objects for submitted tasks
            futures = {executor.submit(download_and_process, log_lines=log_lines, **task_details): task_details
                       for task_details in tasks_to_submit}

            # Process results as tasks complete
            for future in as_completed(futures):
                task_info = futures[future] # Get original task details for logging context
                try:
                    form_type, pdf_path = future.result() # Get result or re-raised exception
                    if pdf_path and form_type in pdf_files: # Check if PDF was created
                        pdf_files[form_type].append(pdf_path)
                        processed_success_count += 1
                except Exception as e:
                    # Log exceptions raised within worker threads
                    log_lines.append(f"ERROR: Task failed for {task_info.get('form','N/A')} {task_info.get('accession','N/A')}: {str(e)}")
                    # log_lines.append(traceback.format_exc()) # Uncomment for full traceback in logs

    # --- Error Handling for Main Processing Block ---
    except KeyError as e:
        log_lines.append(f"ERROR: Data format error in submissions JSON (Missing key: {e}).")
        st.error("Data format error from SEC EDGAR.")
    except Exception as e:
         log_lines.append(f"ERROR: Unexpected error during main filing processing: {str(e)}")
         log_lines.append(traceback.format_exc())
         st.error("An unexpected error occurred during processing.")

    # --- Final Log Summary ---
    total_generated = len(pdf_files['10-K']) + len(pdf_files['10-Q'])
    log_lines.append(f"Processing complete. Successfully generated {total_generated} PDF(s) ({len(pdf_files['10-K'])} 10-K, {len(pdf_files['10-Q'])} 10-Q).")
    return pdf_files

# create_zip_archive function remains the same
def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    """
    Creates a ZIP archive named '<CIK>.zip' containing the generated PDF files,
    organized into subfolders '10-K' and '10-Q'.
    """
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs:
        log_lines.append("No PDFs were generated, skipping ZIP creation.")
        return None

    # --- Use CIK for the ZIP filename ---
    zip_filename = f"{cik}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    log_lines.append(f"Creating ZIP archive '{zip_filename}' with {total_pdfs} PDF(s)...")

    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add files, organized into folders within the zip
            for form_type, paths in pdf_files.items():
                if not paths: continue # Skip if no PDFs for this form type
                for pdf_path in paths:
                    if pdf_path and os.path.exists(pdf_path):
                        # arcname determines the path inside the zip file (e.g., "10-K/NVDA_FY23.pdf")
                        arcname = os.path.join(form_type, os.path.basename(pdf_path))
                        zipf.write(pdf_path, arcname=arcname)
                        added_count += 1
                    else:
                         log_lines.append(f"Warning: Skipping missing/invalid PDF path during zipping: {pdf_path}")

        # --- Log final status of ZIP creation ---
        if added_count == total_pdfs:
             log_lines.append(f"ZIP archive '{zip_filename}' created successfully.")
        else:
             log_lines.append(f"Warning: ZIP archive '{zip_filename}' created, but added only {added_count}/{total_pdfs} files.")
        return zip_path

    except Exception as e:
        log_lines.append(f"ERROR: Failed to create ZIP archive '{zip_filename}': {str(e)}")
        if os.path.exists(zip_path): # Attempt cleanup of partial zip
            try: os.remove(zip_path)
            except OSError: pass
        return None

# -------------------------
# Streamlit UI (Layout and Widgets)
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("📈 Mzansi EDGAR Fetcher")

# Updated description reflecting current logic & WeasyPrint use
st.write(f"Fetch SEC 10-K and 10-Q filings (FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and all subsequent 10-Ks/10-Qs), convert them to PDF using WeasyPrint, and download as a ZIP archive named `<CIK>.zip`.")
st.markdown(f"""
    **Instructions:**
    1.  Enter the company's Central Index Key (CIK). [Find CIK here](https://www.sec.gov/edgar/searchedgar/cik).
    2.  (Optional) Enter the stock ticker (used for PDF filenames if provided).
    3.  Select the company's Fiscal Year-End Month.
    4.  Choose the Fiscal Year Basis (usually "Same Year").
    5.  Click "Fetch Filings". *Fetches FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and all newer 10-Ks/10-Qs.*
    6.  (Optional) Check the box to delete intermediate HTML files after conversion.
    7.  Check the process log for details, especially if PDF quality is unexpected or errors occur.
""")
# --- Removed WeasyPrint specific warnings as requested ---

# --- Input Form ---
with st.form("filing_form"):
    col1, col2 = st.columns(2)
    with col1:
        cik_input = st.text_input("Company CIK (e.g., 1018724 for NVIDIA):", key="cik")
        ticker_input = st.text_input("Ticker (Optional, e.g., NVDA):", key="ticker")
    with col2:
        # Month selection with names
        month_options = {str(i): datetime(2000, i, 1).strftime('%B') for i in range(1, 13)}
        fy_month_input = st.selectbox(
            "Fiscal Year-End Month:",
            options=list(month_options.keys()),
            # --- Updated format_func to show only month name ---
            format_func=lambda x: month_options[x],
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
        # Expander to show logs, initially collapsed
        log_container = st.expander("Show Process Log", expanded=False)
        log_lines = [] # Initialize log list for this specific run

        # Use a temporary directory for all intermediate files (HTML, assets, PDF, ZIP)
        # This directory is automatically cleaned up when the 'with' block exits
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_lines.append(f"Using temporary directory: {tmp_dir}")
            # --- MODIFIED Spinner Text ---
            with st.spinner("Fetching data, converting files into PDF, and creating ZIP"):
                # --- Call the main processing function ---
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
        with log_container:
            st.text_area("Log Output:", "\n".join(log_lines), height=400)

# --- Footer ---
st.markdown("---")
st.caption(f"Mzansi EDGAR Fetcher v1.4 | Data sourced from SEC EDGAR | Uses WeasyPrint | Fetches FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and newer filings.")
