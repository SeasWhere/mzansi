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
DEFAULT_TIMEOUT = 20  # Increased timeout further for potentially large filings/assets

# --- Scope Control ---
# Fiscal Year cutoff: Process filings from this year onwards.
# Filings *before* this year (e.g., FY16 if set to 17) will be skipped.
EARLIEST_FISCAL_YEAR_SUFFIX = 17
# MAX_FILINGS_TO_PROCESS removed to fetch all filings from EARLIEST_FISCAL_YEAR_SUFFIX onwards.
# ----------------------------------


# -------------------------
# Backend Functions
# -------------------------

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
        fiscal_year_end_month = 12 # Default to December if input is invalid

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
            else: # Should not happen
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


def download_assets(soup, base_url, output_dir, log_lines):
    """
    Downloads assets (images, CSS, potentially JS) linked in the HTML,
    saves them locally, and updates the links in the BeautifulSoup object.
    Attempts to handle various URL formats and generate safe filenames.

    Args:
        soup (BeautifulSoup): The parsed HTML object.
        base_url (str): The base URL of the *directory* containing the original HTML document on the server.
        output_dir (str): The local directory to save assets.
        log_lines (list): List to append log messages.

    Returns:
        list: A list of unique local filenames of the downloaded assets.
    """
    downloaded_assets_filenames = set() # Store unique *local filenames* added
    processed_urls = set() # Keep track of absolute URLs already processed to avoid re-downloading

    # Find relevant tags and their URL attributes
    tags_and_attrs = [('img', 'src'), ('link', 'href'), ('script', 'src')] # Add ('video', 'src'), ('source', 'src') etc. if needed

    for tag_name, url_attr in tags_and_attrs:
        for tag in soup.find_all(tag_name):
            # Special handling for <link> tags (only get stylesheets)
            if tag_name == 'link' and tag.get('rel') != ['stylesheet']:
                continue

            asset_url = tag.get(url_attr)
            if not asset_url:
                continue # Skip if tag lacks the specified attribute

            # Skip data URIs and javascript pseudo-URLs
            if asset_url.startswith(('data:', 'javascript:')):
                continue

            # Create absolute URL using the base URL of the document's directory
            try:
                absolute_url = urljoin(base_url, asset_url)
                parsed_url = urlparse(absolute_url)
            except ValueError:
                log_lines.append(f"Skipping invalid asset URL format: {asset_url}")
                continue

            # Skip if not HTTP/HTTPS or already processed this absolute URL
            if parsed_url.scheme not in ['http', 'https']:
                continue
            if absolute_url in processed_urls:
                continue # Already attempted download (success or fail)

            processed_urls.add(absolute_url) # Mark as processed

            try:
                # --- Generate a safe local filename ---
                path_part = parsed_url.path
                filename_base = os.path.basename(path_part)
                if not filename_base: # Handle URLs like example.com/path/ or path ending in /
                    # Attempt to construct a name from the last non-empty path segment
                    segments = [s for s in path_part.split('/') if s]
                    if segments:
                         filename_base = segments[-1]
                    else: # Fallback if path is just '/' or empty
                         filename_base = f"asset_{len(downloaded_assets_filenames) + 1}"


                # Basic sanitization (allow alphanumeric, dot, underscore, hyphen)
                safe_filename = "".join(c if c.isalnum() or c in ['.', '_', '-'] else '_' for c in filename_base)
                # Prevent excessively long filenames
                safe_filename = safe_filename[:100]
                # Ensure it doesn't start/end with problematic characters like '.'
                safe_filename = safe_filename.strip('._')
                if not safe_filename: # If sanitization removed everything
                     safe_filename = f"asset_{len(downloaded_assets_filenames) + 1}"


                # Add a default extension if none exists (improves compatibility)
                _, ext = os.path.splitext(safe_filename)
                if not ext:
                     safe_filename += ".asset" # Generic extension

                local_path = os.path.join(output_dir, safe_filename)

                # --- Download (if not already present locally) ---
                # Check if this *specific local filename* already exists.
                # Note: This simple check might overwrite if different URLs lead to same safe filename.
                # A more robust solution could involve hashing the URL or adding unique identifiers.
                if not os.path.exists(local_path):
                    # Introduce small delay to respect SEC rate limits (10 req/sec)
                    time.sleep(0.11)

                    r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
                    r.raise_for_status() # Check for download errors (4xx, 5xx)

                    # Optional: Refine filename extension based on Content-Type header
                    content_type = r.headers.get('content-type')
                    guessed_ext = None
                    if content_type:
                        guessed_ext = mimetypes.guess_extension(content_type.split(';')[0])

                    # If mime type gives a better extension than the original/default '.asset'
                    if guessed_ext and guessed_ext != ".asset" and not safe_filename.endswith(guessed_ext):
                         base, _ = os.path.splitext(safe_filename)
                         new_safe_filename = base + guessed_ext
                         # Check if this new name conflicts
                         new_local_path = os.path.join(output_dir, new_safe_filename)
                         if not os.path.exists(new_local_path):
                              safe_filename = new_safe_filename
                              local_path = new_local_path
                         # Else: stick with the original safe_filename to avoid overwrite

                    # Save the content
                    with open(local_path, 'wb') as f:
                        f.write(r.content)
                    # log_lines.append(f"Downloaded asset: {safe_filename}") # Less verbose log

                # --- Update HTML tag ---
                # Crucially, update the attribute to the *relative* local filename
                tag[url_attr] = safe_filename
                downloaded_assets_filenames.add(safe_filename) # Add filename to set

            except requests.exceptions.Timeout:
                 log_lines.append(f"Asset download timeout for {absolute_url}")
            except requests.exceptions.RequestException as e:
                log_lines.append(f"Asset download error for {absolute_url}: {str(e)}")
            except IOError as e:
                log_lines.append(f"Asset file write error for {safe_filename}: {str(e)}")
            except Exception as e:
                log_lines.append(f"General asset error processing {absolute_url}: {str(e)}")
                # log_lines.append(traceback.format_exc()) # Uncomment for detailed debug

    if downloaded_assets_filenames:
        log_lines.append(f"Processed {len(downloaded_assets_filenames)} unique local asset files for this filing.")
    return list(downloaded_assets_filenames) # Return list of unique local filenames


def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    """
    Converts the local HTML file (with updated asset links) to PDF using xhtml2pdf.
    Includes a link callback to help resolve local asset paths.
    """
    pdf_path = None # Initialize pdf_path
    try:
        # --- Generate PDF Filename (using safe base name) ---
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        period = get_filing_period(form, filing_date, fy_month_idx, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}"
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name).strip('._')
        if not safe_base_name: safe_base_name = f"{cik}_{accession}" # Fallback filename
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        log_lines.append(f"Attempting PDF conversion: {pdf_filename}")

        # --- Conversion using xhtml2pdf ---
        with open(html_path, "r", encoding="utf-8") as source_html_file, \
             open(pdf_path, "w+b") as result_file:

            # Define a callback function to resolve local file paths for assets (CSS, images)
            # referenced in the modified HTML.
            base_dir = os.path.dirname(html_path)
            def link_callback(uri, rel):
                """
                Resolve links found in the HTML. Prioritize local files within the temp dir.
                uri: the value of the link (e.g., 'style.css', 'image.jpg')
                rel: the base path given by pisa (usually the HTML file path)
                """
                # Check if the URI is a relative path to a file *we downloaded*
                potential_local_path = os.path.abspath(os.path.join(base_dir, uri))

                # Security check: Ensure the resolved path is still within the base directory
                if potential_local_path.startswith(os.path.abspath(base_dir)):
                    if os.path.exists(potential_local_path):
                        # Return the absolute path to the local file
                        return potential_local_path

                # If it's already an absolute file path (less likely needed but possible)
                # if os.path.isabs(uri) and uri.startswith('file:') and os.path.exists(urlparse(uri).path):
                #    return urlparse(uri).path

                # If it's a web URL (http/https), let xhtml2pdf try to handle it (may fail)
                if urlparse(uri).scheme in ['http', 'https']:
                     log_lines.append(f"PDF Conversion: Passing web link to xhtml2pdf: {uri}")
                     return uri

                # If it cannot be resolved locally, return the original URI
                # log_lines.append(f"PDF link_callback could not resolve locally: {uri}")
                return uri

            # Convert HTML to PDF using pisa
            # Note: PDF rendering quality depends heavily on the HTML/CSS complexity
            # and xhtml2pdf's capabilities. It may not perfectly match browser rendering.
            pisa_status = pisa.CreatePDF(
                src=source_html_file,       # Source HTML file object
                dest=result_file,           # Destination PDF file object
                encoding='utf-8',
                link_callback=link_callback # Function to resolve asset paths
            )

        # --- Check Conversion Result ---
        if pisa_status.err:
            log_lines.append(f"ERROR: xhtml2pdf conversion failed for {pdf_filename}. Error code: {pisa_status.err}")
            # Try to clean up potentially corrupted PDF file
            if os.path.exists(pdf_path):
                try: os.remove(pdf_path)
                except OSError as e: log_lines.append(f"Could not remove failed PDF {pdf_filename}: {e}")
            return None # Indicate failure
        elif os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0: # Check file exists and is not empty
            log_lines.append(f"PDF created successfully: {pdf_filename}")
            return pdf_path # Return path on success
        else:
            log_lines.append(f"ERROR: xhtml2pdf conversion resulted in missing or empty file: {pdf_filename}")
            if os.path.exists(pdf_path): # Remove empty file if it exists
                 try: os.remove(pdf_path)
                 except OSError: pass
            return None # Indicate failure

    except FileNotFoundError:
        log_lines.append(f"ERROR: HTML file not found for PDF conversion: {html_path}")
        return None
    except ValueError as e: # Catch date parsing errors etc.
         log_lines.append(f"ERROR: Value error during PDF setup ({os.path.basename(html_path)}): {str(e)}")
         return None
    except Exception as e:
        log_lines.append(f"ERROR: Unexpected error during PDF conversion ({os.path.basename(html_path)}): {str(e)}")
        log_lines.append(traceback.format_exc()) # Log full traceback for debugging
        # Clean up potentially corrupt PDF if conversion failed mid-process
        if pdf_path and os.path.exists(pdf_path):
            try: os.remove(pdf_path)
            except OSError as e: log_lines.append(f"Could not remove failed PDF {pdf_filename} during cleanup: {e}")
        return None


def cleanup_files(html_path, assets, output_dir, log_lines):
    """Removes the temporary HTML file and downloaded asset files."""
    cleaned_count = 0
    try:
        # Clean HTML file
        if html_path and os.path.exists(html_path):
            os.remove(html_path)
            cleaned_count += 1

        # Clean asset files
        for asset_filename in assets:
            asset_path = os.path.join(output_dir, asset_filename)
            if os.path.exists(asset_path):
                try:
                    os.remove(asset_path)
                    cleaned_count += 1
                except OSError as e:
                     log_lines.append(f"Warning: Error cleaning asset {asset_filename}: {e}")

        if cleaned_count > 0:
             log_lines.append(f"Cleaned {cleaned_count} intermediate file(s).")

    except Exception as e:
        log_lines.append(f"ERROR: Exception during file cleanup: {str(e)}")


def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, output_dir):
    """
    Downloads the primary HTML filing document, downloads its assets, updates links,
    converts the result to PDF, and optionally cleans up intermediate files.
    Returns a tuple: (form_type, path_to_pdf or None).
    """
    html_path = None
    downloaded_assets = []
    pdf_path = None # Initialize pdf_path for finally block

    try:
        log_lines.append(f"Processing {form} from {date} ({accession})...")

        # --- Download Primary HTML Document ---
        time.sleep(0.11) # Rate limiting
        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status() # Check for HTTP errors

        # Define local HTML path
        base_html_filename = f"{cik}_{form}_{date}_{accession}.htm"
        html_path = os.path.join(output_dir, base_html_filename)

        # --- Decode HTML Content ---
        # Try decoding with utf-8, fallback to latin-1, then replace errors
        try:
             decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
             try:
                 decoded_text = r.content.decode('latin-1')
                 log_lines.append(f"Note: Used 'latin-1' fallback decoding for {accession}.")
             except UnicodeDecodeError:
                 decoded_text = r.content.decode('utf-8', errors='replace')
                 log_lines.append(f"Warning: Used 'utf-8' with error replacement for {accession}.")

        # --- Pre-process & Parse HTML ---
        # Simple replacements for common display issues before parsing
        replacements = {
            "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—",
            "&nbsp;": " ", "\u00a0": " " # Non-breaking spaces
        }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)

        soup = BeautifulSoup(decoded_text, 'html.parser')

        # Ensure UTF-8 meta tag is present for better rendering consistency
        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            head = soup.head
            if not head: # Create head if missing
                 head = soup.new_tag('head')
                 if soup.html: soup.html.insert(0, head)
                 else: soup.insert(0, head) # Very unlikely case
            head.insert(0, meta_tag) # Insert at beginning of head

        # --- Download Assets & Update Links ---
        # Base URL should be the *directory* containing the HTML file
        doc_base_url = urljoin(doc_url, '.') # Resolves relative to the doc_url
        downloaded_assets = download_assets(soup, doc_base_url, output_dir, log_lines)

        # --- Save Processed HTML Locally ---
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        # log_lines.append(f"Saved processed HTML: {base_html_filename}") # Less verbose log

        # --- Convert to PDF ---
        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)

        # --- Return Result ---
        return (form, pdf_path) # pdf_path is None if conversion failed

    # --- Error Handling ---
    except requests.exceptions.Timeout:
         log_lines.append(f"ERROR: Timeout downloading main document: {doc_url}")
    except requests.exceptions.RequestException as e:
        log_lines.append(f"ERROR: Download failed for {doc_url}: {str(e)}")
    except IOError as e:
         log_lines.append(f"ERROR: File writing error during processing {accession}: {str(e)}")
    except Exception as e:
        log_lines.append(f"ERROR: Unexpected error processing {form} {accession}: {str(e)}")
        log_lines.append(traceback.format_exc()) # Log full traceback

    # --- Cleanup in Finally Block ---
    finally:
        # Cleanup logic based on whether PDF was created and the cleanup flag
        if cleanup_flag:
            if pdf_path: # PDF was created successfully, cleanup is safe
                 # log_lines.append(f"Cleaning intermediate files for {accession} (PDF created).")
                 cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
            # else: # PDF failed or process errored before PDF step
                 # log_lines.append(f"Cleanup flag ON, but PDF failed for {accession}. Keeping intermediate files for debugging.")
                 # If you ALWAYS want cleanup when flag is ON, uncomment below:
                 # cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
        # else: # Cleanup flag is OFF
             # log_lines.append(f"Cleanup flag OFF. Keeping intermediate files for {accession}.")


    # Return None for pdf_path if any exception occurred before successful conversion
    return (form, None)


def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    """
    Fetches filing data from SEC EDGAR, filters for relevant filings (10-K/10-Q)
    from FY17 (10-K only) onwards, processes them in parallel using threads,
    and returns a dictionary mapping form types to lists of generated PDF paths.
    """
    pdf_files = {"10-K": [], "10-Q": []} # Initialize result dictionary

    # --- Input Validation ---
    if not cik.isdigit():
        log_lines.append(f"ERROR: Invalid CIK provided: '{cik}'. Must be numeric.")
        st.error(f"Invalid CIK provided: '{cik}'. Must be numeric.") # Show error in UI too
        return pdf_files
    cik_padded = cik.zfill(10) # Pad CIK for SEC URLs

    # --- Fetch Submissions Index ---
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    log_lines.append(f"Accessing SEC EDGAR index for CIK: {cik_padded}...")
    try:
        time.sleep(0.11) # Rate limit
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        submissions = r.json()
        log_lines.append("Successfully retrieved submission data.")
        # Auto-fill ticker if empty and available
        if not ticker and 'tickers' in submissions and submissions['tickers']:
             ticker = submissions['tickers'][0]
             log_lines.append(f"Note: Ticker not provided, using '{ticker}' found in SEC data.")

    except requests.exceptions.Timeout:
         log_lines.append(f"ERROR: Timeout connecting to SEC submissions URL: {submissions_url}")
         st.error("Timeout connecting to SEC EDGAR. Please try again later.")
         return pdf_files
    except requests.exceptions.RequestException as e:
        log_lines.append(f"ERROR: Could not retrieve submission data for CIK {cik_padded}: {str(e)}")
        err_msg = f"Could not retrieve data for CIK {cik_padded}. "
        if "404 Client Error" in str(e):
             err_msg += "Please double-check the CIK number."
             log_lines.append("-> Hint: Double-check the CIK number or company status.")
        else:
             err_msg += "Check network connection or try again later."
        st.error(err_msg)
        return pdf_files
    except Exception as e: # Catch other errors like JSON parsing
        log_lines.append(f"ERROR: Failed to process submission data: {str(e)}")
        st.error("Failed to process data from SEC EDGAR.")
        return pdf_files

    # --- Process Filings ---
    try:
        filings_data = submissions.get('filings', {}).get('recent', {})
        if not filings_data or 'accessionNumber' not in filings_data:
            log_lines.append("No recent filings found in the submission data.")
            st.warning("No recent filings found for this CIK.")
            return pdf_files

        # Extract filing details - ensure all lists exist and have same length
        accession_numbers = filings_data.get('accessionNumber', [])
        forms = filings_data.get('form', [])
        filing_dates = filings_data.get('filingDate', [])
        primary_documents = filings_data.get('primaryDocument', [])

        if not (len(accession_numbers) == len(forms) == len(filing_dates) == len(primary_documents)):
             log_lines.append("ERROR: Filing data arrays have inconsistent lengths. Cannot process.")
             st.error("Inconsistent data received from SEC EDGAR. Cannot process.")
             return pdf_files

        log_lines.append(f"Found {len(accession_numbers)} recent filings entries. Filtering and processing relevant ones...")

        tasks_to_submit = [] # List to hold details for tasks passing filters

        # --- Filter Filings BEFORE Submitting to Threads ---
        for i in range(len(accession_numbers)):
            form = forms[i]
            # Filter 1: Relevant forms only
            if form not in ["10-K", "10-Q"]:
                continue

            try:
                filing_date_str = filing_dates[i]
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                period = get_filing_period(form, filing_date, fy_month, fy_adjust)

                # Extract year suffix (YY) from the period string
                year_suffix = -1
                if period.startswith("FY"): year_suffix = int(period[2:])
                elif "Q" in period: year_suffix = int(period.split("Q")[-1])

                # Filter 2: Year Cutoff (Skip filings *before* EARLIEST_FISCAL_YEAR_SUFFIX)
                if 0 <= year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX:
                    log_lines.append(f"Reached filing period {period} (before FY{EARLIEST_FISCAL_YEAR_SUFFIX}). Stopping search for older filings.")
                    break # Stop iterating through older filings

                # Filter 3: Special FY17 Handling (Skip 10-Qs from FY17)
                if year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and form == "10-Q":
                    # log_lines.append(f"Skipping {period} ({form}) as per FY{EARLIEST_FISCAL_YEAR_SUFFIX} rule.") # Optional log
                    continue # Skip this filing

                # --- If all filters passed, prepare task details ---
                accession_raw = accession_numbers[i]
                accession_clean = accession_raw.replace('-', '')
                doc_filename = primary_documents[i]
                doc_url = f"{archive_base_url}{accession_clean}/{doc_filename}"

                tasks_to_submit.append({
                    "doc_url": doc_url, "cik": cik_padded, "form": form, "date": filing_date_str,
                    "accession": accession_clean, "ticker": ticker, "fy_month": fy_month,
                    "fy_adjust": fy_adjust, "cleanup_flag": cleanup_flag, "output_dir": tmp_dir
                })

            except (ValueError, TypeError) as e:
                 log_lines.append(f"Warning: Could not parse year/date for {accession_numbers[i]} (Period: {period}). Skipping filters for this filing. Error: {e}")
                 # Decide whether to still queue the task despite parsing error - safer to skip?
                 # For now, let's skip if period parsing fails.
                 continue
            except Exception as e:
                 log_lines.append(f"Warning: Error during period calculation/filtering for {accession_numbers[i]}: {e}. Skipping.")
                 continue

        log_lines.append(f"Identified {len(tasks_to_submit)} filings matching criteria to process.")
        if not tasks_to_submit:
            st.warning(f"No filings found matching the criteria (Form 10-K/10-Q, from FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K onwards).")
            return pdf_files

        # --- Execute Tasks in Parallel ---
        processed_success_count = 0
        # Limit workers to avoid excessive resource use / hitting rate limits harder
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit tasks
            futures = {executor.submit(download_and_process, log_lines=log_lines, **task_details): task_details for task_details in tasks_to_submit}

            # Retrieve results as they complete
            for future in as_completed(futures):
                task_info = futures[future] # Get corresponding task details
                try:
                    form_type, pdf_path = future.result() # result() re-raises exceptions from the worker
                    if pdf_path and form_type in pdf_files: # Check if PDF was created successfully
                        pdf_files[form_type].append(pdf_path)
                        processed_success_count += 1
                        # log_lines.append(f"Successfully processed: {os.path.basename(pdf_path)}") # Optional detailed log
                except Exception as e:
                    # Log exceptions that occurred within the worker threads
                    log_lines.append(f"ERROR: Task failed for {task_info['form']} {task_info['accession']}: {str(e)}")
                    # log_lines.append(traceback.format_exc()) # Uncomment for full traceback in logs

    except KeyError as e:
        log_lines.append(f"ERROR: Data format error in submissions JSON (missing key: {e}). Cannot process filings.")
        st.error("Data format error from SEC EDGAR.")
    except Exception as e:
         log_lines.append(f"ERROR: An unexpected error occurred during the main filing processing loop: {str(e)}")
         log_lines.append(traceback.format_exc())
         st.error("An unexpected error occurred during processing.")

    log_lines.append(f"Processing complete. Successfully generated {len(pdf_files['10-K'])} 10-K and {len(pdf_files['10-Q'])} 10-Q PDFs.")
    return pdf_files


def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    """
    Creates a ZIP archive containing the generated PDF files, organized by form type (10-K, 10-Q).
    The ZIP filename is now just the CIK number.
    """
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs:
        log_lines.append("No PDFs were generated, skipping ZIP creation.")
        return None

    # --- Updated ZIP Filename ---
    zip_filename = f"{cik}.zip" # Use only CIK for the filename
    zip_path = os.path.join(tmp_dir, zip_filename)
    log_lines.append(f"Creating ZIP archive '{zip_filename}' with {total_pdfs} PDF(s)...")

    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add files, organized into folders within the zip
            for form_type, paths in pdf_files.items():
                if not paths: continue
                for pdf_path in paths:
                    if pdf_path and os.path.exists(pdf_path):
                        # arcname determines the path inside the zip file (e.g., "10-K/NVDA_FY23.pdf")
                        arcname = os.path.join(form_type, os.path.basename(pdf_path))
                        zipf.write(pdf_path, arcname=arcname)
                        added_count += 1
                    else:
                         log_lines.append(f"Warning: Skipping missing/invalid PDF path during zipping: {pdf_path}")

        if added_count == total_pdfs:
             log_lines.append(f"ZIP archive '{zip_filename}' created successfully with {added_count} files.")
        else:
             log_lines.append(f"Warning: ZIP archive '{zip_filename}' created, but added only {added_count}/{total_pdfs} files due to issues.")
        return zip_path

    except Exception as e:
        log_lines.append(f"ERROR: Failed to create ZIP archive '{zip_filename}': {str(e)}")
        # Clean up potentially incomplete ZIP file
        if os.path.exists(zip_path):
            try: os.remove(zip_path)
            except OSError: pass
        return None

# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("📈 Mzansi EDGAR Fetcher")
# Updated description reflecting new logic
st.write(f"Fetch SEC 10-K and 10-Q filings (FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and all subsequent 10-Ks/10-Qs), convert them to PDF, and download as a ZIP archive.")
st.markdown(f"""
    **Instructions:**
    1. Enter the company's Central Index Key (CIK). [Find CIK here](https://www.sec.gov/edgar/searchedgar/cik).
    2. (Optional) Enter the stock ticker. If left blank, the app will try to find it.
    3. Select the company's Fiscal Year-End Month.
    4. Choose the Fiscal Year Basis (usually "Same Year"). Use "Previous Year" if filings seem mislabeled by a year.
    5. Click "Fetch Filings". Download link will appear upon completion. *Fetches FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and all newer 10-Ks/10-Qs.*
    6. (Optional) Check the box to delete intermediate HTML/asset files after conversion.
""")

# --- Input Form ---
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
            index=11, # Default to December (12)
            key="fy_month"
        )
        fy_adjust_input = st.selectbox(
            "Fiscal Year Basis:",
            ["Same Year", "Previous Year"],
            index=0,
            key="fy_adjust",
            help="Determines year label (e.g., FY23). 'Same Year' is standard. Use 'Previous Year' if FY labels seem off by one year."
            )

    cleanup_flag_input = st.checkbox(
        "Delete intermediate HTML/asset files after PDF conversion",
        value=False, # Default to keeping files
        key="cleanup",
        help="Check this to save space. Uncheck to keep intermediate files, useful if PDF conversion has issues."
        )

    submitted = st.form_submit_button("🚀 Fetch Filings")

# --- Processing Logic ---
if submitted:
    if not cik_input or not cik_input.strip().isdigit():
        st.error("CIK is required and must be numeric.")
    else:
        cik_clean = cik_input.strip()
        # Ensure ticker is uppercase and handle empty string
        ticker_clean = ticker_input.strip().upper() if ticker_input else ""

        st.info(f"Processing request for CIK: {cik_clean}...")
        # Expander for logs, initially collapsed
        log_container = st.expander("Show Process Log", expanded=False)
        log_lines = [] # Initialize log list for this run

        # Use a temporary directory for all intermediate files
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_lines.append(f"Using temporary directory: {tmp_dir}")
            # Show spinner during processing
            with st.spinner(f"Fetching data (from FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K onwards), converting files, and creating ZIP..."):
                # Run the main processing function
                pdf_files_dict = process_filing(
                    cik=cik_clean,
                    ticker=ticker_clean,
                    fy_month=fy_month_input,
                    fy_adjust=fy_adjust_input,
                    cleanup_flag=cleanup_flag_input,
                    log_lines=log_lines,
                    tmp_dir=tmp_dir
                )

                # --- Create and Offer ZIP Download ---
                if any(pdf_files_dict.values()): # Check if any PDFs were actually created
                    zip_path = create_zip_archive(
                        pdf_files=pdf_files_dict,
                        cik=cik_clean, # Use the cleaned CIK for the filename
                        log_lines=log_lines,
                        tmp_dir=tmp_dir
                    )

                    if zip_path and os.path.exists(zip_path):
                        st.success("✅ Success! Filings processed and zipped.")
                        try:
                            # Read zip file data for download button
                            with open(zip_path, "rb") as f:
                                zip_data = f.read()
                            # Offer download
                            st.download_button(
                                label=f"⬇️ Download {os.path.basename(zip_path)}",
                                data=zip_data,
                                file_name=os.path.basename(zip_path), # Filename for user (e.g., 1018724.zip)
                                mime="application/zip"
                            )
                        except Exception as e:
                             st.error(f"Error reading ZIP file for download: {e}")
                             log_lines.append(f"ERROR: Error reading ZIP file for download: {e}")
                    else:
                        # Log indicates failure reason
                        st.error("❌ Failed to create the final ZIP archive.")
                else:
                    # Log indicates reason (no filings found or processing failed)
                    st.warning("⚠️ No relevant filings were successfully processed into PDFs based on the criteria.")

        # Display the log output inside the expander
        with log_container:
            st.text_area("Log Output:", "\n".join(log_lines), height=400)

# --- Footer ---
st.markdown("---")
st.caption(f"Mzansi EDGAR Fetcher v1.2 | Data sourced from SEC EDGAR | Fetches FY{EARLIEST_FISCAL_YEAR_SUFFIX} 10-K and newer filings.")
