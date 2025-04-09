# streamlit_app.py
import os
import sys
import requests
# import subprocess # No longer needed
import tempfile
import zipfile
import time # Potentially useful for explicit rate limiting if needed, but not added yet
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
    'User-Agent': 'Mzansi EDGAR Viewer (support@example.com)' # Good practice to identify your bot
}

# CHROME_PATH dictionary and get_chrome_path function removed

session = requests.Session()
session.headers.update(HEADERS)
DEFAULT_TIMEOUT = 10  # seconds for requests

# -------------------------
# Backend Functions
# -------------------------

# get_chrome_path function removed

def get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust):
    """Determines the fiscal period string (e.g., FY23, 1Q24) based on filing date and fiscal year end."""
    if fiscal_year_end_month != 12:
        # Non-December fiscal year end logic
        reported_year = filing_date.year if filing_date.month > fiscal_year_end_month else filing_date.year - 1
        if fy_adjust == "Previous Year":
            reported_year -= 1

        if form == "10-K":
            return f"FY{reported_year % 100:02d}"
        elif form == "10-Q":
            # Complex logic for non-December FYE quarters
            if fiscal_year_end_month == 3: # Special case for March FYE
                if 4 <= filing_date.month <= 6: quarter = 4; year = reported_year # Q4 filed Apr-Jun
                elif 7 <= filing_date.month <= 9: quarter = 1; year = reported_year + 1 # Q1 filed Jul-Sep
                elif 10 <= filing_date.month <= 12: quarter = 2; year = reported_year + 1 # Q2 filed Oct-Dec
                elif 1 <= filing_date.month <= 3: quarter = 3; year = reported_year + 1 # Q3 filed Jan-Mar
                else: # Should not happen with valid months
                    return f"Q?{reported_year % 100:02d}" # Fallback
                return f"{quarter}Q{year % 100:02d}"
            else:
                # General case for non-December, non-March FYE
                # Calculate months passed since FYE start month
                months_since_fye_start = (filing_date.month - fiscal_year_end_month - 1 + 12) % 12
                quarter = (months_since_fye_start // 3) + 1
                year = reported_year + 1 if filing_date.month > fiscal_year_end_month else reported_year # Year increments after FYE month passes
                return f"{quarter}Q{year % 100:02d}"
        else: # Default for other forms if needed
             return f"FY{reported_year % 100:02d}"
    else:
        # Standard December fiscal year end logic
        if form == "10-K":
            fiscal_year = filing_date.year -1 # 10-K reports on the *previous* year
            if fy_adjust == "Previous Year": # Adjust further back if requested
                 fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"
        elif form == "10-Q":
            month = filing_date.month
            # Determine quarter based on filing month (approximates end of reporting period)
            if 1 <= month <= 3: quarter = 4; report_year = filing_date.year - 1 # Q4 (Oct-Dec) filed Jan-Mar
            elif 4 <= month <= 6: quarter = 1; report_year = filing_date.year     # Q1 (Jan-Mar) filed Apr-Jun
            elif 7 <= month <= 9: quarter = 2; report_year = filing_date.year     # Q2 (Apr-Jun) filed Jul-Sep
            elif 10 <= month <= 12: quarter = 3; report_year = filing_date.year    # Q3 (Jul-Sep) filed Oct-Dec
            else: # Should not happen
                return f"Q?{(filing_date.year -1) % 100:02d}"

            fiscal_year = report_year # For Dec FYE, fiscal year matches report year
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"{quarter}Q{fiscal_year % 100:02d}"
        else: # Default for other forms
            fiscal_year = filing_date.year - 1
            if fy_adjust == "Previous Year":
                fiscal_year -= 1
            return f"FY{fiscal_year % 100:02d}"


def download_assets(soup, base_url, output_dir, log_lines):
    """Downloads assets (images, CSS) linked in the HTML and updates links."""
    downloaded_assets = []
    for tag in soup.find_all(['img', 'link', 'script']):
        url_attr = None
        if tag.name == 'img' and tag.get('src'):
            url_attr = 'src'
        elif tag.name == 'link' and tag.get('href') and tag.get('rel') == ['stylesheet']: # Only download stylesheets
            url_attr = 'href'
        elif tag.name == 'script' and tag.get('src'):
            url_attr = 'src'
        # Add other tags/attributes if needed (e.g., <video src="...">)

        if not url_attr:
            continue

        asset_url = tag.get(url_attr)
        if not asset_url:
            continue

        # Create absolute URL, handling potential base tags if necessary (not implemented here)
        absolute_url = urljoin(base_url, asset_url)

        try:
            parsed = urlparse(absolute_url)
            if parsed.scheme not in ['http', 'https']:
                log_lines.append(f"Skipping non-HTTP(S) asset: {asset_url}")
                continue

            # Avoid re-downloading
            filename = os.path.basename(parsed.path)
            if not filename: # Skip if URL has no filename part
                 filename = f"asset_{len(downloaded_assets) + 1}.{asset_url.split('.')[-1]}" # Create generic filename


            local_path = os.path.join(output_dir, filename)
            if filename in downloaded_assets:
                tag[url_attr] = filename # Ensure link is updated even if already downloaded
                continue

            # Introduce small delay to respect SEC rate limits (10 req/sec)
            time.sleep(0.11) # Sleep for slightly more than 1/10th of a second

            r = session.get(absolute_url, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status() # Check for download errors (4xx, 5xx)

            with open(local_path, 'wb') as f:
                f.write(r.content)

            # Update the tag's attribute to point to the local file
            tag[url_attr] = filename
            downloaded_assets.append(filename)
            log_lines.append(f"Downloaded asset: {filename}")

        except requests.exceptions.RequestException as e:
            log_lines.append(f"Asset download error for {absolute_url}: {str(e)}")
        except IOError as e:
            log_lines.append(f"Asset file write error for {filename}: {str(e)}")
        except Exception as e:
            log_lines.append(f"General asset error for {absolute_url}: {str(e)}")

    return downloaded_assets


def convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month_idx, fy_adjust, log_lines):
    """Converts the local HTML file to PDF using xhtml2pdf."""
    # --- Generate PDF Filename (same logic as before) ---
    try:
        filing_date = datetime.strptime(date, "%Y-%m-%d")
        fiscal_year_end_month = int(fy_month_idx)
        period = get_filing_period(form, filing_date, fiscal_year_end_month, fy_adjust)
        base_name = f"{ticker}_{period}" if ticker else f"{cik}_{period}" # Use CIK if ticker is absent
        pdf_filename = f"{base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        log_lines.append(f"Attempting PDF conversion: {pdf_filename}")

        # --- Conversion using xhtml2pdf ---
        with open(html_path, "r", encoding="utf-8") as source_html_file, \
             open(pdf_path, "w+b") as result_file:

            # Define a callback function to resolve local file paths for assets
            base_dir = os.path.dirname(html_path)
            def link_callback(uri, rel):
                # Resolve relative paths for images, css etc.
                res = os.path.join(base_dir, uri)
                if os.path.exists(res):
                    return res
                # Potentially handle web paths if needed, but primary goal is local files
                # log_lines.append(f"Could not resolve link: {uri}")
                return uri # Return original if not found locally

            # Convert HTML to PDF
            pisa_status = pisa.CreatePDF(
                src=source_html_file,       # Use the opened file object
                dest=result_file,           # Use the opened file object
                encoding='utf-8',
                link_callback=link_callback # Help find local assets
            )

        # --- Check Conversion Result ---
        if pisa_status.err:
            log_lines.append(f"xhtml2pdf conversion failed for {pdf_filename}: Error {pisa_status.err}")
            # Try to clean up potentially corrupted PDF file
            if os.path.exists(pdf_path):
                try:
                    os.remove(pdf_path)
                except OSError as e:
                    log_lines.append(f"Could not remove failed PDF {pdf_filename}: {e}")
            return None
        elif os.path.exists(pdf_path):
            log_lines.append(f"PDF created successfully: {pdf_filename}")
            return pdf_path
        else:
            log_lines.append(f"xhtml2pdf conversion failed - no output file generated for {pdf_filename}")
            return None

    except FileNotFoundError:
        log_lines.append(f"HTML file not found for PDF conversion: {html_path}")
        return None
    except Exception as e:
        log_lines.append(f"General error during PDF conversion ({pdf_filename}): {str(e)}")
        # Clean up potentially corrupt PDF if conversion failed mid-process
        if 'pdf_path' in locals() and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except OSError as e:
                 log_lines.append(f"Could not remove failed PDF {pdf_filename} during cleanup: {e}")
        return None


def cleanup_files(html_path, assets, output_dir, log_lines):
    """Removes the temporary HTML file and downloaded assets."""
    try:
        if html_path and os.path.exists(html_path):
            os.remove(html_path)
            log_lines.append(f"Cleaned HTML file: {os.path.basename(html_path)}")
        else:
            log_lines.append("Skipped cleaning non-existent HTML file.")

        cleaned_assets_count = 0
        for asset in assets:
            asset_path = os.path.join(output_dir, asset)
            if os.path.exists(asset_path):
                try:
                    os.remove(asset_path)
                    # log_lines.append(f"Cleaned asset: {asset}") # Optional: can make log noisy
                    cleaned_assets_count += 1
                except OSError as e:
                     log_lines.append(f"Error cleaning asset {asset}: {e}")
        if cleaned_assets_count > 0:
             log_lines.append(f"Cleaned {cleaned_assets_count} asset file(s).")

    except Exception as e:
        log_lines.append(f"Error during file cleanup: {str(e)}")


def download_and_process(doc_url, cik, form, date, accession, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, output_dir):
    """Downloads HTML, processes assets, converts to PDF, and optionally cleans up."""
    html_path = None
    downloaded_assets = []
    try:
        log_lines.append(f"Processing {form} from {date} ({accession})")

        # Introduce small delay before primary document download
        time.sleep(0.11)

        r = session.get(doc_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status() # Check for HTTP errors

        # Define HTML filename (unique per filing)
        base_html_filename = f"{cik}_{form}_{date}_{accession}.htm" # Use .htm consistently
        # Sanitize filename if needed (though accession/date/cik/form should be safe)
        html_path = os.path.join(output_dir, base_html_filename)

        # Decode considering potential issues, replace common errors
        try:
             decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
             log_lines.append(f"UTF-8 decode failed for {accession}, trying 'latin-1' fallback.")
             decoded_text = r.content.decode('latin-1', errors='replace') # Common fallback

        # Simple replacements for common display issues before parsing
        replacements = {
            "â€\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—" , "&nbsp;": " "
            # Add more if specific issues are observed
        }
        for wrong, correct in replacements.items():
            decoded_text = decoded_text.replace(wrong, correct)

        # Parse with BeautifulSoup
        soup = BeautifulSoup(decoded_text, 'html.parser')

        # Ensure UTF-8 meta tag is present for better rendering consistency
        if not soup.find('meta', charset=True):
            meta_tag = soup.new_tag('meta', charset='UTF-8')
            if soup.head:
                soup.head.insert(0, meta_tag)
            else:
                # Create head if it doesn't exist (unlikely but possible)
                head = soup.new_tag('head')
                head.append(meta_tag)
                if soup.html:
                    soup.html.insert(0, head)
                else:
                    # If no <html> tag, prepend to document (very unlikely)
                    soup.insert(0, head)

        # Download linked assets (CSS, images) and update links to local paths
        downloaded_assets = download_assets(soup, doc_url, output_dir, log_lines)

        # Save the modified HTML (with local asset links)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
        log_lines.append(f"Saved processed HTML: {base_html_filename}")

        # Convert the saved HTML to PDF
        pdf_path = convert_to_pdf(html_path, form, date, accession, cik, ticker, fy_month, fy_adjust, log_lines)

        # Return the path to the PDF if successful
        return (form, pdf_path) # Return pdf_path (which is None if conversion failed)

    except requests.exceptions.RequestException as e:
        log_lines.append(f"Download error for {doc_url}: {str(e)}")
    except IOError as e:
         log_lines.append(f"File writing error during processing: {str(e)}")
    except Exception as e:
        log_lines.append(f"Unexpected error processing {form} {accession}: {str(e)}")
    finally:
        # Cleanup HTML and assets *only if* PDF was created *and* cleanup flag is true
        # We get pdf_path from the return value of convert_to_pdf in the main try block
        if 'pdf_path' in locals() and pdf_path and cleanup_flag and html_path:
             log_lines.append("Cleanup flag is ON and PDF was created, cleaning intermediate files.")
             cleanup_files(html_path, downloaded_assets, output_dir, log_lines)
        elif 'pdf_path' in locals() and not pdf_path and html_path:
             log_lines.append("PDF conversion failed, intermediate files kept for debugging.")
        elif cleanup_flag and not ('pdf_path' in locals() and pdf_path):
             # If processing failed before PDF conversion but cleanup is on, still try cleanup
             log_lines.append("Processing failed before PDF conversion, attempting cleanup as flag is ON.")
             cleanup_files(html_path, downloaded_assets, output_dir, log_lines)


    # Return None for pdf_path if any exception occurred before successful conversion
    return (form, None)


def process_filing(cik, ticker, fy_month, fy_adjust, cleanup_flag, log_lines, tmp_dir):
    """Fetches filing data, processes documents in parallel, and returns paths to created PDFs."""
    pdf_files = {"10-K": [], "10-Q": []} # Dictionary to hold lists of PDF paths per form type
    MAX_FILINGS_TO_PROCESS = 50 # Limit the number of filings processed to avoid excessive runtime/requests

    if not cik.isdigit():
        log_lines.append(f"ERROR: Invalid CIK provided: '{cik}'. Must be numeric.")
        return pdf_files # Return empty lists

    cik_padded = cik.zfill(10) # Pad CIK for SEC URLs

    # SEC EDGAR URLs
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"

    log_lines.append(f"Accessing SEC EDGAR index for CIK: {cik_padded}...")
    try:
        # Introduce delay before hitting the submissions JSON endpoint
        time.sleep(0.11)
        r = session.get(submissions_url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status() # Check for 4xx/5xx errors
        submissions = r.json()
        log_lines.append("Successfully retrieved submission data.")
        # Extract ticker if not provided and available in submissions data
        if not ticker and 'tickers' in submissions and submissions['tickers']:
             ticker = submissions['tickers'][0] # Use the first ticker found
             log_lines.append(f"Ticker not provided, using '{ticker}' found in SEC data.")

    except requests.exceptions.Timeout:
         log_lines.append(f"ERROR: Timeout connecting to SEC submissions URL: {submissions_url}")
         return pdf_files
    except requests.exceptions.RequestException as e:
        log_lines.append(f"ERROR: Could not retrieve submission data for CIK {cik_padded}: {str(e)}")
        # Common issue: CIK might be wrong or company delisted/merged.
        if "404 Client Error" in str(e):
             log_lines.append("-> Hint: Double-check the CIK number.")
        return pdf_files # Return empty if index fails
    except Exception as e: # Catch other potential errors like JSON parsing
        log_lines.append(f"ERROR: Failed to process submission data: {str(e)}")
        return pdf_files

    try:
        # Navigate the JSON structure to get recent filings
        filings = submissions.get('filings', {}).get('recent', {})
        if not filings or 'accessionNumber' not in filings:
            log_lines.append("No recent filings found in the submission data.")
            return pdf_files

        # Get lists of data - check if they exist and have same length
        accession_numbers = filings.get('accessionNumber', [])
        forms = filings.get('form', [])
        filing_dates = filings.get('filingDate', [])
        primary_documents = filings.get('primaryDocument', [])

        if not (len(accession_numbers) == len(forms) == len(filing_dates) == len(primary_documents)):
             log_lines.append("ERROR: Filing data arrays have inconsistent lengths. Cannot process.")
             return pdf_files

        log_lines.append(f"Found {len(accession_numbers)} recent filings. Processing relevant ones...")

        tasks = []
        processed_count = 0
        # Use ThreadPoolExecutor for parallel downloads/processing
        # Limit workers to avoid hitting rate limits too hard & manage resources
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Iterate through filings, most recent first
            for i in range(len(accession_numbers)):
                form = forms[i]
                # Filter for desired forms
                if form not in ["10-K", "10-Q"]:
                    continue

                # Basic check to prevent processing too many filings
                if processed_count >= MAX_FILINGS_TO_PROCESS:
                     log_lines.append(f"Reached processing limit ({MAX_FILINGS_TO_PROCESS}). Stopping.")
                     break

                try:
                    filing_date_str = filing_dates[i]
                    accession_raw = accession_numbers[i]
                    accession_clean = accession_raw.replace('-', '') # For URL path
                    doc_filename = primary_documents[i]

                    # Construct the URL to the primary filing document
                    doc_url = f"{archive_base_url}{accession_clean}/{doc_filename}"

                    # Submit the download/processing task to the thread pool
                    tasks.append(executor.submit(
                        download_and_process,
                        doc_url,
                        cik_padded, # Use padded CIK for consistency
                        form,
                        filing_date_str,
                        accession_clean,
                        ticker, # Pass ticker (even if empty)
                        fy_month,
                        fy_adjust,
                        cleanup_flag,
                        log_lines, # Pass log list (needs thread-safety if written directly, but append is mostly safe)
                        tmp_dir # Pass the temporary directory path
                    ))
                    processed_count += 1

                except Exception as e:
                    log_lines.append(f"ERROR: Failed to queue filing index {i} ({accession_raw}): {str(e)}")

            log_lines.append(f"Queued {len(tasks)} tasks for processing.")

            # Retrieve results as they complete
            for future in as_completed(tasks):
                try:
                    # Get the result from the completed future
                    form_type, pdf_path = future.result() # result() will raise exceptions from the worker thread
                    if pdf_path and form_type in pdf_files: # Check if PDF was created successfully
                        pdf_files[form_type].append(pdf_path)
                        # log_lines.append(f"Successfully processed and got PDF: {os.path.basename(pdf_path)}") # Optional detailed log
                except Exception as e:
                    # Log exceptions that occurred within the worker threads
                    log_lines.append(f"ERROR: A processing task failed: {str(e)}")

    except KeyError as e:
        log_lines.append(f"ERROR: Data format error in submissions JSON (missing key: {e}). Cannot process filings.")
    except Exception as e:
         log_lines.append(f"ERROR: An unexpected error occurred during filing processing loop: {str(e)}")

    log_lines.append(f"Processing complete. Found {len(pdf_files['10-K'])} 10-K PDFs and {len(pdf_files['10-Q'])} 10-Q PDFs.")
    return pdf_files


def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    """Creates a ZIP archive containing the generated PDF files, organized by form type."""
    if not any(pdf_files.values()):
        log_lines.append("No PDFs were generated, skipping ZIP creation.")
        return None

    zip_filename = f"{cik}_filings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    log_lines.append(f"Creating ZIP archive: {zip_filename}")

    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for form_type, paths in pdf_files.items():
                if not paths: continue # Skip if no PDFs for this form type
                log_lines.append(f"Adding {len(paths)} file(s) to folder '{form_type}' in ZIP...")
                for pdf_path in paths:
                    if pdf_path and os.path.exists(pdf_path):
                        # arcname determines the path inside the zip file
                        arcname = os.path.join(form_type, os.path.basename(pdf_path))
                        zipf.write(pdf_path, arcname=arcname)
                        # log_lines.append(f"-> Added {os.path.basename(pdf_path)}") # Optional detailed log
                    else:
                         log_lines.append(f"Skipping missing PDF file: {pdf_path}")

        log_lines.append(f"ZIP archive created successfully: {zip_filename}")
        return zip_path

    except Exception as e:
        log_lines.append(f"ERROR: Failed to create ZIP archive: {str(e)}")
        # Clean up potentially incomplete ZIP file
        if os.path.exists(zip_path):
            try:
                 os.remove(zip_path)
            except OSError:
                 pass # Ignore cleanup error
        return None

# -------------------------
# Streamlit UI (Unchanged)
# -------------------------
st.set_page_config(page_title="Mzansi EDGAR Fetcher", layout="wide")
st.title("📈 Mzansi EDGAR Fetcher")
st.write("Fetch recent SEC 10-K and 10-Q filings for a company, convert them to PDF, and download as a ZIP archive.")
st.markdown("""
    **Instructions:**
    1. Enter the company's Central Index Key (CIK). [Find CIK here](https://www.sec.gov/edgar/searchedgar/cik).
    2. (Optional) Enter the stock ticker. If left blank, the app will try to find it.
    3. Select the company's Fiscal Year-End Month.
    4. Choose the Fiscal Year Basis (usually "Same Year"). Use "Previous Year" if filings seem mislabeled by a year.
    5. Click "Fetch Filings". Download link will appear upon completion.
    6. (Optional) Check the box to delete intermediate HTML files after conversion.
""")

with st.form("filing_form"):
    col1, col2 = st.columns(2)
    with col1:
        cik_input = st.text_input("Company CIK (e.g., 1018724 for NVIDIA):", key="cik")
        ticker_input = st.text_input("Ticker (Optional, e.g., NVDA):", key="ticker")
    with col2:
        # Create mapping for month number to name
        month_options = {str(i): datetime(2000, i, 1).strftime('%B') for i in range(1, 13)}
        fy_month_input = st.selectbox(
            "Fiscal Year-End Month:",
            options=list(month_options.keys()),
            format_func=lambda x: f"{month_options[x]} ({x})", # Show name and number
            index=11, # Default to December (12)
            key="fy_month"
        )
        fy_adjust_input = st.selectbox(
            "Fiscal Year Basis:",
            ["Same Year", "Previous Year"],
            index=0, # Default to Same Year
            key="fy_adjust",
            help="Determines year label (e.g., FY23). 'Same Year' is standard. Use 'Previous Year' if FY labels seem off by one year."
            )

    cleanup_flag_input = st.checkbox(
        "Delete intermediate HTML/asset files after PDF conversion",
        value=False, # Default to keeping files for debugging
        key="cleanup",
        help="Check this to save space, uncheck to keep HTML files if PDF conversion fails or for inspection."
        )

    submitted = st.form_submit_button("🚀 Fetch Filings")

if submitted:
    if not cik_input or not cik_input.strip().isdigit():
        st.error("CIK is required and must be numeric.")
    else:
        cik_clean = cik_input.strip()
        ticker_clean = ticker_input.strip().upper() if ticker_input else "" # Standardize ticker

        st.info(f"Processing request for CIK: {cik_clean}...")
        log_container = st.expander("Show Process Log", expanded=False)
        log_lines = [] # Initialize list to store log messages

        # Use a temporary directory which is automatically cleaned up
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_lines.append(f"Using temporary directory: {tmp_dir}")
            with st.spinner("Fetching data, converting files, and creating ZIP... This may take a minute or two."):
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

                # Create ZIP archive if PDFs were generated
                if any(pdf_files_dict.values()): # Check if any PDFs were created
                    zip_path = create_zip_archive(
                        pdf_files=pdf_files_dict,
                        cik=cik_clean, # Use the cleaned CIK
                        log_lines=log_lines,
                        tmp_dir=tmp_dir
                    )

                    if zip_path and os.path.exists(zip_path):
                        st.success("✅ Success! Filings processed and zipped.")
                        try:
                            with open(zip_path, "rb") as f:
                                zip_data = f.read()
                            st.download_button(
                                label=f"⬇️ Download {os.path.basename(zip_path)}",
                                data=zip_data,
                                file_name=os.path.basename(zip_path),
                                mime="application/zip"
                            )
                        except Exception as e:
                             st.error(f"Error reading ZIP file for download: {e}")
                             log_lines.append(f"Error reading ZIP file for download: {e}")
                    else:
                        st.error("❌ Failed to create or find the final ZIP archive.")
                        log_lines.append("Failed to create/find ZIP archive.")
                else:
                    st.warning("⚠️ No relevant 10-K or 10-Q filings were successfully processed into PDFs.")
                    log_lines.append("No PDFs were created, so no ZIP file generated.")

        # Display the log output
        with log_container:
            st.text_area("Log Output:", "\n".join(log_lines), height=300)

# Add a footer or additional info if desired
st.markdown("---")
st.caption("Mzansi EDGAR Fetcher | Data sourced from SEC EDGAR | Use responsibly.")
