# app.py
import os
import sys
import requests
import tempfile
import threading
import zipfile
from datetime import datetime, date, timedelta # Added date and timedelta
from io import BytesIO
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import traceback
import time
import mimetypes
import pathlib # For robust path/URI handling
import asyncio # Required for Playwright async (if using async version)
import re # For cleaning styles (though not used in this version)
import platform # Required for get_chrome_path
import multiprocessing # Added for cpu_count
# <<< Added imports for connection pooling >>>
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
# <<< Added import for period inference >>>
from dateutil.relativedelta import relativedelta
# <<< Added import for calendar & defaultdict >>>
import calendar
from collections import defaultdict
from typing import Dict, List, Tuple, Optional # Added Optional for type hinting
import csv # <<< Added for CSV report generation >>>


# Ensure required libraries are installed:
# pip install Flask waitress beautifulsoup4 requests playwright lxml html5lib python-dateutil urllib3
try:
    from bs4 import BeautifulSoup
    from flask import Flask, request, render_template_string, send_file, url_for, jsonify
    from waitress import serve
    # --- Use Playwright Sync API ---
    from playwright.sync_api import sync_playwright, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
    # --- End Playwright Import ---
except ModuleNotFoundError as e:
    print(f"Error: Required library not found ({e.name}). Please install requirements.")
    print("Try: pip install Flask waitress beautifulsoup4 requests playwright lxml html5lib python-dateutil urllib3")
    sys.exit(1)


# --- Constants and Configuration ---
# <<< User-Agent Configuration >>>
DEFAULT_USER_AGENT = 'MzansiApp/1.0 (research@cognitivecredit.com)' # Updated default email
USER_AGENT = os.getenv('EDGAR_USER_AGENT', DEFAULT_USER_AGENT)
print(f"--- Using EDGAR User-Agent: {USER_AGENT} ---") # Make it visible
# <<< Removed warning print block >>>
HEADERS = {'User-Agent': USER_AGENT }
# <<< End User-Agent Configuration >>>

# Paths to Chrome executable based on OS
CHROME_PATH = {
    'windows': 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'darwin': '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'linux': '/usr/bin/google-chrome'
}

# Create a global requests session
session = requests.Session()
# <<< Configure connection pool size >>>
adapter = HTTPAdapter(pool_connections=10, pool_maxsize=50) # Increased pool_maxsize
session.mount('http://', adapter)
session.mount('https://', adapter)
# <<< End pool configuration >>>
session.headers.update(HEADERS) # Apply the determined User-Agent
DEFAULT_TIMEOUT = 20

# --- NEW: Expected FY 8-K Filing Months based on user's table ---
# Key: Fiscal Year End Month (int)
# Value: Tuple (Expected Filing Month 1, Expected Filing Month 2) for the FY report
EXPECTED_FY_FILING_MONTHS = {
    1: (2, 3),    # Jan FYE -> Feb, Mar filing for that FY
    2: (3, 4),    # Feb FYE -> Mar, Apr filing
    3: (4, 5),    # Mar FYE -> Apr, May filing
    4: (5, 6),    # Apr FYE -> May, Jun filing
    5: (6, 7),    # May FYE -> Jun, Jul filing
    6: (7, 8),    # Jun FYE -> Jul, Aug filing
    7: (8, 9),    # Jul FYE -> Aug, Sep filing
    8: (9, 10),  # Aug FYE -> Sep, Oct filing
    9: (10, 11), # Sep FYE -> Oct, Nov filing
    10: (11, 12),# Oct FYE -> Nov, Dec filing
    11: (12, 1), # Nov FYE -> Dec (same year), Jan (next year) filing
    12: (1, 2)    # Dec FYE -> Jan (next year), Feb (next year) filing
}


# --- Flask App Setup ---
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "local-dev-secret-key")
app.config['FILE_STORAGE'] = {}
file_storage_lock = threading.Lock()

# <<< Global dictionary for progress reporting >>>
progress_data = {"text": "", "total": 0, "current": 0}
progress_lock = threading.Lock()

# --- HTML Template ---
# <<< Added progress bar and timer elements and logic >>>
FORM_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mzansi</title>
    <link rel="shortcut icon" href="{{ url_for('static', filename='favicon.ico') }}">
    <style>
        :root {
            --primary-color: #005ea2;
            --secondary-color: #e4edf5;
            --accent-color: #00b0f0;
            --success-color: #28a745;
            --success-hover: #218838;
            --info-color: #17a2b8; /* Color for the report button */
            --info-hover: #138496; /* Hover color for report button */
            --text-color: #333;
            --border-color: #ccc;
            --container-bg: #ffffff;
            --log-bg: #f8f9fa;
            --log-border: #e9ecef;
            --button-hover: #003e70;
            --input-focus-border: #80bdff;
            --input-focus-shadow: 0 0 0 0.2rem rgba(0, 123, 255, 0.25);
            --placeholder-color: #999; /* Placeholder text color */
            --link-color: #007bff; /* Standard link blue */
            --error-color: #dc3545; /* Error color */
            --error-hover: #c82333;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 20px;
            background-color: var(--secondary-color);
            color: var(--text-color);
            line-height: 1.6;
        }
        .container {
            background-color: var(--container-bg);
            padding: 30px 40px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            max-width: 800px;
            margin: 30px auto;
        }
        .header {
            display: flex;
            align-items: center;
            border-bottom: 2px solid var(--secondary-color);
            padding-bottom: 15px;
            margin-bottom: 25px;
        }
        .logo {
            max-height: 50px;
            margin-right: 15px;
        }
        h2 {
            color: var(--primary-color);
            margin: 0;
            font-weight: 600;
        }
        .instructions {
            background-color: var(--secondary-color);
            padding: 15px 20px;
            border-radius: 5px;
            margin-bottom: 25px;
            font-size: 0.9em;
            border-left: 4px solid var(--primary-color);
        }
        .instructions strong { color: var(--primary-color); }
        .instructions ol {
            margin: 5px 0 0 0;
            padding-left: 25px;
        }
         .instructions li { margin-bottom: 3px; }
        .form-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px 30px;
            margin-bottom: 20px;
        }
        .form-options {
             margin-top: 10px;
             padding-top: 15px;
             border-top: 1px solid var(--secondary-color);
             display: grid; /* Use grid for options too */
             grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
             gap: 20px 30px;
             align-items: start; /* <<< Added for better vertical alignment */
        }
        .checkbox-container {
            display: flex;
            align-items: center;
            cursor: pointer;
            padding-top: 5px; /* Adjust as needed */
        }
        .checkbox-container input[type="checkbox"] {
             margin-right: 10px;
             width: 16px;
             height: 16px;
             cursor: pointer;
             margin-top: -2px; /* Fine-tune alignment */
        }
        .checkbox-container label {
             margin-bottom: 0;
             font-weight: normal;
             font-size: 0.95em;
             cursor: pointer;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            font-size: 0.95em;
            color: #444;
        }
        label a.helper-link {
            font-size: 0.8em;
            font-weight: 400;
            margin-left: 8px;
            color: var(--link-color);
            text-decoration: none;
        }
         label a.helper-link:hover {
            text-decoration: underline;
        }
        input[type="text"], select {
            width: 100%;
            padding: 12px;
            margin-bottom: 18px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            box-sizing: border-box;
            transition: border-color 0.2s ease, box-shadow 0.2s ease;
            color: var(--text-color);
        }
        /* Style for invalid CIK input */
        input[type="text"]:invalid {
             border-color: var(--error-color);
             box-shadow: 0 0 0 0.2rem rgba(220, 53, 69, 0.25);
        }
        input[type="text"]:focus, select:focus {
            border-color: var(--input-focus-border);
            outline: 0;
            box-shadow: var(--input-focus-shadow);
        }
        select:required:invalid {
            color: var(--placeholder-color);
        }
        select option {
            color: var(--text-color);
        }
        select:valid {
             color: var(--text-color);
        }
        .button {
            background-color: var(--primary-color);
            color: white;
            padding: 12px 25px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1.05em;
            font-weight: 500;
            transition: background-color 0.2s ease, transform 0.1s ease;
            margin-top: 25px; /* Reset margin */
            text-decoration: none;
            display: inline-block;
            text-align: center;
            margin-right: 10px; /* Add some space between buttons */
        }
        .button:hover {
            background-color: var(--button-hover);
            transform: translateY(-1px);
        }
        .button:disabled {
            background-color: #cccccc;
            cursor: not-allowed;
            transform: none;
        }
        .cancel-button {
            background-color: var(--error-color);
            display: none; /* Hidden by default */
        }
        .cancel-button:hover {
             background-color: var(--error-hover);
        }
        .download-link.button { /* For the main ZIP download */
            background-color: var(--success-color);
            font-weight: bold;
        }
        .download-link.button:hover {
            background-color: var(--success-hover);
        }
        .report-link.button { /* For the CSV report download */
            background-color: var(--info-color);
            font-weight: bold;
        }
        .report-link.button:hover {
            background-color: var(--info-hover);
        }
        details {
            margin-top: 30px;
            border: 1px solid var(--log-border);
            border-radius: 5px;
            background-color: var(--container-bg);
        }
        summary {
            padding: 15px;
            font-weight: 600;
            cursor: pointer;
            color: var(--primary-color);
            background-color: var(--log-bg);
            border-bottom: 1px solid var(--log-border);
            border-radius: 5px 5px 0 0;
            transition: background-color 0.2s ease;
            list-style: none;
        }
        summary:hover { background-color: #e9ecef; }
        summary::-webkit-details-marker { display: none; }
        summary::marker { display: none; }
        summary:before {
            content: '▶';
            margin-right: 10px;
            font-size: 0.8em;
            display: inline-block;
            transition: transform 0.2s ease-in-out;
            color: #666;
        }
        details[open] > summary:before {
            transform: rotate(90deg);
        }
        details[open] > summary {
             border-bottom: 1px solid var(--log-border);
             border-radius: 5px 5px 0 0;
        }
        .log {
            padding: 15px;
            font-family: Consolas, 'Courier New', monospace;
            font-size: 0.85em;
            white-space: pre-wrap;
            word-wrap: break-word;
            max-height: 400px;
            overflow-y: auto;
            background-color: var(--log-bg);
            border-top: none;
            border-radius: 0 0 5px 5px;
        }
        #downloadArea {
            margin-top: 25px;
            margin-bottom: 25px;
            min-height: 50px;
            text-align: center;
        }
        .error { color: var(--error-color); font-weight: bold; }
        .success { color: #28a745; font-weight: bold; }
        .validation-message {
             font-size: 0.85em;
             color: var(--error-color);
             margin-top: -10px; /* Adjust spacing */
             margin-bottom: 10px;
             display: none; /* Hidden by default */
        }
        /* Progress Bar and Timer Styles */
        #progressContainer {
            display: none;
            margin-top: 20px;
        }
        #timer {
            font-size: 0.9em;
            color: #555;
            text-align: right;
            margin-bottom: 5px;
        }
        .progress-bar-outer {
            width: 100%;
            height: 20px;
            background-color: var(--secondary-color);
            border-radius: 10px;
            overflow: hidden;
            border: 1px solid #ddd;
        }
        .progress-bar-inner {
            height: 100%;
            width: 0%;
            background-color: var(--primary-color);
            border-radius: 10px;
            transition: width 0.4s ease-in-out;
            text-align: center;
            color: white;
            font-size: 0.8em;
            line-height: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="{{ url_for('static', filename='images/logo.png') }}" alt="Logo" class="logo" onerror="this.style.display='none'">
            <h2>Mzansi</h2>
        </div>
        <div class="instructions">
            <strong>Instructions:</strong>
            <ol>
                <li>Enter the company's Central Index Key (CIK).</li>
                <li>(Optional) Enter the stock ticker.</li>
                <li>Select Filing Type (10K/Q or 8K Press Releases).</li>
                <li>Select the company's Fiscal Year End (required for all types).</li>
                <li>Choose the Fiscal Year Basis (usually "Same Year").</li>
                <li>Click "Fetch Filings".</li>
                <li>Processing may take a few minutes. Check log below for progress.</li>
            </ol>
        </div>
        <form id="mainForm">
            <div class="form-grid">
                <div> <label for="cik">Company CIK (10 digits only):
                        <a href="https://www.sec.gov/edgar/search-and-access" target="_blank" rel="noopener noreferrer" class="helper-link">(Find CIK)</a>
                    </label>
                    <input type="text" id="cik" name="cik" required pattern="\\d{10}" title="Please enter exactly 10 digits" placeholder="e.g., 0001045810">
                    <div id="cikValidationMessage" class="validation-message">CIK must be exactly 10 digits.</div>
                    <label for="ticker">Ticker (optional):</label>
                    <input type="text" id="ticker" name="ticker" placeholder="e.g., NVDA">
                </div>
                <div> <label for="fy_month">Fiscal Year End:</label>
                    <select id="fy_month" name="fy_month" required>
                        <option value="" disabled selected>Select Fiscal Year End</option>
                        {% for num, month in months %}
                        <option value="{{ num }}">{{ month }}</option>
                        {% endfor %}
                    </select>
                    <label for="fy_adjust">Fiscal Year Basis:</label>
                    <select id="fy_adjust" name="fy_adjust">
                        <option value="Same Year">Same Year</option>
                        <option value="Previous Year">Previous Year</option>
                    </select>
                </div>
            </div>

            <div class="form-options">
                 <div> <label for="fetch_mode">Filing Type:</label>
                     <select id="fetch_mode" name="fetch_mode">
                         <option value="10K_10Q" selected>10K/Q Filings (FY17+)</option>
                         <option value="8K_Earnings">8K Press Releases (FY17+)</option> </select>
                </div>
                 <div> <div class="checkbox-container">
                         <input type="checkbox" id="latest_only" name="latest_only" value="true">
                         <label for="latest_only">Download only the most recent</label>
                    </div>
                </div>
            </div>

            <button type="button" class="button" id="fetchButton" onclick="handleSubmit()">Fetch Filings</button>
            <button type="button" class="button cancel-button" id="cancelButton" onclick="handleCancel()">Cancel</button>
        </form>
        <div id="downloadArea"></div>
        <div id="progressContainer">
            <div id="timer">Elapsed Time: 0s</div>
            <div class="progress-bar-outer">
                <div id="progressBar" class="progress-bar-inner">0%</div>
            </div>
        </div>
        <details class="log-container" open>
            <summary>Process Log</summary>
            <div id="log" class="log">Enter details above and click "Fetch Filings".</div>
        </details>
    </div>
    <script>
        const logDiv = document.getElementById('log');
        const downloadArea = document.getElementById('downloadArea');
        const fetchButton = document.getElementById('fetchButton');
        const cancelButton = document.getElementById('cancelButton');
        const fyMonthSelect = document.getElementById('fy_month');
        const cikInput = document.getElementById('cik');
        const cikValidationMessage = document.getElementById('cikValidationMessage');
        const progressContainer = document.getElementById('progressContainer');
        const progressBar = document.getElementById('progressBar');
        const timerDiv = document.getElementById('timer');

        let progressInterval = null;
        let timerInterval = null;
        let startTime = 0;

        // Set initial style for dropdown if no value is selected
        if (fyMonthSelect.value === "") {
             fyMonthSelect.style.color = getComputedStyle(document.documentElement).getPropertyValue('--placeholder-color').trim() || '#999';
        }
        fyMonthSelect.addEventListener('change', function() {
             if (this.value === "") {
                 this.style.color = getComputedStyle(document.documentElement).getPropertyValue('--placeholder-color').trim() || '#999';
             } else {
                 this.style.color = getComputedStyle(document.documentElement).getPropertyValue('--text-color').trim() || '#333';
             }
        });

        // CIK Input Validation Listener
        cikInput.addEventListener('input', function() {
            if (cikInput.checkValidity()) {
                cikValidationMessage.style.display = 'none';
            } else {
                cikValidationMessage.style.display = 'block';
            }
        });

        function confirmExitDuringProcess(event) {
            event.preventDefault();
            event.returnValue = '';
            return '';
        }

        function handleCancel() {
            window.location.reload();
        }

        function updateTimer() {
            const now = new Date();
            const elapsedSeconds = Math.round((now - startTime) / 1000);

            if (elapsedSeconds < 60) {
                timerDiv.textContent = `Elapsed Time: ${elapsedSeconds}s`;
            } else if (elapsedSeconds < 3600) {
                const minutes = Math.floor(elapsedSeconds / 60);
                const seconds = elapsedSeconds % 60;
                timerDiv.textContent = `Elapsed Time: ${minutes}m ${seconds}s`;
            } else {
                const hours = Math.floor(elapsedSeconds / 3600);
                const minutes = Math.floor((elapsedSeconds % 3600) / 60);
                const seconds = elapsedSeconds % 60;
                timerDiv.textContent = `Elapsed Time: ${hours}h ${minutes}m ${seconds}s`;
            }
        }

        async function pollProgress() {
            try {
                const response = await fetch('/progress');
                if (response.ok) {
                    const progress = await response.json();
                    let progressText = progress.text;
                    logDiv.innerHTML = progressText; // Show current task text
                    
                    if (progress.total > 0) {
                        const percentage = Math.round((progress.current / progress.total) * 100);
                        progressBar.style.width = `${percentage}%`;
                        progressBar.textContent = `${percentage}%`;
                    }
                }
            } catch (error) {
                console.error("Progress poll failed:", error);
            }
        }

        async function handleSubmit() {
            logDiv.innerHTML = 'Initializing...';
            downloadArea.innerHTML = '';
            progressContainer.style.display = 'block';
            progressBar.style.width = '0%';
            progressBar.textContent = '0%';
            timerDiv.textContent = 'Elapsed Time: 0s';
            if (progressInterval) clearInterval(progressInterval);
            if (timerInterval) clearInterval(timerInterval); 

            // --- CIK Validation ---
            if (!cikInput.checkValidity()) {
                 logDiv.innerHTML = '<span class="error">Error: CIK must be exactly 10 digits.</span>';
                 cikValidationMessage.style.display = 'block'; 
                 progressContainer.style.display = 'none';
                 return; 
            } else {
                 cikValidationMessage.style.display = 'none';
            }

            // --- Fiscal Year End Validation ---
            if (fyMonthSelect.value === "") {
                 logDiv.innerHTML = '<span class="error">Error: Please select a Fiscal Year End.</span>';
                 progressContainer.style.display = 'none';
                 return;
            }

            fetchButton.disabled = true;
            fetchButton.textContent = 'Processing...';
            cancelButton.style.display = 'inline-block';
            window.addEventListener('beforeunload', confirmExitDuringProcess);
            logDiv.closest('details').open = true;
            
            startTime = new Date();
            progressInterval = setInterval(pollProgress, 1500); 
            timerInterval = setInterval(updateTimer, 1000);

            const selected_fetch_mode = document.getElementById('fetch_mode').value;
            const formData = {
                cik: cikInput.value,
                ticker: document.getElementById('ticker').value,
                fy_month: fyMonthSelect.value,
                fy_adjust: document.getElementById('fy_adjust').value,
                latest_only: document.getElementById('latest_only').checked,
                fetch_mode: selected_fetch_mode
            };

            try {
                const response = await fetch('/fetch', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(formData)
                });

                clearInterval(progressInterval); 
                clearInterval(timerInterval);

                const contentType = response.headers.get("content-type");
                if (!response.ok || !contentType || !contentType.includes("application/json")) {
                    let errorMsg = `Server error ${response.status}: ${response.statusText}`;
                    try {
                         const textError = await response.text();
                         console.error("Server Response Text:", textError);
                         errorMsg += ` - Response: ${textError.substring(0, 200)}`;
                    } catch(e) { /* Ignore */ }
                    throw new Error(errorMsg);
                }
                const results = await response.json();
                logDiv.innerHTML = results.log.join('\\n');
                
                if (results.success) {
                    progressBar.style.width = '100%';
                    progressBar.textContent = '100%';
                    logDiv.innerHTML += '\\n\\nProcessing complete!';
                    if (results.filename) {
                        logDiv.innerHTML += '\\nZIP file generated.';
                        const downloadLink = document.createElement('a');
                        downloadLink.href = `/download/${results.filename}`;
                        downloadLink.textContent = `Download ${results.filename}`;
                        downloadLink.className = 'download-link button';
                        downloadLink.style.display = 'inline-block';
                        downloadLink.style.textDecoration = 'none';
                        downloadArea.appendChild(downloadLink);
                    }
                    if (results.report_filename) {
                        logDiv.innerHTML += '\\nReport CSV generated.';
                        const reportLink = document.createElement('a');
                        reportLink.href = `/download/${results.report_filename}`;
                        reportLink.textContent = `Download Report CSV (${results.report_filename})`;
                        reportLink.className = 'report-link button';
                        reportLink.style.display = 'inline-block';
                        reportLink.style.textDecoration = 'none';
                        downloadArea.appendChild(reportLink);
                    }
                     if (!results.filename && !results.report_filename && results.message) {
                         logDiv.innerHTML += `\\n\\n${results.message}`;
                    }

                } else {
                    const serverMessage = results.message ? `: ${results.message}` : '. Check logs.';
                    logDiv.innerHTML += `\\n<span class="error">\\nProcessing failed${serverMessage}</span>`;
                }
            } catch (error) {
                console.error("Fetch Error:", error);
                logDiv.innerHTML += `\\n<span class="error">CRITICAL ERROR: ${error.message}</span>`;
            } finally {
                 if (progressInterval) clearInterval(progressInterval);
                 if (timerInterval) clearInterval(timerInterval);
                 fetchButton.disabled = false;
                 fetchButton.textContent = 'Fetch Filings';
                 cancelButton.style.display = 'none';
                 window.removeEventListener('beforeunload', confirmExitDuringProcess);
            }
        }
    </script>
</body>
</html>
"""

# --- Backend Functions ---

def get_chrome_path():
    """Gets the path to the Chrome executable based on the OS."""
    system = platform.system().lower()
    path = CHROME_PATH.get(system)
    if path and os.path.exists(path):
        return path
    if system == 'windows':
        path_x86 = 'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe'
        if os.path.exists(path_x86): return path_x86
        appdata_path = os.path.join(os.getenv('LOCALAPPDATA', ''), 'Google\\Chrome\\Application\\chrome.exe')
        if os.path.exists(appdata_path): return appdata_path
    elif system == 'linux':
        for p in ['/usr/bin/google-chrome-stable', '/usr/bin/google-chrome', '/opt/google/chrome/chrome']:
            if os.path.exists(p): return p
    return None

# --- NEW: 8K Labeling Logic based on distance from FYE ---
def get_8k_period_label_by_distance(filing_date, fiscal_year_end_month, fy_adjust):
    """
    Calculates the 8-K period label based on the month's distance from the fiscal year end.
    """
    try:
        F = int(fiscal_year_end_month)
        M = filing_date.month
    except (ValueError, TypeError):
        return None

    # Determine the calendar year in which the relevant fiscal year ends.
    fy_end_year = filing_date.year
    if M <= F:
        fy_end_year -= 1
    
    # Apply the fiscal year basis adjustment from the UI
    if fy_adjust == "Previous Year":
        fy_end_year -= 1

    # Calculate distance 'd'
    d = ((M - F - 1) % 12) + 1

    # Assign label based on the bucket 'd' falls into
    if 1 <= d <= 3:
        # This is the FY report for the fiscal year that just ended.
        label = f"FY{fy_end_year % 100:02d}"
    elif 4 <= d <= 6:
        # This is the 1Q report for the *next* fiscal year.
        label = f"1Q{(fy_end_year + 1) % 100:02d}"
    elif 7 <= d <= 9:
        # This is the 2Q report for the *next* fiscal year.
        label = f"2Q{(fy_end_year + 1) % 100:02d}"
    else:  # 10 <= d <= 12
        # This is the 3Q report for the *next* fiscal year.
        label = f"3Q{(fy_end_year + 1) % 100:02d}"

    return label


# --- Naming logic for 10-K/10-Q (based on report date) ---
def get_period_label_from_report_date(form, report_date, fiscal_year_end_month, fy_adjust):
    """
    Calculates the fiscal period (e.g., FY24, 1Q25) based on the report date
    and the company's fiscal year end month. Handles year transitions correctly.
    """
    try:
        fyem = int(fiscal_year_end_month)
        if not 1 <= fyem <= 12: raise ValueError("Month out of range")
    except (ValueError, TypeError):
        fyem = 12

    # Determine the calendar year the relevant fiscal year *ends* in
    fiscal_year_end_year = report_date.year if report_date.month <= fyem else report_date.year + 1

    # Determine the label year based on the adjustment
    fiscal_year_label_year = fiscal_year_end_year - 1 if fy_adjust == "Previous Year" else fiscal_year_end_year

    if form == "10-K":
        return f"FY{fiscal_year_label_year % 100:02d}"
    elif form == "10-Q":
        # Calculate months past the *start* of the fiscal year this report belongs to.
        # FY Start Month = (fyem % 12) + 1
        months_past_fye_start = (report_date.month - ((fyem % 12) + 1) + 12) % 12
        quarter = (months_past_fye_start // 3) + 1

        # Ensure quarter is between 1 and 3 for a 10-Q
        if quarter < 1 or quarter > 3:
            months_before_fye = (fyem - report_date.month + 12) % 12
            if months_before_fye < 3: quarter = 3
            elif months_before_fye < 6: quarter = 2
            elif months_before_fye < 9: quarter = 1
            else: quarter = 3 # Fallback clamp
            print(f"Warning: Calculated Q{quarter} for 10-Q based on report date {report_date}, FYEM {fyem}. Using clamped Q{quarter}.")
            quarter = max(1, min(3, quarter)) # Clamp strictly 1-3

        return f"{quarter}Q{fiscal_year_label_year % 100:02d}"
    else: # Fallback for other forms if needed
        return f"FY{fiscal_year_label_year % 100:02d}"


# --- Robust GET with retries ---
def robust_get(url, retries=5, delay=1, backoff=2, params=None): # Added params argument
    """
    Performs a GET request with retries on 503/429 errors using exponential backoff.
    """
    time.sleep(0.1) # Always have a small base delay to be polite
    current_delay = delay
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=DEFAULT_TIMEOUT, params=params) 
            resp.raise_for_status() 
            return resp 
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            if (status_code in [503, 429]) and attempt < retries - 1:
                wait_time = current_delay
                print(f"Warning: Received {status_code} for {url}. Retrying in {wait_time:.2f}s... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait_time)
                current_delay *= backoff # Increase delay for next potential retry
            else:
                if status_code != 404 or "submissions/CIK" in url:
                     print(f"Error: Failed GET request for {url} with status {status_code} after {retries} attempts.")
                raise 
        except requests.exceptions.RequestException as e:
            print(f"Warning: Request failed for {url} on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(current_delay)
                current_delay *= backoff
            else:
                print(f"Error: Failed GET request for {url} after {retries} attempts due to {e}.")
                raise 
    raise requests.exceptions.RequestException(f"Failed to get {url} after {retries} attempts.")


# --- REWRITTEN Exhibit parsing for 8-K earnings release ---
def get_earnings_release_exhibit_info(cik_padded, accession_clean, primary_doc_filename, archive_base_url, log_lines):
    """
    Fetches the primary 8-K document and searches its content for links to the
    earnings press release exhibit (e.g., EX-99.1). This version uses a more
    flexible, multi-stage search strategy.
    """
    primary_doc_url = f"{archive_base_url}{accession_clean}/{primary_doc_filename}"
    log_lines.append(f"    Fetching primary 8-K document: {primary_doc_url}")

    try:
        r_filing = robust_get(primary_doc_url, retries=2, delay=0.5)
        if not r_filing:
            log_lines.append(f"    ERROR: Failed to fetch primary 8-K document {primary_doc_url} after retries.")
            return None

        import warnings
        from bs4 import XMLParsedAsHTMLWarning
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

        try:
            soup = BeautifulSoup(r_filing.content, 'lxml')
        except Exception:
            soup = BeautifulSoup(r_filing.content, 'html.parser')

        # --- STAGE 1: HYPERLINK-BASED SEARCH ---
        exhibit_patterns = [
            # Pattern, Exhibit Type, Priority
            (r'ex[\s\-_.]*99[\s\-_.]*1', 'EX-99.1', 1),
            (r'ex[\s\-_.]*99[\s\-_.]*01', 'EX-99.1', 1),
            (r'ex[\s\-_.]*99[\s\-_.]*2', 'EX-99.2', 2),
            (r'ex[\s\-_.]*99[\s\-_.]*02', 'EX-99.2', 2),
            (r'ex[\s\-_.]*\b99\b(?!\.\d)', 'EX-99', 3), # Match "99" but not "99.1"
        ]
        keyword_patterns = [r'press\srelease', r'earnings', r'financial\sresults', r'news\srelease']
        
        candidate_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            link_text = link.get_text(" ", strip=True).lower()
            if not href or href.startswith(('#', 'javascript:')): continue

            parent_context_text = ""
            parent_row = link.find_parent('tr')
            if parent_row: parent_context_text = parent_row.get_text(" ", strip=True).lower()
            
            full_context = f"{link_text} {parent_context_text}"
            
            exhibit_type, priority, keyword_match, exhibit_match = "EX-?", 99, False, False
            keyword_match = any(re.search(kw, full_context) for kw in keyword_patterns)

            for pattern, ex_type, prio in exhibit_patterns:
                if re.search(pattern, f"{href} {link_text}"):
                    exhibit_type, priority, exhibit_match = ex_type, prio, True
                    break
            
            if exhibit_match and keyword_match:
                exhibit_url = urljoin(primary_doc_url, link['href'])
                if primary_doc_filename.lower() in exhibit_url.lower(): continue
                candidate_links.append({
                    'url': exhibit_url, 'filename': os.path.basename(urlparse(exhibit_url).path),
                    'description': link.get_text(strip=True), 'type': exhibit_type, 'priority': priority
                })

        if candidate_links:
            sorted_candidates = sorted(candidate_links, key=lambda x: x['priority'])
            best_candidate = sorted_candidates[0]
            log_lines.append(f"    Found exhibit via hyperlink: Link='{best_candidate['description']}', Type='{best_candidate['type']}'")
            return {k: v for k, v in best_candidate.items() if k != 'priority'}

        # --- STAGE 2: TEXT-BASED SEARCH (if no hyperlinks found) ---
        log_lines.append("    No hyperlink match found. Starting text-based search for exhibit filename.")
        filename_pattern = re.compile(r'\b[a-zA-Z0-9\-_]+\.htm(l)?\b')
        
        for row in soup.find_all('tr'):
            row_text = row.get_text(" ", strip=True).lower()
            
            exhibit_match = any(re.search(p[0], row_text) for p in exhibit_patterns)
            keyword_match = any(re.search(kw, row_text) for kw in keyword_patterns)

            if exhibit_match and keyword_match:
                # Search for a filename within this promising row
                filename_match = filename_pattern.search(row.get_text(" ", strip=True))
                if filename_match:
                    found_filename = filename_match.group(0)
                    exhibit_url = urljoin(primary_doc_url, found_filename)
                    
                    # Determine exhibit type from the text
                    exhibit_type = "EX-99.1" # Default assumption
                    for pattern, ex_type, _ in exhibit_patterns:
                        if re.search(pattern, row_text):
                            exhibit_type = ex_type
                            break

                    log_lines.append(f"    Found exhibit via text search: Filename='{found_filename}', Type='{exhibit_type}'")
                    return {
                        'url': exhibit_url,
                        'filename': found_filename,
                        'description': f"Exhibit {exhibit_type} (found via text search)",
                        'type': exhibit_type
                    }

        log_lines.append(f"    No earnings-release exhibit found in {primary_doc_url} via any method.")
        return None

    except requests.exceptions.RequestException as e:
        log_lines.append(f"    ERROR downloading primary 8-K doc {primary_doc_url}: {e}")
        return None
    except Exception as e:
        log_lines.append(f"    ERROR parsing primary doc for exhibits: {e} {traceback.format_exc(limit=1)}")
        return None


# --- Asset downloading ---
def download_assets(soup, base_url, filing_output_dir, log_lines):
    """Downloads CSS and image assets linked in the HTML."""
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
                log_lines.append(f"Warning: Skipping invalid asset URL: {asset_url}"); continue
            if parsed_url.scheme not in ['http', 'https']: continue
            if absolute_url in processed_urls: continue
            processed_urls.add(absolute_url)
            try:
                path_part = parsed_url.path
                filename_base = os.path.basename(path_part)
                if not filename_base: filename_base = f"asset_{len(downloaded_assets_filenames) + 1}"
                safe_filename = "".join(c if c.isalnum() or c in ['.', '_', '-'] else '_' for c in filename_base)[:100].strip('._')
                if not safe_filename: safe_filename = f"asset_{len(downloaded_assets_filenames) + 1}"
                _, ext = os.path.splitext(safe_filename)
                if not ext: safe_filename += ".asset"
                local_path = os.path.join(filing_output_dir, safe_filename)
                if not os.path.exists(local_path):
                    r = robust_get(absolute_url)
                    if not r: continue
                    content_type = r.headers.get('content-type')
                    guessed_ext = mimetypes.guess_extension(content_type.split(';')[0]) if content_type else None
                    if guessed_ext and guessed_ext != ".asset" and not safe_filename.lower().endswith(guessed_ext.lower()):
                         base, _ = os.path.splitext(safe_filename)
                         new_safe_filename = base + guessed_ext
                         new_local_path = os.path.join(filing_output_dir, new_safe_filename)
                         if not os.path.exists(new_local_path):
                             safe_filename = new_safe_filename
                             local_path = new_local_path
                    with open(local_path, 'wb') as f: f.write(r.content)
                tag[url_attr] = safe_filename
                downloaded_assets_filenames.add(safe_filename)
            except Exception as e: log_lines.append(f"Warn: Error with asset {absolute_url}: {e}")
    return list(downloaded_assets_filenames)

# --- Clean internal anchor links ---
def clean_internal_links(soup, log_lines):
    cleaned_count = 0
    for a_tag in soup.find_all('a', href=lambda href: href and href.startswith('#')):
        target_id = a_tag['href'].lstrip("#")
        if not target_id: continue 
        if not soup.find(id=target_id) and not soup.find(attrs={'name': target_id}):
            del a_tag['href']; cleaned_count += 1
    if cleaned_count > 0: log_lines.append(f"Cleaned {cleaned_count} broken internal anchor links.")
    return soup

# --- HTML to PDF conversion ---
def convert_generic_to_pdf(html_path, output_filename_base, accession, log_lines) -> Tuple[Optional[str], Optional[str]]:
    pdf_path = None; pdf_filename = None
    try:
        safe_base_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in output_filename_base).strip('._')
        if not safe_base_name: safe_base_name = f"{accession}_document"
        pdf_filename = f"{safe_base_name}.pdf"
        pdf_path = os.path.join(os.path.dirname(html_path), pdf_filename)
        abs_html_path = os.path.abspath(html_path); file_uri = pathlib.Path(abs_html_path).as_uri()
        log_lines.append(f"Attempting PDF: {pdf_filename}")
        chrome_exec = get_chrome_path()
        if not chrome_exec: log_lines.append("ERROR: Chrome not found."); return None, None
        with sync_playwright() as p:
            browser = None
            try:
                browser = p.chromium.launch(headless=True, executable_path=chrome_exec)
                page = browser.new_page()
                page.goto(file_uri, wait_until='networkidle', timeout=90000)
                page.wait_for_timeout(2000); page.emulate_media(media='print')
                page.pdf(path=pdf_path, format='Letter', print_background=True, margin={'top': '1cm', 'bottom': '1cm', 'left': '1cm', 'right': '1cm'}, prefer_css_page_size=True, scale=0.8)
                browser.close()
            except Exception as e_pw:
                 log_lines.append(f"ERROR: Playwright PDF error for {accession}: {e_pw}")
                 if browser and browser.is_connected(): browser.close()
                 if pdf_path and os.path.exists(pdf_path): os.remove(pdf_path)
                 return None, None
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 100:
            log_lines.append(f"PDF created: {pdf_filename}"); return pdf_path, pdf_filename
        else:
            log_lines.append(f"ERROR: PDF failed or empty: {pdf_filename}")
            if os.path.exists(pdf_path): os.remove(pdf_path)
            return None, None
    except Exception as e:
        log_lines.append(f"ERROR: PDF prep error for {accession}: {e}")
        if pdf_path and os.path.exists(pdf_path): os.remove(pdf_path)
        return None, None

# --- Cleanup intermediate files ---
def cleanup_files(html_path, assets, filing_output_dir, log_lines):
    cleaned_count = 0
    try:
        if html_path and os.path.exists(html_path): os.remove(html_path); cleaned_count +=1
        for asset_filename in assets:
            asset_path = os.path.join(filing_output_dir, asset_filename)
            if os.path.exists(asset_path):
                try: os.remove(asset_path); cleaned_count += 1
                except OSError as e: log_lines.append(f"Warning: Error cleaning asset {asset_filename}: {e}")
        if cleaned_count > 0: log_lines.append(f"Cleaned {cleaned_count} intermediate file(s).")
    except Exception as e: log_lines.append(f"ERROR: Cleanup exception: {str(e)}")

# --- Download and process generic filing (10-K/Q and 8-K exhibits) ---
def download_and_process_generic(target_url, form, accession, cik, ticker, output_filename_base, filing_output_dir, log_lines, **kwargs) -> Tuple[str, Optional[str], Optional[str]]:
    html_path = None; downloaded_assets = []; pdf_path = None; generated_pdf_basename = None
    log_prefix = f"[{accession} {form}]"; cleanup_flag = True
    try:
        log_lines.append(f"{log_prefix} Processing in {os.path.basename(filing_output_dir)} for {target_url}")
        r = robust_get(target_url) # Use robust_get to handle rate limiting
        if not r:
            raise Exception(f"Failed to download {target_url} after multiple retries.")
            
        base_html_filename = f"{output_filename_base}_{accession}.htm"
        html_path = os.path.join(filing_output_dir, base_html_filename)
        try: decoded_text = r.content.decode('utf-8')
        except UnicodeDecodeError:
             try: decoded_text = r.content.decode('latin-1')
             except UnicodeDecodeError: decoded_text = r.content.decode('utf-8', errors='replace'); log_lines.append(f"{log_prefix} Warn: UTF-8 with replacement.")
        replacements = { "Â\x9d": "\"", "â€œ": "\"", "â€™": "'", "â€˜": "'", "â€“": "-", "â€”": "—", "&nbsp;": " ", "\u00a0": " " }
        for wrong, correct in replacements.items(): decoded_text = decoded_text.replace(wrong, correct)
        soup = BeautifulSoup(decoded_text, 'html.parser')
        head = soup.head
        if not head: head = soup.new_tag('head'); (soup.find('html') or soup).insert(0, head)
        if not head.find('meta', charset=True): head.insert(0, soup.new_tag('meta', charset='UTF-8'))
        cleaned_p_count = 0
        for p_tag in soup.find_all('p'):
            if not p_tag.get_text(strip=True) and not p_tag.find(['img', 'table', 'svg', 'hr']):
                p_tag.decompose(); cleaned_p_count += 1
        if cleaned_p_count > 0: log_lines.append(f"{log_prefix} Removed {cleaned_p_count} empty paragraphs.")
        if 'clean_internal_links' in globals(): soup = clean_internal_links(soup, log_lines)
        doc_base_url = urljoin(target_url, '.'); downloaded_assets = download_assets(soup, doc_base_url, filing_output_dir, log_lines)
        with open(html_path, 'w', encoding='utf-8') as f: f.write(str(soup))
        pdf_path, generated_pdf_basename = convert_generic_to_pdf(html_path, output_filename_base, accession, log_lines)
        return (form, pdf_path, generated_pdf_basename)
    except Exception as e:
        log_lines.append(f"{log_prefix} ERROR processing {target_url}: {e} {traceback.format_exc(limit=1)}")
    finally:
        if cleanup_flag: cleanup_files(html_path, downloaded_assets, filing_output_dir, log_lines)
    return (form, None, None)

# --- Main process for fetching filings ---
def process_filing(cik, ticker, fy_month_str, fy_adjust, log_lines, tmp_dir, latest_only=False, fetch_mode='10K_10Q') -> Tuple[Dict[str, List[str]], List[Dict[str,str]], List[str]]:
    pdf_files = {"10-K": [], "10-Q": [], "8-K_ER": []}
    report_items = [] 
    if not cik.isdigit(): log_lines.append("ERROR: Invalid CIK."); return pdf_files, report_items, log_lines
    cik_padded = cik.zfill(10)
    user_fy_end_month_int = int(fy_month_str) 

    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    archive_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/"
    log_lines.append(f"Accessing EDGAR index for CIK: {cik_padded}...")
    try:
        r = robust_get(submissions_url, retries=3, delay=0.5)
        if not r: log_lines.append(f"ERROR: Failed to retrieve submission data."); return pdf_files, report_items, log_lines
        submissions = r.json()
        log_lines.append("Successfully retrieved submission data.")
        if not ticker and 'tickers' in submissions and submissions['tickers']:
             ticker = submissions['tickers'][0]; log_lines.append(f"Note: Using ticker '{ticker}' from SEC data.")
    except Exception as e:
        log_lines.append(f"ERROR: Failed to retrieve/process submission data: {str(e)}"); return pdf_files, report_items, log_lines

    try:
        # <<< MODIFIED to fetch from all historical data files >>>
        all_filings_raw = []
        filings = submissions.get('filings', {})
        all_filing_sources = [] 
        
        # <<< MODIFIED LOGIC: Check for 'files' first, fall back to 'recent' >>>
        if 'files' in filings and filings.get('files'):
            historical_files_to_fetch = filings['files']
            total_files = len(historical_files_to_fetch)
            log_lines.append(f"Found {total_files} historical data files. Fetching all...")
            with progress_lock:
                progress_data["text"] = f"Fetching historical data..."
                progress_data["total"] = total_files
                progress_data["current"] = 0

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_url = {
                    executor.submit(robust_get, f"https://data.sec.gov/submissions/{file_info['name']}"): file_info['name']
                    for file_info in historical_files_to_fetch
                }
                for i, future in enumerate(as_completed(future_to_url)):
                    url_name = future_to_url[future]
                    try:
                        resp = future.result()
                        all_filing_sources.append(resp.json())
                    except Exception as exc:
                        log_lines.append(f'Warn: Failed to fetch historical file {url_name}: {exc}')
                    finally:
                        with progress_lock:
                            progress_data["current"] = i + 1
        
        # <<< ALWAYS add 'recent' data as well >>>
        if 'recent' in filings and filings.get('recent'):
            log_lines.append("Adding recent filings to the data source.")
            all_filing_sources.append(filings.get('recent'))

        if not all_filing_sources:
             log_lines.append("No filing data sources found (neither recent nor historical).")
             return pdf_files, report_items, log_lines
        
        with progress_lock: progress_data["text"] = "Aggregating all filing data..."
        
        for source_data in all_filing_sources:
            if not source_data: continue
            accession_numbers = source_data.get('accessionNumber', [])
            forms = source_data.get('form', [])
            filing_dates_str_list = source_data.get('filingDate', [])
            report_dates_str_list = source_data.get('reportDate', [])
            primary_documents = source_data.get('primaryDocument', [])
            items_list = source_data.get('items', []) 
            acceptance_datetimes = source_data.get('acceptanceDateTime', [])

            list_len = len(accession_numbers)
            if not (list_len == len(forms) == len(filing_dates_str_list) == len(primary_documents) == len(report_dates_str_list) == len(acceptance_datetimes)):
                 log_lines.append("WARN: Skipping a block of filing data due to inconsistent lengths.")
                 continue
            if len(items_list) < list_len: items_list.extend([''] * (list_len - len(items_list)))

            for i in range(list_len):
                try:
                    filing_dt = datetime.strptime(filing_dates_str_list[i], "%Y-%m-%d").date()
                    report_dt = datetime.strptime(report_dates_str_list[i], "%Y-%m-%d").date() if report_dates_str_list[i] else None
                    all_filings_raw.append({
                        "accession_raw": accession_numbers[i], "accession_clean": accession_numbers[i].replace('-', ''),
                        "form": forms[i], "filing_date": filing_dt, "filing_date_str": filing_dates_str_list[i],
                        "acceptance_datetime_str": acceptance_datetimes[i], 
                        "report_date": report_dt, "report_date_str": report_dates_str_list[i],
                        "primary_doc": primary_documents[i], "items": items_list[i]
                    })
                except ValueError as e: log_lines.append(f"Warn: Skipping {accession_numbers[i]}, date error: {e}")

        seen_accessions = set()
        deduped_filings = []
        for f in all_filings_raw:
            if f['accession_clean'] not in seen_accessions:
                seen_accessions.add(f['accession_clean'])
                deduped_filings.append(f)
        
        all_filings_raw = deduped_filings
        all_filings_raw.sort(key=lambda x: x['filing_date']) 
        log_lines.append(f"Processing a complete set of {len(all_filings_raw)} filings...")
        
        tasks_to_submit = []
        EARLIEST_FISCAL_YEAR_SUFFIX = 17 
        processed_periods = set() 
        
        if fetch_mode == '8K_Earnings':
            # 1. Gather all potential 8-K earnings exhibits with their info
            potential_exhibits_with_filing = []
            log_lines.append("Gathering potential 8-K earnings exhibits...")
            for filing in all_filings_raw:
                items_set = {item.strip() for item in filing.get('items', '').split(',')}
                if filing['form'] == '8-K' and ('2.02' in items_set or '9.01' in items_set):
                    if not filing['primary_doc']:
                        log_lines.append(f"  Skipping {filing['accession_raw']} ({filing['filing_date_str']}): Missing primary document.")
                        continue
                    
                    exhibit_info = get_earnings_release_exhibit_info(
                        cik_padded, filing['accession_clean'], filing['primary_doc'], archive_base_url, log_lines
                    )
                    if exhibit_info:
                        log_lines.append(f"  Found potential exhibit for {filing['accession_raw']}")
                        potential_exhibits_with_filing.append({
                            'filing_date': filing['filing_date'],
                            'exhibit_info': exhibit_info,
                            'original_filing': filing
                        })
                    else:
                        log_lines.append(f"  Skipping Item 2.02/9.01 on {filing['filing_date_str']}: no true earnings exhibit found.")

            # 2. De-duplicate by exhibit URL, keeping the earliest filing date instance
            log_lines.append(f"De-duplicating {len(potential_exhibits_with_filing)} potential exhibits by URL...")
            potential_exhibits_with_filing.sort(key=lambda x: x['filing_date'])
            seen_urls = set()
            unique_verified_exhibits = []
            for exhibit_data in potential_exhibits_with_filing:
                exhibit_url = exhibit_data['exhibit_info']['url']
                if exhibit_url not in seen_urls:
                    seen_urls.add(exhibit_url)
                    unique_verified_exhibits.append(exhibit_data)
                else:
                    log_lines.append(f"  Skipping duplicate exhibit URL {exhibit_url} from filing {exhibit_data['original_filing']['accession_raw']} ({exhibit_data['filing_date']})")

            # 3. Label filings using the distance-based method
            for exhibit_data in unique_verified_exhibits:
                filing = exhibit_data['original_filing']
                exhibit_info = exhibit_data['exhibit_info']
                
                label = get_8k_period_label_by_distance(filing['filing_date'], user_fy_end_month_int, fy_adjust)

                if label is None:
                    log_lines.append(f"  Could not determine label for {filing['accession_raw']}.")
                    continue

                # <<< ADDED THE FY17 FILTERING LOGIC HERE >>>
                try:
                    year_suffix = -1
                    if "Q" in label:
                        year_suffix = int(label.split("Q")[-1])
                    elif "FY" in label:
                        year_suffix = int(label.split("FY")[-1])

                    if not latest_only:
                        is_pre_fy17 = year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX and not (year_suffix > 50 and EARLIEST_FISCAL_YEAR_SUFFIX < 50)
                        is_q1_q3_fy17 = (year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and "Q" in label)
                        
                        if is_pre_fy17 or is_q1_q3_fy17:
                            log_lines.append(f"  Skipping {filing['accession_raw']} ({label}): Filing is before FY17 annual report.")
                            continue
                except (ValueError, IndexError):
                     log_lines.append(f"  Warning: Could not parse year from label '{label}' for filtering.")
                     continue
                # <<< END OF FILTERING LOGIC >>>

                if label in processed_periods:
                    log_lines.append(f"  Skipping {filing['accession_raw']} ({filing['filing_date_str']}): {label} already processed (amendment).")
                    continue
                
                log_lines.append(f"  Labeling {filing['accession_raw']} ({filing['filing_date_str']}) as {label}")

                out_base = f"{ticker or cik_padded}_{label}_prelim"
                acc_clean = filing['accession_clean']
                out_dir  = os.path.join(tmp_dir, f"filing_{acc_clean}_ER_{label.replace('/','-')}")
                os.makedirs(out_dir, exist_ok=True)

                tasks_to_submit.append({
                    'target_url': exhibit_info['url'],
                    'form': '8-K_ER',
                    'accession': acc_clean,
                    'cik': cik_padded,
                    'ticker': ticker,
                    'output_filename_base': out_base,
                    'filing_output_dir': out_dir,
                    'filing_date_str': filing['filing_date_str'],
                    'acceptance_datetime_str': filing['acceptance_datetime_str'],
                    'report_date_str': filing['report_date_str'],
                    'fy_month': fy_month_str,
                    'fy_adjust': fy_adjust,
                    'exhibit_description': exhibit_info.get('description'),
                    'exhibit_type': exhibit_info.get('type'),
                    'assigned_period_label': label
                })
                processed_periods.add(label)

                if latest_only:
                    log_lines.append("Stopping after latest 8-K earnings release.")
                    break
        
        elif fetch_mode == '10K_10Q':
            found_latest_10k_q = False 
            for filing in reversed(all_filings_raw): 
                base_form = filing['form'].split('/')[0]
                if base_form not in ["10-K", "10-Q"]: continue
                if not filing['report_date']: log_lines.append(f"Warn: Skipping {filing['accession_raw']}, missing report date."); continue
                period = "N/A"
                try:
                    period = get_period_label_from_report_date(base_form, filing['report_date'], user_fy_end_month_int, fy_adjust)
                    year_suffix = -1
                    if period.startswith("FY"): year_suffix = int(period[2:])
                    elif "Q" in period: year_suffix = int(period.split("Q")[-1])

                    if not latest_only: 
                        is_pre_fy17 = year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX and not (year_suffix > 50 and EARLIEST_FISCAL_YEAR_SUFFIX < 50)
                        is_q1_q3_fy17 = year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and "Q" in period and period.startswith(("1Q","2Q","3Q"))
                        if is_pre_fy17 or is_q1_q3_fy17: continue
                    
                    if period in processed_periods and not latest_only: continue 
                    if not filing['primary_doc']: log_lines.append(f"Warn: Skipping {filing['accession_raw']}, missing primary doc."); continue
                    
                    target_url = f"{archive_base_url}{filing['accession_clean']}/{filing['primary_doc']}"
                    output_filename_base = f"{ticker or cik_padded}_{period}"
                    filing_output_dir = os.path.join(tmp_dir, f"filing_{filing['accession_clean']}_{base_form.replace('/','-')}")
                    os.makedirs(filing_output_dir, exist_ok=True)
                    
                    current_task = {
                        "target_url": target_url, "form": filing['form'], "report_date_str": filing['report_date_str'],
                        "accession": filing['accession_clean'], "cik": cik_padded, "ticker": ticker,
                        "fy_month": fy_month_str, "fy_adjust": fy_adjust, "output_filename_base": output_filename_base,
                        "filing_output_dir": filing_output_dir, "filing_date_str": filing['filing_date_str'],
                        "acceptance_datetime_str": filing['acceptance_datetime_str'] 
                    }
                    
                    if latest_only:
                        is_pre_fy17 = year_suffix < EARLIEST_FISCAL_YEAR_SUFFIX and not (year_suffix > 50 and EARLIEST_FISCAL_YEAR_SUFFIX < 50)
                        is_q1_q3_fy17 = year_suffix == EARLIEST_FISCAL_YEAR_SUFFIX and "Q" in period and period.startswith(("1Q","2Q","3Q"))
                        if not (is_pre_fy17 or is_q1_q3_fy17):
                             tasks_to_submit.append(current_task)
                             processed_periods.add(period) 
                             log_lines.append(f"Selected latest 10K/Q: {period}: {filing['form']} {filing['accession_raw']}")
                             found_latest_10k_q = True; break 
                        else:
                            log_lines.append(f"Skipping {period} ({filing['accession_raw']}) for latest_only due to year filter.")
                    else: 
                        if period not in processed_periods:
                            tasks_to_submit.append(current_task)
                            processed_periods.add(period)
                            log_lines.append(f"Adding task for {period}: {filing['form']} {filing['accession_raw']}")

                except Exception as e: log_lines.append(f"Warn: Skipping {filing['accession_raw']} due to filter error: {e}"); continue
            if latest_only and found_latest_10k_q: log_lines.append("Stopped 10K/Q search due to 'latest_only'.")


        # --- Execute Tasks ---
        if not tasks_to_submit:
            log_lines.append(f"No filings found matching criteria (Mode: {fetch_mode}).")
            return pdf_files, report_items, log_lines
        
        total_tasks = len(tasks_to_submit)
        log_lines.append(f"Identified {total_tasks} tasks to process.")
        with progress_lock:
            progress_data["text"] = f"Starting download of {total_tasks} filings..."
            progress_data["total"] = total_tasks
            progress_data["current"] = 0
        
        workers = min(int(os.getenv("EDGAR_THREADS", multiprocessing.cpu_count() * 2)), 16)
        workers = max(1, workers)
        log_lines.append(f"Using {workers} worker threads.")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures_map = {executor.submit(download_and_process_generic, **task_details, log_lines=log_lines): task_details for task_details in tasks_to_submit}
            for i, future in enumerate(as_completed(futures_map)):
                task_info = futures_map[future]
                
                with progress_lock:
                    progress_data["text"] = f"Downloading and processing {i + 1}/{total_tasks} filings..."
                    progress_data["current"] = i + 1
                    
                try:
                    form_type_returned, pdf_path, generated_pdf_basename = future.result(timeout=180)
                    if pdf_path and generated_pdf_basename:
                        key = "8-K_ER" if form_type_returned == "8-K_ER" else task_info.get("form","").split('/')[0]
                        if key in pdf_files: pdf_files[key].append(pdf_path)
                        report_items.append({
                            'document_name': generated_pdf_basename, 
                            'filing_date': task_info['filing_date_str'],
                            'acceptance_datetime': task_info['acceptance_datetime_str']
                        })
                        log_lines.append(f"Successfully processed PDF: {generated_pdf_basename}")
                    else: log_lines.append(f"Task for {task_info.get('accession','N/A')} ({task_info.get('form','N/A')}) did not yield a PDF.")
                except Exception as e: log_lines.append(f"--- ERROR for {task_info.get('accession','N/A')}: {e} {traceback.format_exc(limit=1)} ---")
        
        with progress_lock:
            progress_data["text"] = f"Processing complete. Downloaded {progress_data['current']}/{total_tasks} filings."
            
        log_lines.append("Thread pool shutdown.")
    except Exception as e:
        log_lines.append(f"ERROR: Main processing error: {e} {traceback.format_exc()}")
    total_pdfs = sum(len(v) for v in pdf_files.values())
    log_lines.append(f"Generated {total_pdfs} PDF(s). {len(report_items)} items for report.")
    return pdf_files, report_items, log_lines


# create_zip_archive function (no changes needed for this request)
def create_zip_archive(pdf_files, cik, log_lines, tmp_dir):
    total_pdfs = sum(len(paths) for paths in pdf_files.values())
    if not total_pdfs: log_lines.append("No PDFs, skipping ZIP."); return None, log_lines
    zip_filename = f"{cik}_filings.zip" 
    zip_path = os.path.join(tmp_dir, zip_filename)
    log_lines.append(f"Creating ZIP: '{zip_filename}'...")
    added_count = 0
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            folder_map = {"10-K": "10-K", "10-Q": "10-Q", "8-K_ER": "8-K Earnings Release"}
            for form_key, folder_name in folder_map.items():
                 paths = pdf_files.get(form_key, [])
                 if not paths: continue
                 paths.sort() 
                 for pdf_full_path in paths:
                      if pdf_full_path and os.path.exists(pdf_full_path):
                          zipf.write(pdf_full_path, arcname=os.path.join(folder_name, os.path.basename(pdf_full_path)))
                          added_count += 1
        log_lines.append(f"ZIP created with {added_count} files.")
        return zip_path, log_lines
    except Exception as e:
        log_lines.append(f"ERROR: Failed to create ZIP: {e}")
        if os.path.exists(zip_path): 
            try: 
                os.remove(zip_path)
            except OSError: 
                pass
        return None, log_lines

# --- Function to create CSV report ---
def create_download_report_csv(report_items: List[Dict[str, str]], cik: str, tmp_dir: str, log_lines: List[str], fetch_mode: str) -> Tuple[Optional[str], List[str]]:
    if not report_items:
        log_lines.append("No items for report, skipping CSV.")
        return None, log_lines
    report_type_str = "10KQ" if fetch_mode == '10K_10Q' else "8K" if fetch_mode == '8K_Earnings' else "UnknownType"
    report_filename = f"{cik}_{report_type_str}_Filing_Dates_Report.csv"
    report_csv_path = os.path.join(tmp_dir, report_filename)
    log_lines.append(f"Creating CSV report: {report_filename}")
    try:
        report_items.sort(key=lambda x: (datetime.strptime(x['acceptance_datetime'], "%Y-%m-%dT%H:%M:%S.%fZ"), x['document_name']))
        with open(report_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Document Name', 'Filing Date', 'Filing Time (UTC)', 'UK Date', 'UK Time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in report_items:
                try:
                    utc_dt_obj = datetime.strptime(item['acceptance_datetime'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    
                    # Calculate UK time by adding 5 hours
                    uk_dt_obj = utc_dt_obj + timedelta(hours=5)
                    
                    # Format dates and times
                    formatted_utc_date = utc_dt_obj.strftime("%Y/%m/%d")
                    formatted_utc_time = utc_dt_obj.strftime("%H:%M:%S")
                    formatted_uk_date = uk_dt_obj.strftime("%Y/%m/%d")
                    formatted_uk_time = uk_dt_obj.strftime("%H:%M:%S")

                    writer.writerow({
                        'Document Name': item['document_name'], 
                        'Filing Date': formatted_utc_date, 
                        'Filing Time (UTC)': formatted_utc_time,
                        'UK Date': formatted_uk_date,
                        'UK Time': formatted_uk_time
                    })
                except (ValueError, TypeError):
                    writer.writerow({
                        'Document Name': item['document_name'], 
                        'Filing Date': item.get('filing_date', ''), 
                        'Filing Time (UTC)': '',
                        'UK Date': '',
                        'UK Time': ''
                    })
        log_lines.append(f"CSV report '{report_filename}' created with {len(report_items)} entries.")
        return report_csv_path, log_lines
    except Exception as e:
        log_lines.append(f"ERROR: Failed to create CSV report '{report_filename}': {e}")
        if os.path.exists(report_csv_path):
            try:
                os.remove(report_csv_path)
            except OSError:
                pass
        return None, log_lines

# --- Flask Routes ---
@app.route("/", methods=["GET"])
def index():
    months = [(str(i), datetime(2000, i, 1).strftime('%B')) for i in range(1, 13)]
    return render_template_string(FORM_TEMPLATE, months=months)

@app.route("/fetch", methods=["POST"])
def fetch_filing():
    log_lines = ["Initializing backend processing..."]
    response_data = {"success": False, "message": "Process started...", "log": log_lines, "filename": None, "report_filename": None}
    status_code = 500 
    try:
        data = request.json
        cik = data.get("cik", "").strip()
        fy_month_str = data.get("fy_month", "").strip()
        ticker = data.get("ticker", "").strip()
        fy_adjust = data.get("fy_adjust", "Same Year").strip()
        latest_only = data.get("latest_only", False)
        fetch_mode = data.get("fetch_mode", "10K_10Q")

        if not cik or not cik.isdigit() or len(cik) != 10 : 
             response_data = {"success": False, "message": "CIK is required and must be exactly 10 digits.", "log": ["ERROR: Invalid CIK."]}
             return jsonify(response_data), 400
        if not fy_month_str:
             response_data = {"success": False, "message": "Fiscal Year End is required.", "log": ["ERROR: Fiscal Year End not selected."]}
             return jsonify(response_data), 400

        with tempfile.TemporaryDirectory() as tmp_dir:
            log_lines.append(f"Using temp directory: {tmp_dir}")
            pdf_files, report_items, log_lines = process_filing(
                cik, ticker, fy_month_str, fy_adjust, log_lines, tmp_dir, latest_only, fetch_mode
            )
            zip_ok, report_ok = False, False
            final_zip_filename, final_report_filename = None, None

            if any(pdf_files.values()):
                zip_path, log_lines = create_zip_archive(pdf_files, cik, log_lines, tmp_dir)
                if zip_path:
                    final_zip_filename = os.path.basename(zip_path)
                    with open(zip_path, 'rb') as f_zip: app.config['FILE_STORAGE'][final_zip_filename] = f_zip.read()
                    zip_ok = True
            
            if report_items:
                csv_path, log_lines = create_download_report_csv(report_items, cik, tmp_dir, log_lines, fetch_mode)
                if csv_path:
                    final_report_filename = os.path.basename(csv_path)
                    with open(csv_path, 'rb') as f_csv: app.config['FILE_STORAGE'][final_report_filename] = f_csv.read()
                    report_ok = True

            if zip_ok or report_ok:
                response_data = {"success": True, "filename": final_zip_filename, "report_filename": final_report_filename, "log": log_lines, "message": "Processing complete."}
                status_code = 200
            else:
                response_data = {"success": False, "message": "No valid filings found or processed.", "log": log_lines}
                status_code = 200 
            return jsonify(response_data), status_code

    except Exception as e:
        error_details = traceback.format_exc()
        log_lines.append(f"CRITICAL SERVER ERROR: {str(e)}\n{error_details}")
        print(f"CRITICAL SERVER ERROR: {str(e)}\n{error_details}", file=sys.stderr) 
        response_data = { "success": False, "message": f"An internal server error occurred. Details: {str(e)}", "log": log_lines }
        return jsonify(response_data), 500


@app.route("/download/<filename>")
def download_file(filename):
    with file_storage_lock: file_data = app.config['FILE_STORAGE'].pop(filename, None)
    if not file_data: return "File not found or already downloaded.", 404
    mimetype = 'application/zip' if filename.lower().endswith('.zip') else 'text/csv' if filename.lower().endswith('.csv') else 'application/octet-stream'
    return send_file(BytesIO(file_data), mimetype=mimetype, as_attachment=True, download_name=filename)


@app.route("/progress")
def progress_endpoint():
    """Provides progress updates to the frontend."""
    with progress_lock:
        return jsonify(progress_data)

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Mzansi EDGAR Fetcher (Local Flask Version)...")
    print("Ensure Google Chrome is installed and accessible.")
    print("Serving on http://127.0.0.1:5000")
    serve(app, host="127.0.0.1", port=5000, threads=8)
