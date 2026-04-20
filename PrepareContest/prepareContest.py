import sys
import requests
import os
import gspread
import time
from gspread_formatting import *
import sheetFormat
import urllib.parse
import logging
import random
import io
from typing import Optional, List, Dict, Tuple
from urllib.parse import quote
from google.oauth2.service_account import Credentials
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
MINIMUM_SIMILARITY = 0.6
MAX_RETRIES = 3
BASE_RETRY_DELAY = 1

# HiDrive API credentials
CLIENT_ID = "9fe1b9ad74d3891f14e1270708c20780"
CLIENT_SECRET = "6d350ec3781bb674ef0dabe1688a2060"
REFRESH_TOKEN = "rt-znct5efv2boz6avorywgpsxwnu8w"

# Source spreadsheet (from Google Drive) usuario info@photosagrera.com
# "Mi Unidad > PUNTUACIONES SOCIAL > Puntuaciones Concurso Social"                          
social_source_sheet_id = '1uehoM3-I3yEFTjgwDwiCkOwGRToKqJm57thwpK254uE'
social_origen_numcols = 4
social_headers = ["Nº Foto", "Archivo", "Timestamp", "Email", "Autor", "Photo URL ID","Total Puntos"]
social_dest_numcols = len(social_headers)
social_base_path = "/users/photosagrera/SOCIALES"
social_originals_path = f"{social_base_path}/SOCIAL_2025-2026"  # Append the month name to get final path for each contest, i.e. /06_FEBRER
social_numbered_path = f"{social_base_path}/PENDIENTES DE FALLO/AL JURADO"
social_randomize_order = False  # Whether to randomize the order of photos in the destination sheet (for better anonymization during judging)
social_sort_column_index = 4  # Column index for random sort key Author name (0-based, column E in the sheet)
social_dest_url_col_index = 5  # Column index for Photo URL ID in the source data (0-based, column G in the sheet)

#Agustí Umbert: Source spreadsheet (from Google Drive) usuario fmlasagrera@photosagrera.com 
# "Mi Unidad > CONCURSOS> CONCURSO 2026 > AGUSTI UMBERT""
aumbert_source_sheet_id = '1yb0m44PtxLNhTJCQ46bRM4XqaL2SGHQy6XJA0JBChlU'
aumbert_origen_numcols = 8
aumbert_headers = ["Nº Foto", "Archivo", "Timestamp", "Autor", "Email", "Teléfono", "Es Miembro", "Federado", "ID federación", "Photo URL ID", "Random Sort Key", "Total Puntos"]
aumbert_dest_numcols = len(aumbert_headers)
aumbert_base_path = "/users/photosagrera/PREMI AGUSTI UMBERT/Concurso 2026"
aumbert_originals_path = f"{aumbert_base_path}/Originales"
aumbert_numbered_path = f"{aumbert_base_path}/Numeradas"
aumbert_randomize_order = True
aumbert_sort_column_index = 10  # Column index for random sort key Ramdom Sort key (0-based, column K in the sheet)
aumbert_dest_url_col_index = 9  # Column index for Photo URL ID in the source data (0-based, column J in the sheet)

#Cartel FM: Source spreadsheet (from Google Drive) usuario fmlasagrera@phosagrera.com
# "Mi Unidad > CONCURSOS> CONCURSO 2026 > CARTEL FESTA MAJOR"
cartel_source_sheet_id = '1cjnmoPTwAvlL_NY44d-wT6wC2ev6r2IVki4Jwe8PTwM'
cartel_origen_numcols = 5
cartel_headers = ["Nº Foto", "Archivo", "Timestamp", "Autor", "Email", "Teléfono", "Photo URL ID", "Random Sort Key", "Total Puntos"]
cartel_dest_numcols = len(cartel_headers)
cartel_base_path = "/users/photosagrera/CARTEL FESTA MAJOR/Concurso 2026"
cartel_originals_path = f"{cartel_base_path}/Originales"
cartel_numbered_path = f"{cartel_base_path}/Numeradas"
cartel_randomize_order = False
cartel_sort_column_index = 7  # Column index for random sort key Random Sort key (0-based, column H in the sheet)
cartel_dest_url_col_index = 6  # Column index for Photo URL ID in the source data (0-based, column G in the sheet)

# Global dictionary to access contest parameters by name
# Index 0: social_* values | Index 1: aumbert_* values | Index 2: cartel_* values
contest_params = {
    "contest_name": ["Puntuaciones Concurso Social", "Concurso Agustí Umbert", "Concurso Festa Major de La Sagrera"],
    "source_sheet_id": [social_source_sheet_id, aumbert_source_sheet_id, cartel_source_sheet_id],
    "headers": [social_headers, aumbert_headers, cartel_headers],
    "origen_numcols": [social_origen_numcols, aumbert_origen_numcols, cartel_origen_numcols],
    "dest_numcols": [social_dest_numcols, aumbert_dest_numcols, cartel_dest_numcols],
    "base_path": [social_base_path, aumbert_base_path, cartel_base_path],
    "originals_path": [social_originals_path, aumbert_originals_path, cartel_originals_path],
    "numbered_path": [social_numbered_path, aumbert_numbered_path, cartel_numbered_path],
    "randomize_order": [social_randomize_order, aumbert_randomize_order, cartel_randomize_order],
    "sort_column_index": [social_sort_column_index, aumbert_sort_column_index, cartel_sort_column_index],
    "dest_url_col_index": [social_dest_url_col_index, aumbert_dest_url_col_index, cartel_dest_url_col_index]    
}

# Column properties configuration: width (in pixels) and hidden status
column_properties = {
    "Nº Foto": {"width": 50, "hidden": False},
    "Archivo": {"width": 300, "hidden": False},
    "Timestamp": {"width": 130, "hidden": True},
    "Autor": {"width": 200, "hidden": False},
    "Email": {"width": 250, "hidden": False},
    "Teléfono": {"width": 120, "hidden": False},
    "Es Miembro": {"width": 100, "hidden": False},
    "Federado": {"width": 100, "hidden": False},
    "ID federación": {"width": 120, "hidden": False},
    "Photo URL ID": {"width": 150, "hidden": True},
    "Random Sort Key": {"width": 80, "hidden": True},
    "Total Puntos": {"width": 130, "hidden": False}
}

source_sheet_name = "Inscripciones"           # the worksheet with the form responses from Google Forms
destination_sheet_name = "Puntuaciones"       # the worksheet to be created with the photo entries
folder_path = "Mi Unidad > PUNTUACIONES SOCIAL > Puntuacines Concurso Social" # Only for reference

# Google Sheets API credentials
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_file("credentials.json", scopes=scopes)


#Selected contest index: 0 for Puntuaciones Concurso Social, 1 for Agustí Umbert, 2 for Cartel Fiesta Mayor
selected_contest = 9

class RetryableException(Exception):
    """Exception that can be retried"""
    pass


def retry_with_backoff(func, max_retries=MAX_RETRIES, base_delay=BASE_RETRY_DELAY, operation_name=""):
    """Retry a function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempt {attempt + 1}/{max_retries} for {operation_name}")
            return func()
        except (requests.exceptions.RequestException, RetryableException) as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed after {max_retries} attempts for {operation_name}: {str(e)}")
                raise
            wait_time = base_delay * (2 ** attempt)
            logger.warning(f"Retry {attempt + 1}/{max_retries} for {operation_name} after {wait_time}s: {str(e)}")
            time.sleep(wait_time)


class HiDriveAPI:
    """Manages cloud file operations via OAuth2"""
    BASE_URL = "https://api.hidrive.strato.com/2.1"
    TOKEN_URL = "https://my.hidrive.com/oauth2/token"

    def __init__(self, client_id, client_secret, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.token_expiry = 0
        self.refresh_access_token()
        logger.debug("HiDriveAPI initialized")

    def refresh_access_token(self):
        """Refresh OAuth2 access token with retry logic"""
        def _refresh():
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            response = requests.post(self.TOKEN_URL, data=data, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data["access_token"]
            self.token_expiry = time.time() + token_data["expires_in"] - 300  # 5-minute buffer
            logger.debug("Access token refreshed")

        retry_with_backoff(_refresh, operation_name="refresh_access_token")

    def get_headers(self, content_type="application/json"):
        """Get authorization headers, refreshing token if needed"""
        if time.time() > self.token_expiry:
            self.refresh_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type
        }

    def list_files(self, directory):
        """List files in a directory with retry logic"""
        def _list():
            url = f"{self.BASE_URL}/dir"
            params = {
                "path": directory,
                "fields": "members.name,members.type"
            }
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=10)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            response.raise_for_status()
            return response.json().get('members', [])

        return retry_with_backoff(_list, operation_name=f"list_files({directory})")

    def copy_file(self, src_path, dest_path):
        """Copy file with retry logic"""
        def _copy():
            url = f"{self.BASE_URL}/file/copy"
            params = {
                "src": src_path,
                "dst": dest_path
            }
            response = requests.post(url, headers=self.get_headers(), params=params, timeout=30)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            response.raise_for_status()
            return response.json()

        result = retry_with_backoff(_copy, operation_name=f"copy_file({src_path} -> {dest_path})")
        logger.info(f"File copied: {src_path} -> {dest_path}")
        return result

    def check_and_create_directory(self, directory):
        """Check if directory exists, create if not, or recreate if requested"""
        def _check():
            url = f"{self.BASE_URL}/dir"
            params = {"path": directory}
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=10)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            return response

        response = retry_with_backoff(_check, operation_name=f"check_directory({directory})")
        
        if response.status_code == 200:
            logger.debug(f"Directory exists: {directory}")
            return True
        elif response.status_code == 404:
            logger.debug(f"Directory not found: {directory}")
            return False
        else:
            response.raise_for_status()

    def create_directory(self, directory):
        """Create a new directory"""
        def _create():
            url = f"{self.BASE_URL}/dir"
            params = {"path": directory}
            response = requests.post(url, headers=self.get_headers(), params=params, timeout=10)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            response.raise_for_status()
            return response.json()

        retry_with_backoff(_create, operation_name=f"create_directory({directory})")
        #logger.info(f"Directory created: {directory}")

    def remove_directory(self, directory, recursive=False):
        """Remove directory with retry logic"""
        def _remove():
            url = f"{self.BASE_URL}/dir"
            params = {
                "path": directory,
                "recursive": "true" if recursive else "false"
            }
            response = requests.delete(url, headers=self.get_headers(), params=params, timeout=10)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            return response

        response = retry_with_backoff(_remove, operation_name=f"remove_directory({directory})")
        
        if response.status_code == 204:
            #logger.info(f"Directory removed: {directory}")
            pass    
        elif response.status_code == 404:
            logger.debug(f"Directory not found (nothing to remove): {directory}")
        else:
            response.raise_for_status()

    def upload_file(self, file_handle, dest_path):
        """Upload file to HiDrive with retry logic"""
        def _upload():
            url = f"{self.BASE_URL}/file"
            params = {"dir": os.path.dirname(dest_path),
                      "name": os.path.basename(dest_path)}
            headers = self.get_headers(content_type="image/gif")  #content_type="application/octet-stream"
            
            response = requests.post(url, headers=headers, params=params, data=file_handle, timeout=60)
            if response.status_code == 429 or response.status_code == 503:
                raise RetryableException(f"HiDrive rate limit or temporarily unavailable: {response.status_code}")
            response.raise_for_status()
            return response.json()

        result = retry_with_backoff(_upload, operation_name=f"upload_file({dest_path})")
        #logger.info(f"File uploaded: {dest_path}")
        return result


class GoogleDriveAPI:
    """Manages Google Drive file operations"""
    
    def __init__(self, credentials):
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        
        self.credentials = credentials
        self.service = build('drive', 'v3', credentials=credentials)
        logger.debug("GoogleDriveAPI initialized")

    def download_file(self, file_id):
        """Download file from Google Drive by ID with retry logic"""
        def _download():
            from googleapiclient.http import MediaIoBaseDownload
            
            request = self.service.files().get_media(fileId=file_id)
            file_handle = io.BytesIO()
            downloader = MediaIoBaseDownload(file_handle, request)
            done = False
            while done is False:
                try:
                    status, done = downloader.next_chunk()
                except Exception as e:
                    raise RetryableException(f"Error downloading file {file_id}: {str(e)}")
            file_handle.seek(0)
            return file_handle

        return retry_with_backoff(_download, operation_name=f"download_file({file_id})")

    def get_file_metadata(self, file_id):
        """Get file metadata (name, mimeType) with retry logic"""
        def _get_metadata():
            request = self.service.files().get(fileId=file_id, fields='name,mimeType')
            return request.execute()

        return retry_with_backoff(_get_metadata, operation_name=f"get_file_metadata({file_id})")

    def rename_file(self, file_id, new_name):
        """Rename a file in Google Drive with retry logic"""
        def _rename():
            request = self.service.files().update(fileId=file_id, body={'name': new_name})
            return request.execute()

        return retry_with_backoff(_rename, operation_name=f"rename_file({file_id}, {new_name})")


def parse_google_drive_url(url):
    """Extract file ID from Google Drive URL"""
    # Format: https://drive.google.com/open?id=FILE_ID
    if "id=" in url:
        return url.split("id=")[1].split("&")[0]
    # Format: https://drive.google.com/file/d/FILE_ID/view
    elif "/d/" in url:
        return url.split("/d/")[1].split("/")[0]
    else:
        raise ValueError(f"Invalid Google Drive URL: {url}")

def get_filename_from_google_drive_url(google_drive_api, url):
    """Extract filename from Google Drive URL by fetching file metadata
    
    Args:
        google_drive_api: GoogleDriveAPI instance
        url: Google Drive URL or file ID
        
    Returns:
        filename: The name of the file from Google Drive metadata
        
    Raises:
        ValueError: If URL is invalid or file metadata cannot be retrieved
    """
    try:
        file_id = parse_google_drive_url(url)
        file_metadata = google_drive_api.get_file_metadata(file_id)
        filename = file_metadata['name']
        logger.debug(f"Retrieved filename from Google Drive: {filename}")
        return filename
    except ValueError as e:
        logger.error(f"Invalid Google Drive URL: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to get filename from URL {url}: {str(e.message)}")
        raise


def get_config_parameter(workbook, parameter_name):
    """Retrieve a parameter value from the Config worksheet.
    
    Looks for the parameter name in the first column of the Config worksheet
    and returns the corresponding value from the second column.
    
    Args:
        workbook: The gspread workbook instance
        parameter_name: The name of the parameter to retrieve (case-sensitive)
        
    Returns:
        str: The parameter value from the second column
        
    Raises:
        WorksheetNotFound: If the Config worksheet doesn't exist
        ValueError: If the parameter is not found in the Config worksheet
        
    Example:
        >>> value = get_config_parameter(worksheet, "max_photos")
    """
    try:
        # Open the workbook and Config worksheet
        config_worksheet = workbook.worksheet("Config")
        
        # Get all data from Config worksheet
        config_data = config_worksheet.get_all_values()
        
        if not config_data:
            raise ValueError(f"Config worksheet is empty")
        
        # Search for the parameter in the first column
        for row in config_data:
            if len(row) >= 2 and row[0] == parameter_name:
                logger.debug(f"Found parameter '{parameter_name}' with value: {row[1]}")
                return row[1]
        
        # Parameter not found
        raise ValueError(f"Parameter '{parameter_name}' not found in Config worksheet")
        
    except WorksheetNotFound as e:
        logger.error(f"Config worksheet not found in the workbook: {workbook.name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error retrieving parameter '{parameter_name}': {str(e)}")
        raise


def create_destination_rows_dataset(source_data, google_drive_api):
    """Create destination rows dataset from source data for 'Puntuaciones' sheet"""
    logger.info(f"Creating destination rows dataset from source data for 'Puntuaciones' sheet")
    
    try:
        # Add headers: A (Nº Foto) + B (Filename) + C-J (original columns) + K (URL ID) + L (random sort key)
    
        # Build all rows from source data
        all_rows = [contest_params["headers"][selected_contest]] # Start with header row
        num_contest_cols = contest_params["origen_numcols"][selected_contest]
        randomize_order  = contest_params["randomize_order"][selected_contest]

        for source_row in source_data[1:]:  # Skip header row
            if len(source_row) < num_contest_cols:  # Ensure we have at least the numcols of the contest columns
                logger.warning(f"Skipping incomplete row: {source_row}")
                continue

            # Extract URL list from column H (index numcols - 1)
            url_string = source_row[num_contest_cols-1] if len(source_row) > num_contest_cols-1 else ""
            if not url_string.strip():
                logger.warning(f"Row with no photos: {source_row[1]}")
                continue

            # Split URLs by comma
            urls = [url.strip() for url in url_string.split(",")]

            # For each URL, create a new row
            for url in urls:
                try:
                    # Get filename from Google Drive
                    try:
                        filename = get_filename_from_google_drive_url(google_drive_api, url)
                    except Exception as e:
                        logger.warning(f"Failed to retrieve filename for URL {url}: {str(e)}, using empty string")
                        filename = ""
                    
                    # Add 0 as placeholder for Nº Foto at the beginning, plus Filename as second column                   
                    new_row = [0, filename] + source_row[:num_contest_cols-1] + [url]
                    if randomize_order:
                        random_sort_key = random.randint(1, 10000)
                        new_row = new_row + [ f"{random_sort_key:05d}"]

                    all_rows.append(new_row)
                    logger.debug(f"Prepared row for URL: {url}, Filename: {filename}")
                except Exception as e:
                    logger.error(f"Error preparing row for URL {url}: {str(e)}")
                    raise

        logger.info(f"{len(all_rows) - 1} photo rows generated from the source data")

        return all_rows

    except Exception as e:
        logger.error(f"Error creating destination rows dataset: {str(e)}")
        raise



def create_destination_spreadsheet(source_workbook, all_rows):
    """Create and populate 'Puntuaciones' sheet in source workbook from source data using batch operations"""
    logger.info(f"Creating '{destination_sheet_name}' sheet in source workbook")
    
    try:
        num_destination_cols = contest_params["dest_numcols"][selected_contest] 
        # Check if 'Puntuaciones' sheet already exists and remove it
        try:
            puntuaciones_worksheet = source_workbook.worksheet(destination_sheet_name)
            source_workbook.del_worksheet(puntuaciones_worksheet)
            logger.info(f"Removed existing {destination_sheet_name} sheet")
        except WorksheetNotFound:
            pass

        # Add new sheet where to receive the scores and set it as the second sheet (index=1) to keep Form responses as the first sheet
        puntuaciones_worksheet = source_workbook.add_worksheet(title=destination_sheet_name, rows=0, cols=num_destination_cols, index=1)
        logger.info(f"Created new {destination_sheet_name} sheet")

        # Insert all rows 
        logger.info(f"Inserting {len(all_rows)-1} photo rows ")
        try:
            puntuaciones_worksheet.insert_rows(all_rows, 1)

        except Exception as e:
            logger.error(f"Error inserting rows to {destination_sheet_name} sheet: {str(e)}")
            raise

        logger.info(f"{destination_sheet_name} sheet created with {len(all_rows)-1} photo rows")
        return  puntuaciones_worksheet

    except Exception as e:
        logger.error(f"Error creating '{destination_sheet_name}' sheet: {str(e)}")
        raise

def format_destination_spreadsheet(sheet, num_rows):
    # Fill up the column headers row
    #column_headers = [["NUM.", "NOMBRE", "FICHERO JPG", "Total Puntos" ]]
    #sheet.update(column_headers, "A1:D1")
    num_destination_cols = contest_params["dest_numcols"][selected_contest] 
    last_column_letter=chr(64+num_destination_cols)
    cell_range=f"A1:{last_column_letter}1"  # e.g. A1:D1 for 4 columns  
    sheet.format(cell_range, {"textFormat": {"bold": True}})   

    # Set sum formula for the Total column(G) by updating selected range
    cell_range=f"{last_column_letter}2:{last_column_letter}{num_rows+1}"  # e.g. G2:G101 for 100 rows
    
    cell_list = sheet.range(cell_range)
    formula=f'=SI.ERROR(SUMA(INDIRECTO("{chr(64+num_destination_cols+1)}" & FILA()); INDIRECTO("{chr(64+num_destination_cols+2)}" & FILA()); INDIRECTO("{chr(64+num_destination_cols+3)}" & FILA())  ); "")'
    for i,cell in enumerate(cell_list):
        cell.value= formula
    sheet.update_cells(cell_list, value_input_option='USER_ENTERED')

    # Format Header
    cell_range=f"A1:{last_column_letter}1"  # e.g. A1:D1 for 4 columns
    sheetFormat.header_colors(sheet, cell_range)
   
    #Format the sheet rows with alternate colors
    start_row = 2
    end_row = num_rows + 1
    column_range = "A:D"
    # sheetFormat.alternate_colors(sheet, start_row, end_row, column_range)
    
    # Apply column properties (width and hidden status) based on column_properties dictionary
    headers = contest_params["headers"][selected_contest]
    column_hidden_updates = []
    
    for col_index, header_name in enumerate(headers):
        # Get properties from column_properties dictionary
        if header_name in column_properties:
            props = column_properties[header_name]
            col_letter = chr(65 + col_index)  # Convert index to column letter (A=65)
            
            # Apply width
            set_column_width(sheet, col_letter, props["width"])
            
            # Collect hidden column updates for batch processing
            if props["hidden"]:
                column_hidden_updates.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_index,
                            "endIndex": col_index + 1
                        },
                        "properties": {
                            "hiddenByUser": True
                        },
                        "fields": "hiddenByUser"
                    }
                })
    
    # Execute all hidden column updates in a single batch request
    if column_hidden_updates:
        try:
            sheet.spreadsheet.batch_update({"requests": column_hidden_updates})
            logger.info(f"Applied hidden status to {len(column_hidden_updates)} columns")
        except Exception as e:
            logger.warning(f"Error applying hidden column properties: {str(e)}")

def setup_hidrive_folders(api):
    """Create HiDrive folder structure: Concurso 2026, Originales, Numeradas"""
    logger.info("Setting up HiDrive folder structure")
    
    try:
        base_path = contest_params["base_path"][selected_contest]
        originals_path = contest_params["originals_path"][selected_contest]
        numbered_path = contest_params["numbered_path"][selected_contest]
        
        # Check if Concurso 2026 folder exists
        if not api.check_and_create_directory(base_path):
            api.create_directory(base_path)
            logger.info(f"Created folder: {base_path}")

        # Setup Originales folder (delete if exists, then create)
        if api.check_and_create_directory(originals_path):
            api.remove_directory(originals_path, recursive=True)
            logger.info(f"Removed existing folder: {originals_path}")
        api.create_directory(originals_path)
        logger.info(f"Created folder: {originals_path}")

        # Setup Numeradas folder (delete if exists, then create)
        if api.check_and_create_directory(numbered_path):
            api.remove_directory(numbered_path, recursive=True)
            logger.info(f"Removed existing folder: {numbered_path}")
        api.create_directory(numbered_path)
        logger.info(f"Created folder: {numbered_path}")

    except Exception as e:
        logger.error(f"Error setting up HiDrive folders: {str(e)}")
        raise


def sort_worksheet_by_column(all_data, column_index):
    """Sort dataset by specified column"""
    logger.info(f"Sorting dataset by column {column_index} (random sort key)")
    
    try:
        header = all_data[0]
        data_rows = all_data[1:]
        sorted_rows = sorted(data_rows, key=lambda x: x[column_index])
        logger.info(f"Destination dataset of {len(sorted_rows)} rows sorted")
        return [header] + sorted_rows  # Return flat list, not nested

    except Exception as e:
        logger.error(f"Error sorting destination dataset: {str(e)}")
        raise

def number_photos(all_rows):
    """Assign sequential photo numbers to each row in dataset"""
    logger.info("Numbering photos in dataset")
    
    try:
        photo_number = 0
        for row in all_rows[1:]:  # Skip header
            photo_number += 1
            row[0] = f"{photo_number:04d}"  # Update 'Nº foto' column (index 0)
        logger.info(f"Assigned photo numbers up to {photo_number}")
        return all_rows

    except Exception as e:
        logger.error(f"Error numbering photos: {str(e)}")
        raise


def insert_photo_number_column(worksheet):
    """Insert 'Nº foto' column at the beginning (column A)"""
    logger.info("Inserting 'Nº foto' column")
    
    try:
        # Get all data
        all_data = worksheet.get_all_values()
        
        # Insert new column A with header
        new_header = ["Nº foto"] + all_data[0]
        new_rows = [new_header]
        
        # Add empty placeholder for data rows (will be filled during processing)
        for row in all_data[1:]:
            new_rows.append([""] + row)

        # Clear and rewrite
        worksheet.clear()
        worksheet.insert_rows(new_rows, 1)
        logger.info(f"'Nº foto' column inserted")

    except Exception as e:
        logger.error(f"Error inserting photo number column: {str(e)}")
        raise


def upload_photos_to_Hidrive(hidrive_api, google_drive_api, dest_worksheet, selected_contest):
    """Download photos, copy to HiDrive"""
    logger.info("Processing photos and uploading to HiDrive")
    
    try:
        all_data = dest_worksheet.get_all_values()
        photo_number = 0
        url_colum = contest_params["dest_url_col_index"][selected_contest] # Column index for Photo URL ID in the source data (0-based)
        originals_path = contest_params["originals_path"][selected_contest]
        numbered_path = contest_params["numbered_path"][selected_contest]
        
        for row_index, row in enumerate(all_data[1:], start=2):  # Skip header
            try:
                # Column K contains the URL (after adding Filename column, it's now at index 9)
                url = row[url_colum] if len(row) > url_colum else None
                
                if not url or not url.strip():
                    logger.warning(f"Row {row_index} has no URL, skipping")
                    continue

                # Get filename from Google Drive using helper function
                try:
                    filename = get_filename_from_google_drive_url(google_drive_api, url)
                except Exception as e:
                    logger.warning(f"Row {row_index}: Failed to get filename: {str(e)}, using existing value")
                    filename = row[1] if len(row) > 1 else ""

                # Download file
                try:
                    file_id = parse_google_drive_url(url)
                    file_handle = google_drive_api.download_file(file_id)
                except Exception as e:
                    logger.error(f"Row {row_index}: Failed to download file from URL {url}: {str(e)}")
                    continue

                # Get file extension
                file_ext = os.path.splitext(filename)[1]
                original_numbered_filename = f"{row[0]}-{filename}"

                # Copy to Originales folder (use original filename)
                try:
                    dest_path_original = f"{originals_path}/{original_numbered_filename}"
                    _upload_file_to_hidrive(hidrive_api, file_handle, dest_path_original)
                    logger.debug(f"Row {row_index}: Uploaded to Originales: {original_numbered_filename}")
                except Exception as e:
                    logger.error(f"Row {row_index}: Failed to upload to Originales: {str(e)}")
                    continue

                # Increment photo number
                photo_number += 1

                # Create numbered filename for Numeradas folder
                numbered_filename = f"{row[0]}{file_ext}"
                
                # Reset file handle and copy to Numeradas folder
                try:
                    file_handle.seek(0)
                    dest_path_numbered = f"{numbered_path}/{numbered_filename}"
                    _upload_file_to_hidrive(hidrive_api, file_handle, dest_path_numbered)
                    logger.debug(f"Row {row_index}: Uploaded to Numeradas: {numbered_filename}")
                except Exception as e:
                    logger.error(f"Row {row_index}: Failed to upload to Numeradas: {str(e)}")
                    continue

            except Exception as e:
                logger.error(f"Row {row_index}: Unexpected error: {str(e)}")
                continue

        logger.info(f"Photo processing complete: {photo_number} photos processed")
        return photo_number

    except Exception as e:
        logger.error(f"Error in process_photos_and_number: {str(e)}")
        raise


def _upload_file_to_hidrive(api, file_handle, dest_path):
    """Helper to upload file to HiDrive"""
    try:
        # Reset file handle to beginning in case it was read
        file_handle.seek(0)
        api.upload_file(file_handle, dest_path)
        logger.info(f"Successfully uploaded file to: {dest_path}")
    except Exception as e:
        logger.error(f"Failed to upload file to {dest_path}: {str(e)}")
        raise


def rename_photos_in_google_drive(google_drive_api, numbered_rows, selected_contest):
    """Rename files in Google Drive based on photo numbers from numbered_rows
    
    Iterates over the numbered_rows list and renames each file in Google Drive
    to the photo number (first element of the row). Skips rows where the filename
    (second element) is empty, as this indicates a retrieval error.
    
    Args:
        google_drive_api: GoogleDriveAPI instance
        numbered_rows: List of rows with structure [photo_number, filename, ...other_cols..., url]
        selected_contest: Contest index to get the correct URL column index
        
    Returns:
        int: Number of files successfully renamed
    """
    logger.info("Starting Google Drive file rename process")
    
    try:
        url_col_index = contest_params["dest_url_col_index"][selected_contest]
        renamed_count = 0
        
        for row_index, row in enumerate(numbered_rows[1:], start=2):  # Skip header
            try:
                photo_number = row[0]
                filename = row[1] if len(row) > 1 else ""
                
                # Skip if filename is empty (indicates retrieval error)
                if not filename or not filename.strip():
                    logger.debug(f"Row {row_index}: Skipping file rename - empty filename")
                    continue
                
                # Get the Google Drive URL
                url = row[url_col_index] if len(row) > url_col_index else None
                
                if not url or not url.strip():
                    logger.warning(f"Row {row_index}: No URL found, skipping rename")
                    continue
                
                # Extract file ID from URL
                try:
                    file_id = parse_google_drive_url(url)
                except ValueError as e:
                    logger.error(f"Row {row_index}: Invalid Google Drive URL {url}: {str(e)}")
                    continue
                
                # Get file extension from current filename
                file_ext = os.path.splitext(filename)[1]
                new_filename = f"{photo_number}{file_ext}"
                
                # Rename file in Google Drive
                try:
                    google_drive_api.rename_file(file_id, new_filename)
                    logger.info(f"Row {row_index}: Successfully renamed file to {new_filename}")
                    renamed_count += 1
                except Exception as e:
                    logger.error(f"Row {row_index}: Failed to rename file {filename} to {new_filename}: {str(e)}")
                    continue
                    
            except Exception as e:
                logger.error(f"Row {row_index}: Unexpected error during file rename: {str(e)}")
                continue
        
        logger.info(f"File rename process completed: {renamed_count} files successfully renamed")
        return renamed_count
        
    except Exception as e:
        logger.error(f"Error in rename_photos_in_google_drive: {str(e)}")
        raise


def get_command_line_arguments():
    """Get command line arguments"""
    while True:
        if len(sys.argv) >= 2:
            action = sys.argv[1]
        else:
            action = input("Enter action (all/prepare/folders/upload/rename) [all]: ").strip()

        if action == "":
            action = "all"
        
        if action not in ["all", "prepare", "folders", "upload", "rename"]:
            print("Opción inválida. Por favor, entre: all, prepare, folders o upload.")
        else:
            break

    return action

def select_contest_type():
    """Display the contest selection menu and return the selected index."""
    while True:
        print("\n" + "="*50)
        print("Seleccione el tipo de concurso a procesar:")
        print("="*50)
        print(f"1-{contest_params['contest_name'][0]}")
        print(f"2-{contest_params['contest_name'][1]}")
        print(f"3-{contest_params['contest_name'][2]}")
        print("FIN-Para salir sin procesar ningún concurso")
        print("="*50)
        
        choice = input("\nIngrese su opción (1, 2, 3 o FIN): ").strip().upper()
        
        if choice == "FIN":
            print("Saliendo del programa...")
            return None
        elif choice in ["1", "2", "3"]:
            selected_contest = int(choice) - 1
            return selected_contest
        else:
            print("Opción inválida. Por favor, ingrese 1, 2, 3 o FIN.")

def get_contest_month_year():
    while True:
        folder_name = input("Entre el nombre de la subcarpeta de Hidrive bajo SOCIALES/SOCIAL_2025-2026 con las fotos  (Ex. 2026-03 NATURA): ")
        if folder_name.strip() == "":
            print("El nombre de la carpeta no puede estar vacío. Por favor, inténtelo de nuevo.")
        else:
            break

    return folder_name

def ask_confirmation_to_continue():
    """Display the contest selection menu and return the selected index."""
    while True:
       
        choice = input("\nPulse Enter para continuar o FIN para salir: ").strip().upper()
        
        if choice == "FIN":
            print("Saliendo del programa...")
            return None
        elif choice.strip() == "":
            return "OK"
        else:
            print("Opción inválida. Por favor, entre [ENTER] o FIN.")


def main():
    """Main program entry point."""
    global selected_contest, destination_sheet_name  # Declare which globals you'll modify
    
    try:
        selected_contest = select_contest_type()
    
        if selected_contest is None:
            sys.exit(0)
    
 
        action = get_command_line_arguments() # Supported actions: all, prepare, folders, upload, rename
        
        # Initialize clients
        gspread_client = gspread.authorize(credentials)
        hidrive_api = HiDriveAPI(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        google_drive_api = GoogleDriveAPI(credentials)
        source_workbook = gspread_client.open_by_key(contest_params["source_sheet_id"][selected_contest])
        
        if selected_contest == 0:
            destination_sheet_name = get_config_parameter(source_workbook, "CONCURS")
            social_contest_estat = get_config_parameter(source_workbook, "ESTAT")
            contest_folder_name = destination_sheet_name  #get_contest_month_year()
            contest_params['originals_path'][selected_contest] = f"{contest_params['originals_path'][selected_contest]}/{contest_folder_name}"      


        print(f"Ha seleccionado: {contest_params['contest_name'][selected_contest]}")
        print(f"Las fotos se cargaran en la carpeta: {contest_params['originals_path'][selected_contest]}")
        print(f"Las fotos numeradas se dejaran en la carpeta: {contest_params['numbered_path'][selected_contest]}")

        if selected_contest == 0:
            print(f"El tema del mes actualmente configurado es: {destination_sheet_name}")
            print(f"El estado actual para el concurso es: {social_contest_estat}") 

        if ask_confirmation_to_continue() is None:
            sys.exit(0)
        
        logger.info(f"Starting Concurso Social with action: {action}")

        # Create destination spreadsheet from source data
        if action in ["prepare", "all"]:
            logger.info("=" * 60)
            logger.info("Retrieving data from the source Excel spreadsheet")
            logger.info("=" * 60)
            
            # Read source spreadsheet
           
            # source_worksheet = source_workbook.worksheet(source_sheet_name)
            source_worksheet = source_workbook.get_worksheet(0) #  Use the first worksheet as source, which should contain the form responses
            source_data = source_worksheet.get_all_values()
            logger.info(f"Read {len(source_data) - 1} rows from source spreadsheet")


        
        #  Enhance data and sort by random column to create destination sheet
        if action in ["prepare", "all"]:
            logger.info("=" * 60)
            logger.info("Creating the destination sheet with extra columns and sorting by random column")
            logger.info("=" * 60)
            # Create rows dataset for 'Puntuaciones' sheet in source workbook
            all_rows = create_destination_rows_dataset(source_data, google_drive_api)
            
           
            # Sort dataset by random sort key column (index dest_, column K)
            sort_column_index = contest_params["sort_column_index"][selected_contest] # Index of the random sort key column in the dataset (after adding Filename column, it's now at index 10)
            sorted_rows = sort_worksheet_by_column(all_rows, sort_column_index)  # Flat list: [header, row1, row2, ...]

            # Number the photos sequentially in the sorted order
            numbered_rows = number_photos(sorted_rows)  # Modifies in-place, returns reference

            # Create 'Puntuaciones' sheet in source workbook
            dest_worksheet = create_destination_spreadsheet(source_workbook, numbered_rows) 
            format_destination_spreadsheet(dest_worksheet, len(numbered_rows) - 1)  # Exclude header row 

        #  Create folder structure in HiDrive
        if action in ["folders", "all"]:
            logger.info("=" * 60)
            logger.info("Setting up HiDrive folders")
            logger.info("=" * 60)
            setup_hidrive_folders(hidrive_api)

        # Upload photos to MyHidrive 
        if action in ["upload", "all"]:
            logger.info("=" * 60)
            logger.info("Uploading photos to HiDrive")
            logger.info("=" * 60)
            
            # Re-fetch destination worksheet if needed
            if action == "upload":
                dest_workbook = gspread_client.open_by_key(contest_params["source_sheet_id"][selected_contest])
                dest_worksheet = dest_workbook.worksheet(destination_sheet_name)    

            photo_count = upload_photos_to_Hidrive(hidrive_api, google_drive_api, dest_worksheet, selected_contest)
            logger.info(f"Successfully processed {photo_count} photos")

        # Rename files in Google Drive
        if action in ["rename", "all"]:
            logger.info("=" * 60)
            logger.info("Renaming files in Google Drive")
            logger.info("=" * 60)
            
            # Re-fetch destination worksheet if needed
            if action == "rename":
                dest_workbook = gspread_client.open_by_key(contest_params["source_sheet_id"][selected_contest])
                dest_worksheet = dest_workbook.worksheet(destination_sheet_name)
                numbered_rows = dest_worksheet.get_all_values()
            
            rename_count = rename_photos_in_google_drive(google_drive_api, numbered_rows, selected_contest)
            logger.info(f"***Successfully renamed {rename_count} files of {len(numbered_rows)-1} photos in Google Drive ***")

        logger.info("=" * 60)
        logger.info("Process completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {e.__doc__} - {str(e)}")
        sys.exit(1)

   

if __name__ == "__main__":
    main()
