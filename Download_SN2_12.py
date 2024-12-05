import datetime
import os
import random
import time
import zipfile
import requests
from datetime import timedelta
from pathlib import Path
from tqdm import tqdm
import shutil

class SentinelDownloader:
    def __init__(self):
        # Configuration
        """
        Initialize the SentinelDownloader object.

        Parameters
        ----------
        None

        Notes
        -----
        This method sets up the configuration for the SentinelDownloader object.
        The configuration includes the date range to look back for downloads, the user
        credentials, the satellite configuration, the area of interest, the data
        collection, and the tiles to process. The method also initializes the
        necessary directories and sets up the download tracking.
        """
        self.date_option = 1  # 1 = Number of days from now, 2 = Start Day to End Day
        self.num_days = 10 # specifies the total number of days from the current date to look back for downloads
        self.end_day = datetime.datetime.strptime('2024-11-30', '%Y-%m-%d').date()  # specify end date in YYYY-MM-DD format
        self.start_day = datetime.datetime.strptime('2024-11-01', '%Y-%m-%d').date()  # specify start date in YYYY-MM-DD format
        self.sep_days = 10
        
        # User credentials
        self.users = [
            {'email': 'siripoom.su@gmail.com', 'password': '799M94401%f6'},
            {'email': 'SIRIPOOM31155@gmail.com', 'password': 'iezLxeZ945$9tfmX*A*rDp3WHW$D8y'},
            {'email': '6231302018@lamduan.mfu.ac.th', 'password': 'AVCwnQCNVs3ZVn%h&!NpJFxYF*nR9W'}
        ]
        
        # Satellite configuration
        self.satellite = 'Sentinel-2'
        self.main_directory = 'Sentinel_2' if self.satellite == 'Sentinel-2' else 'Sentinel_1'
        self.levels = ['MSIL2A']
        self.small_file_size = 10240
        
        # Area of interest and collection
        self.aoi = "POLYGON((92.0 28.5,109.5 28.5,109.5 5.5,92.0 5.5,92.0 28.5))'"
        self.data_collection = "SENTINEL-2"
        
        # Tiles to process
        self.tiles = ['T47QLA', 'T47QLB', 'T47PMR', 'T47PMT', 'T47QQB', 'T48QTE', 
                     'T48QTD', 'T47PMS', 'T47QNC', 'T48QTF', 'T47PRR', 'T48QVD',
                     'T47QMC', 'T47QMU', 'T47PRQ', 'T48QVE', 'T47PNP', 'T47QQA', 
                     'T48QUF', 'T47PNQ', 'T48PTC']
        
        # Initialize paths
        self.root_dir = Path(os.getcwd())
        self.log_dir = self.root_dir / 'download_log'
        self.data_dir = self.root_dir / self.main_directory
        
        # Create necessary directories
        self.log_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize download tracking
        self.downloaded_files = []
        self.logger = None

    def setup_logging(self):
        """Setup logging file with timestamp
        
        This method sets up the logging file with the current timestamp.
        The logging file is created in the 'download_log' directory.
        """
        log_date = datetime.datetime.now()
        log_name = f"DownSN_GISTDA_LOG_{log_date.strftime('%Y%m%d%H%M')}.txt"
        log_path = self.log_dir / log_name
        self.logger = open(log_path, 'w')
        self.log_and_print('Download Sentinel-2 file')
        self.log_and_print('Script Download Sentinel-2 From Gistda Version 1.12')
        self.log_and_print(f"Starting time is: {datetime.datetime.now()}")

    def log_and_print(self, message):
        """
        Helper method to both print and log a message
        
        Parameters
        ----------
        message : str
            The message to be logged and printed
        
        Notes
        -----
        This method is used to log and print messages throughout the downloading process.
        It is useful for debugging and tracking the progress of the downloading process.
        """
        print(message)
        if self.logger:
            self.logger.write(f"{message}\n")
            self.logger.flush()

    def get_random_credentials(self):
        """Get random user credentials
        
        This method is used to get a random user credentials from the list of users.
        The user credentials are used to authenticate with the API.
        """
        user = random.choice(self.users)
        return user['email'], user['password']

    def get_keycloak_token(self, username: str, password: str) -> str:
        """Get authentication token
        
        This method is used to get an authentication token from the Keycloak server.
        The authentication token is then used to authenticate with the API.
        
        Parameters
        ----------
        username : str
            The username to use for authentication
        password : str
            The password to use for authentication
        
        Returns
        -------
        str
            The authentication token
        
        Raises
        ------
        Exception
            If the authentication token creation fails
        """
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password"
        }
        try:
            response = requests.post(
                "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                data=data
            )
            response.raise_for_status()
            return response.json()["access_token"]
        except Exception as e:
            raise Exception(f"Keycloak token creation failed: {str(e)}")

    def calculate_date_ranges(self):
        """
        Calculate date ranges for search
        
        This method calculates the date ranges that will be used for searching Sentinel data.
        The date ranges are calculated based on the date option chosen by the user. If the date
        option is 1, the date range is calculated as the current date minus the number of days specified
        by the user. If the date option is 2, the date range is the start date to the end date specified
        by the user.
        
        The date ranges are then split into 10-day chunks. This is done to avoid hitting the API rate
        limit.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        list
            The list of date ranges that will be used for searching Sentinel data
        """
        if self.date_option == 1:
            start_date = self.end_day - timedelta(self.num_days)
            date_ranges = [[start_date.strftime("%Y-%m-%d"), self.end_day.strftime("%Y-%m-%d")]]
        else:
            date_ranges = [[self.start_day.strftime("%Y-%m-%d"), self.end_day.strftime("%Y-%m-%d")]]
            
        # Split into 10-day chunks
        final_ranges = []
        for start, end in date_ranges:
            current = datetime.datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.datetime.strptime(end, '%Y-%m-%d')
            while current < end_date:
                chunk_end = min(current + timedelta(days=10), end_date)
                final_ranges.append([
                    current.strftime("%Y-%m-%d"),
                    chunk_end.strftime("%Y-%m-%d")
                ])
                current = chunk_end + timedelta(days=1)
        
        return final_ranges


    def search_sentinel_data(self, date_range, tile):
        """
        Search for Sentinel data based on the given date range and tile
        
        Parameters
        ----------
        date_range : list
            The date range to search for
        tile : str
            The tile to search for
        
        Returns
        -------
        list
            A list of tuples containing the product ID, name, checksum, and content length
        """
        start_date, end_date = date_range
        try:
            # Construct the URL for the API query
            url = (f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
                  f"$filter=contains(Name,'{tile}') and "
                  f"Collection/Name eq '{self.data_collection}' and "
                  f"OData.CSC.Intersects(area=geography'SRID=4326;{self.aoi}) and "
                  f"ContentDate/Start gt {start_date}T00:00:00.000Z and "
                  f"ContentDate/Start lt {end_date}T00:00:00.000Z")
            
            # Get the response from the API
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Extract the product information from the response
            products = []
            for item in data['value'][:20]:  # Limit to 20 items
                products.append([
                    item['Id'],
                    item['Name'],
                    item['Checksum'],
                    item['ContentLength']
                ])
            return products
        except Exception as e:
            # Log the error and wait for 2 minutes before retrying
            self.log_and_print(f"Error searching data for {start_date} to {end_date}: {str(e)}")
            time.sleep(120)
            return []

    def download_file(self, product, year_dir):
        """
        Download a single file with progress display
        
        Parameters
        ----------
        product : tuple
            A tuple containing the product ID, name, checksum, and content length
        year_dir : Path
            The directory path to save the downloaded file
        
        Returns
        -------
        bool
            True if the download is successful, False otherwise
        """
        try:
            # Get a random user credentials
            username, password = self.get_random_credentials()
            
            # Get the authentication token
            token = self.get_keycloak_token(username, password)
            
            # Create a requests session with the authentication token
            session = requests.Session()
            session.headers.update({'Authorization': f'Bearer {token}'})
            
            # Extract the product information
            product_id, product_name, checksum, content_length = product
            filename = f"{product_name[:-5]}.zip"
            file_path = year_dir / filename
            url = f'https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value'
            
            # Make the API request
            response = session.get(url, allow_redirects=False)
            while response.status_code in (301, 302, 303, 307):
                url = response.headers['Location']
                response = session.get(url, allow_redirects=False)
            response = session.get(url, verify=False, stream=True)
            response.raise_for_status()
            
            # Save the file to disk
            with open(file_path, 'wb') as f:
                total_size = int(response.headers.get('content-length', 0))
                progress = tqdm(total=total_size, unit='iB', unit_scale=True, desc=filename)
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(len(chunk))
                progress.close()
            
            # Add the file to the list of downloaded files
            self.downloaded_files.append(filename)
            self.log_and_print(f"Download completed: {filename}")
            return True
            
        except Exception as e:
            # Log the error and remove the file if it exists
            self.log_and_print(f"Error downloading {product_name}: {str(e)}")
            if file_path.exists():
                file_path.unlink()
            return False

    def verify_downloads(self):
        """
        Verify all downloaded files
        
        This method verifies all downloaded files by checking if they exist and are valid zip files.
        If a file is corrupt or missing, it is removed.
        """
        for filename in self.downloaded_files:
            year = filename[11:15]
            year_dir = self.data_dir / year
            file_path = year_dir / filename
            
            if not file_path.exists():
                # Skip if the file does not exist
                continue
                
            try:
                if not zipfile.is_zipfile(file_path):
                    # Remove the file if it is corrupt
                    self.log_and_print(f"Corrupt zip file, removing: {filename}")
                    file_path.unlink()
            except Exception as e:
                # Log the error and remove the file if it exists
                self.log_and_print(f"Error verifying {filename}: {str(e)}")
                if file_path.exists():
                    file_path.unlink()

    def run(self):
        """
        Main execution method
        
        This method is the main entry point for the SentinelDownloader class. It sets up the logging,
        calculates the date ranges to search for, and processes each date range and tile by searching
        for products, filtering them by level, and downloading each product.
        
        :return: None
        """
        try:
            self.setup_logging()
            
            # Calculate the date ranges to search for
            date_ranges = self.calculate_date_ranges()
            
            # Process each date range
            for date_range in date_ranges:
                self.log_and_print(f"Processing date range: {date_range[0]} to {date_range[1]}")
                
                # Process each tile
                for tile in self.tiles:
                    # Search for products
                    products = self.search_sentinel_data(date_range, tile)
                    
                    # Filter products by level
                    products = [p for p in products if any(level in p[1] for level in self.levels)]
                    
                    # Process each product
                    for product in products:
                        year = product[1][11:15]
                        year_dir = self.data_dir / year
                        
                        # Create the year directory if it doesn't exist
                        year_dir.mkdir(exist_ok=True)
                        
                        # Check if file already exists and is valid
                        filename = f"{product[1][:-5]}.zip"
                        file_path = year_dir / filename
                        
                        if file_path.exists():
                            if zipfile.is_zipfile(file_path):
                                # Skip if the file already exists and is valid
                                continue
                            else:
                                # Remove the file if it is corrupt
                                file_path.unlink()
                        
                        # Download file
                        self.download_file(product, year_dir)
            
            # Verify all downloads at the end
            self.verify_downloads()
            
            self.log_and_print("Download process completed successfully")
            self.log_and_print("Sentinel-2 Imagery havee been success extracted")
            self.log_and_print(f"Ending time is: {datetime.datetime.now()}")
            
        except Exception as e:
            self.log_and_print(f"Critical error in download process: {str(e)}")
        finally:
            if self.logger:
                self.logger.close()

if __name__ == "__main__":
    downloader = SentinelDownloader()
    downloader.run()