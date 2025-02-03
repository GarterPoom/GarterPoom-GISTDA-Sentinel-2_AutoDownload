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
        self.date_option = 2  # 1 = Number of days from now, 2 = Start Day to End Day
        self.num_days = 10 
        self.end_day = datetime.datetime.strptime('2025-02-03', '%Y-%m-%d').date()
        self.start_day = datetime.datetime.strptime('2025-01-31', '%Y-%m-%d').date()
        self.sep_days = 10
        self.max_cloud_coverage = 15  # Maximum c loud coverage percentage
        
        # User credentials
        self.users = [
            {'email': 'your_email_1', 'password': 'your_password_1'},
            {'email': 'your_email_2', 'password': 'your_password_2'},
            {'email': 'your_email_3', 'password': 'your_password_3'}
        ]
        
        # Satellite configuration
        self.satellite = 'Sentinel-2'
        self.main_directory = 'Sentinel_2' if self.satellite == 'Sentinel-2' else 'Sentinel_1'
        self.levels = ['MSIL2A'] # Sentinel-2 MSIL2A || MSIL1C
        self.small_file_size = 10240
        
        # Area of interest and collection
        self.aoi = "POLYGON((92.0 28.5,109.5 28.5,109.5 5.5,92.0 5.5,92.0 28.5))"  # Removed extra quote
        self.data_collection = "SENTINEL-2"

        # Tiles to process
        self.tiles = ['T47QLA', 'T47QLB', 'T47PMR', 'T47PMT', 'T47QQB', 'T48QTE', 
                     'T48QTD', 'T47PMS', 'T47QNC', 'T48QTF', 'T47PRR', 'T48QVD',
                     'T47QMC', 'T47QMU', 'T47PRQ', 'T48QVE', 'T47PNP', 'T47QQA', 
                     'T48QUF', 'T47PNQ', 'T48PTC', 'T47PQS', 'T47PQT', 'T47PPT',
                     'T47PPS', 'T47QQT', 'T47QQS', 'T47QPT', 'T47QPS', 'T48QYJ',
                     'T48QYK', 'T47PQU', 'T47PPS', 'T47PPR', 'T48PTA', 'T47QRV', 
                     'T47QPC', 'T47QNB', 'T47QPB', 'T47QPA', 'T47QLV', 'T48PTB',
                     'T48PUB', 'T48PVB', 'T48PWB', 'T48PWC', 'T48PUC', 'T48PVC',
                     'T47PNS']
        
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
        """Setup logging file with timestamp"""
        log_date = datetime.datetime.now()
        log_name = f"DownSN_GISTDA_LOG_{log_date.strftime('%Y%m%d%H%M')}.txt"
        log_path = self.log_dir / log_name
        self.logger = open(log_path, 'w')
        self.log_and_print('Download Sentinel-2 file')
        self.log_and_print('Script Download Sentinel-2 From Gistda Version 1.13')
        self.log_and_print(f"Starting time is: {datetime.datetime.now()}")
        self.log_and_print(f"Maximum cloud coverage set to: {self.max_cloud_coverage}%")

    def search_sentinel_data(self, date_range, tile):
        """Search for Sentinel data based on the given date range, tile, and cloud coverage"""
        start_date, end_date = date_range
        try:
            # Construct the URL for the API query with cloud coverage filter
            url = (f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
                f"$filter=contains(Name,'{tile}') and "
                f"Collection/Name eq '{self.data_collection}' and "
                f"OData.CSC.Intersects(area=geography'SRID=4326;{self.aoi}') and "
                f"ContentDate/Start gt {start_date}T00:00:00.000Z and "
                f"ContentDate/Start lt {end_date}T00:00:00.000Z and "
                f"Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value lt {self.max_cloud_coverage})")  # Fixed cloud coverage filter syntax
        
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
            self.log_and_print(f"Error searching data for {start_date} to {end_date}: {str(e)}")
            time.sleep(120)
            return []

    # [Rest of the methods remain the same as in the original code]
    def log_and_print(self, message):
        """Helper method to both print and log a message"""
        print(message)
        if self.logger:
            self.logger.write(f"{message}\n")
            self.logger.flush()

    def get_random_credentials(self):
        """Get random user credentials"""
        user = random.choice(self.users)
        return user['email'], user['password']

    def get_keycloak_token(self, username: str, password: str) -> str:
        """Get authentication token"""
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
        """Calculate date ranges for search"""
        if self.date_option == 1:
            start_date = self.end_day - timedelta(self.num_days)
            date_ranges = [[start_date.strftime("%Y-%m-%d"), self.end_day.strftime("%Y-%m-%d")]]
        else:
            date_ranges = [[self.start_day.strftime("%Y-%m-%d"), self.end_day.strftime("%Y-%m-%d")]]
            
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

    def download_file(self, product, year_dir):
        """Download a single file with progress display"""
        try:
            username, password = self.get_random_credentials()
            token = self.get_keycloak_token(username, password)
            
            session = requests.Session()
            session.headers.update({'Authorization': f'Bearer {token}'})
            
            product_id, product_name, checksum, content_length = product
            filename = f"{product_name[:-5]}.zip"
            file_path = year_dir / filename
            url = f'https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value'
            
            response = session.get(url, allow_redirects=False)
            while response.status_code in (301, 302, 303, 307):
                url = response.headers['Location']
                response = session.get(url, allow_redirects=False)
            response = session.get(url, verify=False, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                total_size = int(response.headers.get('content-length', 0))
                progress = tqdm(total=total_size, unit='iB', unit_scale=True, desc=filename)
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(len(chunk))
                progress.close()
            
            self.downloaded_files.append(filename)
            self.log_and_print(f"Download completed: {filename}")
            return True
            
        except Exception as e:
            self.log_and_print(f"Error downloading {product_name}: {str(e)}")
            if file_path.exists():
                file_path.unlink()
            return False

    def verify_downloads(self):
        """Verify all downloaded files"""
        for filename in self.downloaded_files:
            year = filename[11:15]
            year_dir = self.data_dir / year
            file_path = year_dir / filename
            
            if not file_path.exists():
                continue
                
            try:
                if not zipfile.is_zipfile(file_path):
                    self.log_and_print(f"Corrupt zip file, removing: {filename}")
                    file_path.unlink()
            except Exception as e:
                self.log_and_print(f"Error verifying {filename}: {str(e)}")
                if file_path.exists():
                    file_path.unlink()

    def run(self):
        """Main execution method"""
        try:
            self.setup_logging()
            date_ranges = self.calculate_date_ranges()
            
            for date_range in date_ranges:
                self.log_and_print(f"Processing date range: {date_range[0]} to {date_range[1]}")
                tiles_downloaded_in_range = set()
                
                for tile in self.tiles:
                    if tile in tiles_downloaded_in_range:
                        continue
                    
                    products = self.search_sentinel_data(date_range, tile)
                    products = [p for p in products if any(level in p[1] for level in self.levels)]
                    
                    if products:
                        product = products[0]
                        year = product[1][11:15]
                        year_dir = self.data_dir / year
                        year_dir.mkdir(exist_ok=True)
                        
                        filename = f"{product[1][:-5]}.zip"
                        file_path = year_dir / filename
                        
                        if file_path.exists():
                            if zipfile.is_zipfile(file_path):
                                self.log_and_print(f"Tile {tile} already exists: {filename}")
                                tiles_downloaded_in_range.add(tile)
                                continue
                            else:
                                file_path.unlink()
                        
                        if self.download_file(product, year_dir):
                            tiles_downloaded_in_range.add(tile)
                            self.log_and_print(f"Downloaded tile {tile}: {filename}")
                
                self.log_and_print(f"Tiles downloaded in range {date_range}: {tiles_downloaded_in_range}")
            
            self.verify_downloads()
            self.log_and_print("Download process completed successfully")
            self.log_and_print(f"Ending time is: {datetime.datetime.now()}")
            
        except Exception as e:
            self.log_and_print(f"Critical error in download process: {str(e)}")
        finally:
            if self.logger:
                self.logger.close()

if __name__ == "__main__":
    downloader = SentinelDownloader()
    downloader.run()