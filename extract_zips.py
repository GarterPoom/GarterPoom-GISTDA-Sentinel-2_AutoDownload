import os
import zipfile
import shutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s',
                    handlers=[
                        logging.StreamHandler()
                    ]) # Add more handlers as needed

def extract_zips(root_folder):
    """
    Extract zip files ensuring original folder name is maintained.
    Processes all subdirectories recursively.
    """
    # Convert root_folder to absolute path
    root_folder = os.path.abspath(root_folder)

    # Process all subdirectories
    for subdir, _, files in os.walk(root_folder):
        zip_files = [f for f in files if f.lower().endswith('.zip')]
        
        if not zip_files:
            continue

        logging.info(f"Found {len(zip_files)} zip file(s) in {subdir}")

        for zip_filename in zip_files:
            try:
                zip_path = os.path.join(subdir, zip_filename)
                extract_folder_name = os.path.splitext(zip_filename)[0]
                extract_folder = os.path.join(subdir, extract_folder_name)

                if os.path.exists(extract_folder):
                    logging.warning(f"Folder {extract_folder_name} already exists. Skipping.")
                    continue

                os.makedirs(extract_folder, exist_ok=True)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_folder)

                logging.info(f"Successfully extracted {zip_filename} to {extract_folder}")
            except zipfile.BadZipFile:
                logging.error(f"Corrupt zip file: {zip_filename}")
            except PermissionError:
                logging.error(f"Permission denied when extracting: {zip_filename}")
            except Exception as e:
                logging.error(f"Unexpected error extracting {zip_filename}: {e}")

def extract_jp2_files(root_folder, output_folder):
    """
    Extract Sentinel-2 JP2 files from all subdirectories.
    """
    root_folder = os.path.abspath(root_folder) 
    output_folder = os.path.abspath(output_folder)
    os.makedirs(output_folder, exist_ok=True)

    resolution_priority = ['R10m', 'R20m', 'R60m']
    jp2_files = {}
    scl_files = {}

    for root, dirs, files in os.walk(root_folder):
        resolution = next((res for res in resolution_priority if root.endswith(res)), None)
        if not resolution:
            continue

        for file in files:
            if file.lower().endswith('.jp2'):
                try:
                    parts = file.split('_')
                    if len(parts) < 2:
                        continue

                    band_identifier = parts[-2]
                    granule_id = parts[0] + "_" + parts[1]

                    if band_identifier == 'SCL' and resolution == 'R20m':
                        scl_files[granule_id] = (resolution, root, file)
                        continue

                    if band_identifier.startswith('B'):
                        band_number = band_identifier[1:]
                        if band_number in ['01', '09']:  # Skip Band 01 and 09
                            continue

                        key = (granule_id, band_number)
                        if key not in jp2_files or resolution_priority.index(resolution) < resolution_priority.index(jp2_files[key][0]):
                            jp2_files[key] = (resolution, root, file)
                except Exception as e:
                    logging.error(f"Error processing file {file}: {e}")

    processed_granules = set()
    for (granule_id, band_number), (_, root, file) in jp2_files.items():
        try:
            granule_output_folder = os.path.join(output_folder, granule_id)
            os.makedirs(granule_output_folder, exist_ok=True)

            source_path = os.path.join(root, file)
            destination_path = os.path.join(granule_output_folder, file)

            shutil.copy2(source_path, destination_path)
            logging.info(f"Copied {file} (Band {band_number}) to {granule_output_folder}")
            processed_granules.add(granule_id)
        except Exception as e:
            logging.error(f"Error copying file {file}: {e}")

    for granule_id, (resolution, root, file) in scl_files.items():
        try:
            if granule_id in processed_granules:
                granule_output_folder = os.path.join(output_folder, granule_id)
                source_path = os.path.join(root, file)
                destination_path = os.path.join(granule_output_folder, file)
                shutil.copy2(source_path, destination_path)
                logging.info(f"Copied {file} (SCL at 20m) to {granule_output_folder}")
        except Exception as e:
            logging.error(f"Error copying SCL file {file}: {e}")

    logging.info(f"Successfully extracted {len(jp2_files)} JP2 files and {len(scl_files)} SCL files to {output_folder}")

# Main execution
if __name__ == "__main__":
    current_dir = r'Sentinel_2'  # Root directory containing ZIP files and subfolders
    output_dir = r'SN2_Extract'  # Folder to save extracted JP2 files

    extract_zips(current_dir)  # Extract ZIP files recursively
    extract_jp2_files(current_dir, output_dir)  # Extract JP2 files recursively

    logging.info("Extraction completed.")