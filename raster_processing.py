import os
import sys
from osgeo import gdal
from pathlib import Path
import time
import logging
def setup_logging():
    """
    Set up logging to help diagnose issues.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sentinel_processing.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def resample_image(input_path, output_path, target_resolution=10):
    """
    Resamples a single image to a target resolution using GDAL and saves it as a compressed GeoTIFF file.
    """
    try:
        logger.info(f"Resampling image: {input_path} to {output_path} at {target_resolution}m resolution.")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Open the input dataset
        src_ds = gdal.Open(input_path)
        if not src_ds:
            logger.error(f"Could not open {input_path}")
            return False
            
        # Get the input resolution
        gt = src_ds.GetGeoTransform()
        input_res = gt[1]  # pixel width
        
        # Calculate new dimensions
        src_xsize = src_ds.RasterXSize
        src_ysize = src_ds.RasterYSize
        dst_xsize = int(src_xsize * (input_res / target_resolution))
        dst_ysize = int(src_ysize * (input_res / target_resolution))

        # Create translation options
        translate_options = gdal.TranslateOptions(
            format='GTiff',
            width=dst_xsize,
            height=dst_ysize,
            resampleAlg=gdal.GRA_NearestNeighbour,
            creationOptions=[
                'COMPRESS=LZW',
                'PREDICTOR=2',
                'TILED=YES',
                'BLOCKXSIZE=256',
                'BLOCKYSIZE=256',
                'BIGTIFF=YES'
            ]
        )
        
        # Perform resampling with compression
        gdal.Translate(
            destName=output_path,
            srcDS=src_ds,
            options=translate_options
        )
        
        # Close the dataset
        src_ds = None
        
        logger.info(f"Resampling completed with compression: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error resampling image {input_path}: {e}", exc_info=True)
        return False

def safe_remove(file_path, max_attempts=5, delay=1):
    """
    Safely remove a file with multiple attempts and delay between attempts.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        return True
    
    for attempt in range(max_attempts):
        try:
            file_path.unlink()
            return True
        except PermissionError:
            if attempt < max_attempts - 1:
                time.sleep(delay)
            continue
        except Exception as e:
            logger.error(f"Error removing file {file_path}: {e}", exc_info=True)
            return False
    return False

def build_pyramids_nearest(raster_path, overview_levels=[2, 4, 8, 16, 32], resample_alg='NEAREST'):
    """
    Build raster pyramids (overviews) using Nearest Neighbor resampling.
    
    Args:
        raster_path (str): Path to the GeoTIFF file
        overview_levels (list): List of overview levels to build
        resample_alg (str): Resampling algorithm ('NEAREST', 'AVERAGE', etc.)
    """
    try:
        logger.info(f"Building pyramids for: {raster_path}")
        
        dataset = gdal.Open(raster_path, gdal.GA_Update)
        if not dataset:
            logger.error(f"Could not open raster for pyramid building: {raster_path}")
            return False
        
        # Build overviews
        dataset.BuildOverviews(resample_alg, overview_levels)
        dataset = None  # Close dataset
        
        logger.info(f"Successfully built pyramids: {overview_levels} using {resample_alg}")
        return True

    except Exception as e:
        logger.error(f"Error building pyramids for {raster_path}: {e}", exc_info=True)
        return False

def process_bands(input_folder, output_folder, scl_output_folder=None):
    """
    Processes Sentinel-2 band files in a given input folder with GDAL compression.
    Optionally exports SCL (Scene Classification Layer) to a separate folder.
    
    Args:
        input_folder (str or Path): Input folder containing JP2 files
        output_folder (str or Path): Output folder for band files
        scl_output_folder (str or Path, optional): Output folder for SCL files
    """
    temp_folder = None
    try:
        logger.info(f"Processing bands in folder: {input_folder}")
        
        input_folder = Path(input_folder)
        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)
        
        # Handle SCL output folder
        if scl_output_folder:
            scl_output_folder = Path(scl_output_folder)
            scl_output_folder.mkdir(parents=True, exist_ok=True)
        
        jp2_files = list(input_folder.glob('*.jp2'))
        if not jp2_files:
            logger.warning(f"No JP2 files found in the input folder: {input_folder}")
            return

        temp_folder = output_folder / 'temp'
        temp_folder.mkdir(parents=True, exist_ok=True)

        resampled_files = []
        band_paths = {}
        scl_file = None

        for jp2_file in jp2_files:
            output_path = temp_folder / f"{jp2_file.stem}_resampled.tif"
            
            if resample_image(str(jp2_file), str(output_path)):
                resampled_files.append(str(output_path))
                
                # Band mapping
                band_map = {
                    'B01': 'B01', 'B02': 'B02', 'B03': 'B03', 'B04': 'B04', 'B05': 'B05', 'B06': 'B06', 'B07': 'B07', 
                    'B08': 'B08', 'B8A': 'B8A', 'B09': 'B09','B11': 'B11', 'B12': 'B12'
                }
                
                # Check for SCL file
                if 'SCL' in jp2_file.name:
                    scl_file = str(output_path)
                    continue
                
                for band_key in band_map:
                    if band_key in jp2_file.name:
                        band_paths[band_key] = str(output_path)
                        break

        # Process regular bands
        # B04, B03, B02 for natural color composite as default
        ordered_bands = ['B04', 'B03', 'B02', 'B01', 'B05', 'B06', 'B07', 
                         'B08', 'B8A', 'B09', 'B11', 'B12']
        final_band_order = [band for band in ordered_bands if band in band_paths]
        final_resampled_files = [band_paths[band] for band in final_band_order]

        if not final_resampled_files:
            raise ValueError("No valid band files were processed")

        # Filename generation
        sample_filename = jp2_files[0].name
        parts = sample_filename.split('_')
        tile_date_timestamp = f"{parts[0]}_{parts[1]}"
        output_filename = f"{tile_date_timestamp}.tif"
        output_path = output_folder / output_filename

        logger.info(f"Creating compressed output file: {output_path}")

        # Create VRT with options
        vrt_options = gdal.BuildVRTOptions(separate=True)
        vrt_path = str(temp_folder / 'temp.vrt')
        vrt_ds = gdal.BuildVRT(vrt_path, final_resampled_files, options=vrt_options)
        
        # Create final output with compression
        translate_options = gdal.TranslateOptions(
            format='GTiff',
            creationOptions=[
                'COMPRESS=LZW',
                'PREDICTOR=2',
                'TILED=YES',
                'BLOCKXSIZE=256',
                'BLOCKYSIZE=256',
                'BIGTIFF=YES',
            ],
            stats=True  # <-- THIS IS THE KEY FIX: Calculate and save statistics
        )
        
        output_ds = gdal.Translate(
            destName=str(output_path),
            srcDS=vrt_ds,
            options=translate_options
        )

        # Set band descriptions and color interpretation for default RGB display
        color_map = {
            'B04': gdal.GCI_RedBand,
            'B03': gdal.GCI_GreenBand,
            'B02': gdal.GCI_BlueBand
        }
        for idx, band_name in enumerate(final_band_order, start=1):
            band = output_ds.GetRasterBand(idx)
            if band:
                band.SetDescription(band_name)
                if band_name in color_map:
                    band.SetColorInterpretation(color_map[band_name])

        # Close VRT dataset
        output_ds = None
        vrt_ds = None

        # ✅ BUILD PYRAMIDS HERE
        build_pyramids_nearest(str(output_path))
        
        if not output_path.exists():
            raise ValueError(f"Failed to create output file: {output_path}")
        
        # Export SCL if requested and file exists
        if scl_file and scl_output_folder:
            scl_output_filename = f"{tile_date_timestamp}_SCL.tif"
            scl_output_path = scl_output_folder / scl_output_filename
            
            translate_options_scl = gdal.TranslateOptions(
                format='GTiff',
                creationOptions=[
                    'COMPRESS=LZW',
                    'PREDICTOR=2',
                    'TILED=YES',
                    'BLOCKXSIZE=256',
                    'BLOCKYSIZE=256'
                ]
            )
            
            gdal.Translate(
                destName=str(scl_output_path),
                srcDS=scl_file,
                options=translate_options_scl
            )
            
            logger.info(f"Exported SCL file: {scl_output_path}")
        
        # Clean up temporary files
        logger.info("Cleaning up temporary files.")
        for file in resampled_files:
            safe_remove(file)
        safe_remove(vrt_path)
        
        if temp_folder and temp_folder.exists():
            try:
                temp_folder.rmdir()
            except Exception as e:
                logger.warning(f"Could not remove temp folder: {e}")

    except Exception as e:
        logger.error(f"Error processing bands in {input_folder}: {e}", exc_info=True)
        raise
    
    finally:
        # Final cleanup attempt
        if temp_folder and temp_folder.exists():
            try:
                for file in temp_folder.glob('*'):
                    safe_remove(file)
                temp_folder.rmdir()
            except Exception as e:
                logger.warning(f"Failed final cleanup of temp folder: {e}")

def find_and_process_folders(root_folder, output_folder, scl_output_folder):
    """
    Searches for and processes folders containing .jp2 files.
    """
    try:
        root_folder = Path(root_folder)
        output_folder = Path(output_folder)
        scl_output_folder = Path(scl_output_folder)
        
        logger.info(f"Searching for folders in: {root_folder}")
        
        processed_folders = 0
        for dirpath in root_folder.rglob('*'):
            if dirpath.is_dir() and any(f.suffix == '.jp2' for f in dirpath.iterdir()):
                relative_path = dirpath.relative_to(root_folder)
                current_output_folder = output_folder / relative_path
                current_scl_output_folder = scl_output_folder / relative_path
                logger.info(f"Found JP2 files in: {dirpath}. Processing...")
                process_bands(dirpath, current_output_folder, current_scl_output_folder)
                processed_folders += 1
        
        if processed_folders == 0:
            logger.warning("No folders with JP2 files were found to process.")
        else:
            logger.info(f"All {processed_folders} folders processed.")
        
    except Exception as e:
        logger.error(f"Error processing folders: {e}", exc_info=True)
        sys.exit(1)

def main():
    try:
        # Enable GDAL exceptions
        gdal.UseExceptions()
        
        # Get current working directory
        current_dir = Path.cwd()
        logger.info(f"Current working directory: {current_dir}")

        # Check input folders
        root_folder = current_dir / 'SN2_Extract'
        output_folder = current_dir / 'Raster_Processed'
        scl_output_folder = current_dir / 'SCL_Classified'

        # Check if input folder exists
        if not root_folder.exists():
            logger.error(f"Input folder 'SN2_Extract' does not exist in {current_dir}")
            logger.info("Please create the 'SN2_Extract' folder and place your Sentinel-2 JP2 files inside.")
            sys.exit(1)

        # Create output folders if they don't exist
        output_folder.mkdir(parents=True, exist_ok=True)
        scl_output_folder.mkdir(parents=True, exist_ok=True)

        # Find and process JP2 files
        jp2_files = list(root_folder.rglob('*.jp2'))
        
        if not jp2_files:
            logger.error(f"No .jp2 files found in {root_folder} or its subdirectories.")
            logger.info("Ensure Sentinel-2 .jp2 files are present in the 'SN2_Extract' folder.")
            sys.exit(1)

        logger.info(f"Found {len(jp2_files)} JP2 files to process")
        
        # Run processing
        find_and_process_folders(root_folder, output_folder, scl_output_folder)
        
        logger.info("Processing complete.")

    except Exception as e:
        logger.critical(f"An unexpected error occurred in main(): {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()