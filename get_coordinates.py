import requests
import json
import os
import re
import logging
# Set up logging
logging.basicConfig(level=logging.INFO, filename="coordinates_scrape.log", format="%(asctime)s - %(levelname)s - %(message)s")

# Function to sanitize filenames (replace invalid characters)
def sanitize_filename(name):
    return re.sub(r'[^\w\s-]', '', name).replace(' ', '_')

# Create output directory
output_dir = "coordinates"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Districts to process
#districts = ["Bengaluru Urban", "Bengaluru Rural"]
districts = [ "Ramanagara", "Kolar" ]
# API endpoints
url_areas = "http://kiadb.karnataka.gov.in/kiadbgisportal/sulb.asmx/GetIndustrialAreaDetailsbyDistrict"
url_coords_template = "https://kgis.ksrsac.in/kgismaps2/rest/services/KIADB/KIADB/MapServer/2/query?f=json&where=industryname%20%3D%27{}%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100"

# Headers for API requests
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "X-Requested-With": "XMLHttpRequest"
}

for district in districts:
    print(f"Processing district: {district}")
    
    # Step 1: Fetch industrial areas for the district
    payload = {"District": district}
    try:
        response = requests.post(url_areas, json=payload, headers=headers)
        response.raise_for_status()
        industrial_areas = [item["nmindar"] for item in json.loads(json.loads(response.text)["d"])]
        logging.info(f"Found {len(industrial_areas)} industrial areas in {district}")
    except requests.RequestException as e:
        logging.error(f"Error fetching industrial areas for {district}: {e}")
        industrial_areas = []
        continue

    # Step 2: Fetch coordinates for each industrial area and create files
    for area in industrial_areas:
        print(f"  Processing {area}...")
        logging.info(f"  Processing {area}...")
        try:
            # Fetch coordinates
            url_coords = url_coords_template.format(area.replace("'", "%27"))  # Escape single quotes
            response = requests.get(url_coords)
            response.raise_for_status()
            data = response.json()

            # Extract coordinates from all rings in all features
            lat_lon = []
            for feature in data.get("features", []):
                for ring in feature.get("geometry", {}).get("rings", []):
                    lat_lon.extend(ring)  # Each ring is a list of [x, y] coordinates

            # Convert coordinates to tuple format
            lat_lon_tuples = [(x, y) for x, y in lat_lon]

            # Create output file in coordinates/ folder
            filename = os.path.join(output_dir, f"{sanitize_filename(district)}-{sanitize_filename(area)}.txt")
            with open(filename, 'w') as f:
                f.write("lat_lon = [\n")
                for i, (x, y) in enumerate(lat_lon_tuples):
                    f.write(f"    ({x}, {y})")
                    if i < len(lat_lon_tuples) - 1:
                        f.write(",")
                    f.write("\n")
                f.write("]")

            logging.info(f"  Created file: {filename} with {len(lat_lon_tuples)} coordinates")

        except requests.RequestException as e:
            logging.error(f"  Error fetching coordinates for {area}: {e}")
        except Exception as e:
            logging.error(f"  Error processing {area}: {e}")

print("Processing complete. Check the coordinates/ folder for generated files.")
