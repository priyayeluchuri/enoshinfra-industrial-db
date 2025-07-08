import requests
import json
import pandas as pd
import logging
import os
import ast
import argparse
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from ratelimit import limits, sleep_and_retry
from functools import lru_cache
from xml.etree import ElementTree as ET
import backoff
from tqdm import tqdm

# Set up logging (INFO for debugging)
logging.basicConfig(level=logging.INFO, filename="kiadb_scrape.log", format="%(asctime)s - %(levelname)s - %(message)s")

# Field mapping for CSV
field_mapping = {
    "District Name": {"primary": "dstr", "arcgis": "dstr", "iis": None},
    "Name of the Industrial Area": {"primary": "nmindar", "arcgis": "industrialname", "iis": None},
    "Project Approved By": {"primary": "prjapr", "arcgis": None, "iis": None},
    "Plot Number": {"primary": "plno", "arcgis": "plotno", "iis": None},
    "Plot Category": {"primary": None, "arcgis": None, "iis": "plotcat"},
    "Plot Size": {"primary": "pltar", "arcgis": "plotarea", "iis": "plotsize"},
    "Plot Rate": {"primary": None, "arcgis": None, "iis": "plotrate"},
    "Maintenance Charge": {"primary": None, "arcgis": None, "iis": "mainchare"},
    "Plot Survey No": {"primary": None, "arcgis": None, "iis": "plotsurno"},
    "Reservation": {"primary": "rsvr", "arcgis": None, "iis": None},
    "Area in acres": {"primary": "pltar", "arcgis": "plotarea", "iis": "plotsize"},
    "Plot Status": {"primary": "plst", "arcgis": "indx", "iis": None},
    "Date of Allotment": {"primary": "dtaltm", "arcgis": None, "iis": None},
    "Name of Allottee": {"primary": "nmalt", "arcgis": None, "iis": None},
    "Allottee Phone": {"primary": None, "arcgis": None, "iis": ["ownphone", "phone", "contact_no", "mobile", "telephone", "allottee_phone"]},
    "Allottee Email": {"primary": None, "arcgis": None, "iis": ["ownemail", "email", "email_id", "contact_email", "allottee_email"]},
    "Address of the Allottee": {"primary": "addalt", "arcgis": None, "iis": None},
    "Nature Of Industry": {"primary": "ntrind", "arcgis": None, "iis": None},
    "Due date for payment": {"primary": "ddtpmt", "arcgis": None, "iis": None},
    "Date of Possession": {"primary": "dtpss", "arcgis": None, "iis": None},
    "Date of Lease Agreement Executed": {"primary": "dtleagrex", "arcgis": None, "iis": None},
    "Stipulated time for commencement of production": {"primary": "stcmprd", "arcgis": None, "iis": None},
    "Extension of time Granted": {"primary": "extgrt", "arcgis": None, "iis": None},
    "Implementation Status": {"primary": "implst", "arcgis": None, "iis": None},
    "Notice under 34-B issued": {"primary": "n34b", "arcgis": None, "iis": None},
    "Remarks": {"primary": "remark", "arcgis": None, "iis": None},
    "ULPIN": {"primary": "ulpin", "arcgis": "ulpin", "iis": None},
    "Preapproved Clearances Permissions": {"primary": "preappoved_clearances_permissions", "arcgis": None, "iis": None},
    "Preapproved Clearance Details": {"primary": "preappoved_clearances_permissions_details", "arcgis": None, "iis": None},
    "Water Availability": {"primary": "water_availability", "arcgis": None, "iis": None},
    "Electric Power Availability": {"primary": "electricpoweravailability", "arcgis": None, "iis": None},
    "Gaspipeline Connectivity": {"primary": "gaspipeline_connectivity", "arcgis": None, "iis": None},
    "OFC": {"primary": "ofc", "arcgis": None, "iis": None},
    "STP": {"primary": "stp", "arcgis": None, "iis": None},
    "WTP": {"primary": "wtp", "arcgis": None, "iis": None},
    "ETP": {"primary": "etp", "arcgis": None, "iis": None}
}

# Headers
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,zh-CN;q=0.7,en;q=0.3",
    "Content-Type": "application/json; charset=utf-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "http://kiadb.karnataka.gov.in",
    "Connection": "keep-alive",
    "Referer": "http://kiadb.karnataka.gov.in/kiadbgisportal/",
}

# SOAP template
soap_template = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <{method} xmlns="http://kiadb.karnataka.gov.in/kiadbgisportal/sulb">
            <plcd>{plcd}</plcd>
        </{method}>
    </soap:Body>
</soap:Envelope>"""

# Rate limiting: 3 calls per second
CALLS = 3
PERIOD = 1

# Cache for invalid pltcode responses
invalid_pltcode_cache = set()

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def make_request(session, url, method="POST", json=None, data=None, headers=None):
    try:
        response = session.request(method, url, json=json, data=data, headers=headers, timeout=5)
        response.raise_for_status()
        logging.info(f"Request succeeded for {url}")
        return response
    except requests.RequestException as e:
        logging.error(f"Request failed for {url}: {str(e)}")
        return None

@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=3)
def soap_fallback(session, url, method, plcd):
    soap_data = soap_template.format(method=method, plcd=plcd)
    soap_headers = headers.copy()
    soap_headers["Content-Type"] = "text/xml; charset=utf-8"
    soap_headers["SOAPAction"] = f"http://kiadb.karnataka.gov.in/kiadbgisportal/sulb/{method}"
    response = make_request(session, url, data=soap_data, headers=soap_headers)
    if response:
        root = ET.fromstring(response.text)
        result = root.find(f".//{method}Result").text
        logging.info(f"SOAP fallback succeeded for {method} with plcd {plcd}")
        return json.loads(result) if result else []
    logging.error(f"SOAP fallback failed for {method} with plcd {plcd}")
    return []

@lru_cache(maxsize=100)
def query_arcgis(session, geometry_json):
    arcgis_base = "https://kgis.ksrsac.in/kgismaps2/rest/services/KIADB/KIADB/MapServer/1/query"
    query_params = {
        "f": "json",
        "returnGeometry": "true",
        "spatialRel": "esriSpatialRelIntersects",
        "geometry": geometry_json,
        "geometryType": "esriGeometryEnvelope",
        "inSR": 102100,
        "outFields": "*",
        "outSR": 102100,
    }
    arcgis_url = f"{arcgis_base}?{urlencode(query_params)}"
    response = make_request(session, arcgis_url, method="GET")
    return response.json() if response else {}

def process_plotcode(session, plotcode, district_space, industrial_area_space, coord_file):
    if plotcode in invalid_pltcode_cache:
        logging.info(f"Skipping cached invalid plotcode {plotcode}")
        return None

    # Initialize row with all field_mapping keys
    row = {"Plotcode": plotcode}
    for csv_field in field_mapping:
        row[csv_field] = "N/A"
    
    # getdeatilsforidentifier
    details_url = "http://kiadb.karnataka.gov.in/kiadbgisportal/sulb.asmx/getdeatilsforidentifier"
    response = make_request(session, details_url, json={"plcd": plotcode})
    if not response:
        details = soap_fallback(session, details_url, "getdeatilsforidentifier", plotcode)
        details = details[0] if details else {}
    else:
        details = response.json()
        if details.get("d") == "\"Wrong Input\"" or not details.get("d"):
            invalid_pltcode_cache.add(plotcode)
            logging.info(f"Invalid response for plotcode {plotcode}: {details.get('d')}")
            return None
        try:
            details = json.loads(details.get("d", "[]"))
            details = details[0] if isinstance(details, list) and details else {}
        except json.JSONDecodeError:
            logging.error(f"JSON decode error for plotcode {plotcode}: {details.get('d')}")
            return None

    if details.get("plst") != "Allotted":
        logging.info(f"Skipping non-allotted plotcode {plotcode} with plst: {details.get('plst')}")
        return None

    # Map fields from getdeatilsforidentifier
    for csv_field, field_info in field_mapping.items():
        primary_field = field_info["primary"]
        if primary_field:
            if isinstance(primary_field, list):
                for field in primary_field:
                    if field in details:
                        row[csv_field] = details[field]
                        logging.info(f"Mapped {csv_field} from getdeatilsforidentifier: {row[csv_field]} for {plotcode}")
                        break
            else:
                if primary_field in details:
                    row[csv_field] = details.get(primary_field)
                    logging.info(f"Mapped {csv_field} from getdeatilsforidentifier: {row[csv_field]} for {plotcode}")

    # getplotiisdetails
    iis_url = "http://kiadb.karnataka.gov.in/kiadbgisportal/sulb.asmx/getplotiisdetails"
    response = make_request(session, iis_url, json={"plcd": plotcode})
    if not response:
        iis_details = soap_fallback(session, iis_url, "getplotiisdetails", plotcode)
    else:
        iis_details = response.json()
        iis_details = json.loads(iis_details.get("d", "[]")) if iis_details.get("d") else []

    if iis_details and isinstance(iis_details, list) and iis_details:
        iis_details = iis_details[0]
        for csv_field, field_info in field_mapping.items():
            iis_field = field_info["iis"]
            if iis_field and (row[csv_field] == "N/A"):
                if isinstance(iis_field, list):
                    for field in iis_field:
                        if field in iis_details:
                            row[csv_field] = iis_details[field]
                            logging.info(f"Mapped {csv_field} from getplotiisdetails: {row[csv_field]} for {plotcode}")
                            break
                else:
                    if iis_field in iis_details:
                        row[csv_field] = iis_details.get(iis_field)
                        logging.info(f"Mapped {csv_field} from getplotiisdetails: {row[csv_field]} for {plotcode}")

    return row

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Debuggable optimized KIADB plot data scraper")
parser.add_argument("--coord_file", help="Path to a single coordinate file")
args = parser.parse_args()

# Determine coordinate files
input_dir = "coordinates"
if args.coord_file:
    coord_file = args.coord_file
    if not os.path.exists(coord_file):
        print(f"Error: Coordinate file {coord_file} does not exist")
        logging.error(f"Coordinate file {coord_file} does not exist")
        exit()
    coordinate_files = [coord_file]
else:
    if not os.path.exists(input_dir):
        print(f"Error: Coordinates directory {input_dir} does not exist")
        logging.error(f"Coordinates directory {input_dir} does not exist")
        exit()
    coordinate_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".txt")]
    if not coordinate_files:
        print(f"No coordinate files found in {input_dir}/")
        logging.error(f"No coordinate files found in {input_dir}/")
        exit()

# Create output directory
output_dir = "kiadb_data"
os.makedirs(output_dir, exist_ok=True)

# ArcGIS API base URL
arcgis_base = "https://kgis.ksrsac.in/kgismaps2/rest/services/KIADB/KIADB/MapServer/1/query"

# Initialize session
session = requests.Session()
session.headers.update(headers)
session.get("http://kiadb.karnataka.gov.in/kiadbgisportal/")

# Initialize summary data
summary_data = []

# Process each coordinate file
for coord_file in coordinate_files:
    filename = os.path.splitext(os.path.basename(coord_file))[0]
    try:
        district, industrial_area = filename.split("-", 1)
        district_space = district.replace("_", " ")
        industrial_area_space = industrial_area.replace("_", " ")
        district_underscore = district
        industrial_area_underscore = industrial_area
    except ValueError:
        print(f"Error: Filename {filename} must be in format <district>-<industrial_area>.txt")
        logging.error(f"Filename {filename} must be in format <district>-<industrial_area>.txt")
        continue

    logging.info(f"Processing coordinate file: {coord_file}")
    print(f"Processing {coord_file}...")

    # Read coordinates
    try:
        with open(coord_file, "r") as f:
            lat_lon = ast.literal_eval(f.read().split("=", 1)[1].strip())
    except Exception as e:
        print(f"Error reading {coord_file}: {str(e)}")
        logging.error(f"Error reading {coord_file}: {str(e)}")
        continue

    coordinates = [{"x": x, "y": y} for x, y in lat_lon]
    logging.info(f"Loaded {len(coordinates)} coordinates from {coord_file}")

    # Batch coordinates for ArcGIS queries
    extracted_data = []
    processed_plotcodes = set()
    pltcode_bases = set()
    known_suffixes = defaultdict(set)

    def batch_coordinates(coords, batch_size=5):
        for i in range(0, len(coords), batch_size):
            batch = coords[i:i + batch_size]
            min_x = min(c["x"] for c in batch) - 100
            max_x = max(c["x"] for c in batch) + 100
            min_y = min(c["y"] for c in batch) - 100
            max_y = max(c["y"] for c in batch) + 100
            yield {"xmin": min_x, "ymin": min_y, "xmax": max_x, "ymax": max_y}

    # Process ArcGIS queries
    for batch in tqdm(batch_coordinates(coordinates), total=(len(coordinates) + 4) // 5, desc="ArcGIS Queries"):
        arcgis_data = query_arcgis(session, json.dumps({**batch, "spatialReference": {"wkid": 102100}}))
        features = arcgis_data.get("features", [])
        if not features:
            logging.warning(f"No features found for batch {batch} in {coord_file}")
            continue

        plotcodes = []
        for feature in features:
            plotcode = feature.get("attributes", {}).get("plotcode")
            if not plotcode or plotcode in processed_plotcodes:
                continue
            processed_plotcodes.add(plotcode)
            plotcodes.append(plotcode)

            if len(plotcode) == 14 and plotcode[12:].isdigit():
                pltcode_base = plotcode[:12]
                suffix = int(plotcode[12:])
                pltcode_bases.add(pltcode_base)
                known_suffixes[pltcode_base].add(suffix)
                logging.info(f"Added pltcode_base: {pltcode_base} with suffix {suffix:02d} from {plotcode}")
            else:
                logging.warning(f"Invalid plotcode format: {plotcode} (length: {len(plotcode)})")

        # Process plotcodes in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_plotcode = {
                executor.submit(process_plotcode, session, pc, district_space, industrial_area_space, coord_file): pc
                for pc in plotcodes
            }
            for future in tqdm(as_completed(future_to_plotcode), total=len(plotcodes), desc="Processing Plotcodes"):
                row = future.result()
                if row:
                    extracted_data.append(row)
                    logging.info(f"Processed plotcode {row['Plotcode']} with plno {row.get('Plot Number', 'N/A')}")

    # Getplotdetailsbystatus
    status_url = "http://kiadb.karnataka.gov.in/kiadbgisportal/sulb.asmx/Getplotdetailsbystatus"
    expected_plnos = set()
    total_plots = 0

    for dstr, nmindar in [(district_space, industrial_area_space), (district_underscore, industrial_area_underscore)]:
        response = make_request(session, status_url, json={"dstr": dstr, "nmindar": nmindar, "indx": "Allotted"})
        if response and response.json().get("d") != "\"Wrong Input\"":
            status_plots = json.loads(response.json().get("d", "[]"))
            expected_plnos = set(plot.get("plno") for plot in status_plots if plot.get("plno"))
            total_plots = len(expected_plnos)
            logging.info(f"Getplotdetailsbystatus found {total_plots} plots for {dstr}, {nmindar}")
            break
        else:
            logging.warning(f"Getplotdetailsbystatus failed for {dstr}, {nmindar}")

    # Identify missing plots
    extracted_plnos = set(row.get("Plot Number", "N/A") for row in extracted_data if row.get("Plot Number") != "N/A")
    missing_plnos = expected_plnos - extracted_plnos
    logging.info(f"Missing plnos: {missing_plnos}")

    # Fallback iteration for missing plots
    if missing_plnos:
        print(f"Found {len(missing_plnos)} missing plots: {missing_plnos}")
        logging.info(f"Found {len(missing_plnos)} missing plots: {missing_plnos}")
        if not pltcode_bases:
            pltcode_bases.add("Z06572016300")
            logging.warning(f"No pltcode_bases found. Using default: Z06572016300")
        pltcodes = []
        seen_suffixes = defaultdict(set)

        for pltcode_base in pltcode_bases:
            for suffix in sorted(known_suffixes[pltcode_base]):
                for i in range(max(0, suffix - 5), min(100, suffix + 6)):
                    if i not in seen_suffixes[pltcode_base]:
                        pltcode = f"{pltcode_base}{i:02d}"
                        if len(pltcode) == 14 and pltcode not in processed_plotcodes:
                            pltcodes.append(pltcode)
                            seen_suffixes[pltcode_base].add(i)
            for i in range(100):
                if i not in seen_suffixes[pltcode_base]:
                    pltcode = f"{pltcode_base}{i:02d}"
                    if len(pltcode) == 14 and pltcode not in processed_plotcodes:
                        pltcodes.append(pltcode)
                        seen_suffixes[pltcode_base].add(i)

        logging.info(f"Generated {len(pltcodes)} pltcodes for fallback iteration")
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_plotcode = {
                executor.submit(process_plotcode, session, pc, district_space, industrial_area_space, coord_file): pc
                for pc in pltcodes
            }
            for future in tqdm(as_completed(future_to_plotcode), total=len(pltcodes), desc="Fallback Plotcodes"):
                row = future.result()
                if row and row.get("Plot Number") in missing_plnos:
                    extracted_data.append(row)
                    processed_plotcodes.add(row["Plotcode"])
                    extracted_plnos.add(row["Plot Number"])
                    missing_plnos = expected_plnos - extracted_plnos
                    logging.info(f"Found missing plot {row['Plot Number']} with pltcode {row['Plotcode']}")
                    if not missing_plnos:
                        logging.info("All missing plots found. Stopping fallback iteration.")
                        break

    # Save data
    total_plots = len(expected_plnos)
    missed_plots = len(missing_plnos)
    if extracted_data:
        df = pd.DataFrame(extracted_data).drop_duplicates(subset=["Plotcode"])
        plots_with_contact = len(df[
            (df["Allottee Phone"].notna() & (df["Allottee Phone"] != "N/A") & (df["Allottee Phone"] != "")) |
            (df["Allottee Email"].notna() & (df["Allottee Email"] != "N/A") & (df["Allottee Email"] != ""))
        ])
        output_file = os.path.join(output_dir, f"{filename}.csv")
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file} with {len(df)} records")
        logging.info(f"Data saved to {output_file} with {len(df)} records")
        summary_data.append({
            "District": district_space,
            "Area": industrial_area_space,
            "Total Plots": total_plots,
            "Missed Plots": missed_plots,
            "Plots with Phone or Email": plots_with_contact
        })
    else:
        print(f"No data extracted for {coord_file}")
        logging.warning(f"No data extracted for {coord_file}")
        summary_data.append({
            "District": district_space,
            "Area": industrial_area_space,
            "Total Plots": total_plots,
            "Missed Plots": missed_plots,
            "Plots with Phone or Email": 0
        })

# Save summary
if summary_data:
    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(output_dir, "summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"Summary saved to {summary_file} with {len(summary_df)} records")
    logging.info(f"Summary saved to {summary_file} with {len(summary_df)} records")
else:
    print("No summary data to save")
    logging.warning("No summary data to save")

print(f"Processing complete. Check {output_dir} for CSV files and kiadb_scrape.log for details.")
