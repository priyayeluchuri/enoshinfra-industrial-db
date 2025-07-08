import pandas as pd
import re
import os
import logging
import argparse
from supabase import create_client, Client
from dotenv import load_dotenv

# Set up argument parser
parser = argparse.ArgumentParser(description="Process KIADB CSV files and update Supabase database")
parser.add_argument("--update", action="store_true", help="Process only new CSV files (without corresponding updated_ files) and update tables instead of clearing")
args = parser.parse_args()

# Set up logging
logging.basicConfig(
    filename="processing_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env.local
load_dotenv(".env.local")
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase = create_client(supabase_url, supabase_key)

# Validation and cleaning functions
def is_valid_phone(phone):
    if pd.isna(phone) or phone == "N/A" or not phone:
        return False
    pattern = r"^(\+91)?[6-9][0-9]{9}$"
    return bool(re.match(pattern, str(phone)))

def is_valid_email(email):
    if pd.isna(email) or email == "N/A" or not email:
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, str(email)))

def clean_address(address):
    if pd.isna(address) or address == "N/A" or not address:
        return None
    # Remove line feeds, carriage returns, and normalize whitespace
    cleaned = re.sub(r'[\n\r]+', ' ', str(address)).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned

def truncate_nature_of_industry(industry):
    if pd.isna(industry) or industry == "N/A" or not industry:
        return None
    # Truncate to 300 characters to avoid length errors
    return str(industry)[:300]

# Directory containing CSVs
csv_dir = "kiadb_data"
# Get all CSV files excluding summary.csv and updated_ files
all_csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv") and f != "summary.csv" and not f.startswith("updated_")]

# Filter files based on --update flag
if args.update:
    csv_files = [f for f in all_csv_files if not os.path.exists(os.path.join(csv_dir, f"updated_{f}"))]
    print(f"Processing new files only: {csv_files}")
else:
    csv_files = all_csv_files
    print(f"Processing all files: {csv_files}")

# Dictionary to store merged data by Plotcode
plot_data = {}

# Process each CSV with error handling
for csv_file in csv_files:
    try:
        # Remove corresponding updated_ file if it exists
        updated_file = os.path.join(csv_dir, f"updated_{csv_file}")
        if os.path.exists(updated_file):
            os.remove(updated_file)
            print(f"Removed existing file: {updated_file}")

        df = pd.read_csv(os.path.join(csv_dir, csv_file))
        df["validation_status"] = "both_invalid"

        for index, row in df.iterrows():
            plotcode = str(row["Plotcode"])
            phone = str(row["Allottee Phone"]) if pd.notna(row["Allottee Phone"]) else "N/A"
            email = str(row["Allottee Email"]) if pd.notna(row["Allottee Email"]) else "N/A"
            address = clean_address(row["Address of the Allottee"])
            industry = truncate_nature_of_industry(row["Nature Of Industry"])
            phone_valid = is_valid_phone(phone)
            email_valid = is_valid_email(email)

            # Set validation status
            if phone_valid and email_valid:
                df.at[index, "validation_status"] = "valid"
            elif phone_valid:
                df.at[index, "validation_status"] = "invalid_email"
            elif email_valid:
                df.at[index, "validation_status"] = "invalid_phone"

            # Initialize or update plot_data (for valid contacts only)
            if phone_valid or email_valid:
                if plotcode not in plot_data:
                    plot_data[plotcode] = {
                        "district_name": set(),
                        "industrial_area": set(),
                        "plot_number": str(row["Plot Number"]) if pd.notna(row["Plot Number"]) else None,
                        "area_acres": float(row["Area in acres"]) if pd.notna(row["Area in acres"]) else None,
                        "allottee_name": str(row["Name of Allottee"]) if pd.notna(row["Name of Allottee"]) else None,
                        "allottee_phone": None,
                        "allottee_email": None,
                        "address": address,
                        "nature_of_industry": industry,
                        "ulpin": str(row["ULPIN"]) if pd.notna(row["ULPIN"]) else None,
                        "plot_status": str(row["Plot Status"]) if pd.notna(row["Plot Status"]) else None,
                        "phone_valid": False,
                        "email_valid": False
                    }

                # Update district and industrial area
                if pd.notna(row["District Name"]):
                    plot_data[plotcode]["district_name"].add(str(row["District Name"]))
                if pd.notna(row["Name of the Industrial Area"]):
                    plot_data[plotcode]["industrial_area"].add(str(row["Name of the Industrial Area"]))

                # Update contact details if valid
                if phone_valid and not plot_data[plotcode]["phone_valid"]:
                    plot_data[plotcode]["allottee_phone"] = phone
                    plot_data[plotcode]["phone_valid"] = True
                if email_valid and not plot_data[plotcode]["email_valid"]:
                    plot_data[plotcode]["allottee_email"] = email
                    plot_data[plotcode]["email_valid"] = True
                if address and not plot_data[plotcode]["address"]:
                    plot_data[plotcode]["address"] = address
                if industry and not plot_data[plotcode]["nature_of_industry"]:
                    plot_data[plotcode]["nature_of_industry"] = industry

        # Save updated CSV
        df.to_csv(updated_file, index=False)
        print(f"Created updated CSV: {updated_file}")

    except Exception as e:
        logging.error(f"Error processing {csv_file}: {e}")
        print(f"Error processing {csv_file}. Check processing_errors.log for details. Continuing with next file.")
        continue

# Clear Supabase tables only if --update is not used
if not args.update:
    try:
        supabase.table("ai_agent_data").delete().neq("id", 0).execute()
        supabase.table("kiadb_property_owners").delete().neq("id", 0).execute()
        print("Cleared Supabase tables")
    except Exception as e:
        logging.error(f"Error truncating Supabase tables: {e}")
        print(f"Error truncating Supabase tables. Check processing_errors.log for details.")
        exit(1)

# Populate or update Supabase tables
for plotcode, data in plot_data.items():
    # Convert sets to lists for PostgreSQL arrays
    data["district_name"] = list(data["district_name"])
    data["industrial_area"] = list(data["industrial_area"])

    # Upsert into kiadb_property_owners
    try:
        supabase.table("kiadb_property_owners").upsert({
            "plotcode": plotcode,
            "district_name": data["district_name"],
            "industrial_area": data["industrial_area"],
            "plot_number": data["plot_number"],
            "area_acres": data["area_acres"],
            "allottee_name": data["allottee_name"],
            "allottee_phone": data["allottee_phone"],
            "allottee_email": data["allottee_email"],
            "address": data["address"],
            "nature_of_industry": data["nature_of_industry"],
            "ulpin": data["ulpin"],
            "plot_status": data["plot_status"],
            "phone_valid": data["phone_valid"],
            "email_valid": data["email_valid"]
        }, on_conflict="plotcode").execute()

        # Check if plotcode exists in ai_agent_data
        existing = supabase.table("ai_agent_data").select("plotcode").eq("plotcode", plotcode).execute()
        if not existing.data:  # Insert only if plotcode doesn't exist
            supabase.table("ai_agent_data").insert({
                "plotcode": plotcode
            }).execute()

    except Exception as e:
        logging.error(f"Error processing plotcode {plotcode}: {e}")
        print(f"Error processing plotcode {plotcode}. Check processing_errors.log for details.")
        continue

print("Supabase population complete.")
