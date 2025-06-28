import pandas as pd
import glob
import json
import os

OUTPUT_DIR = "metadata_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLUMNS = {
    "id": str,
    "title": str,
    "uploader": str,
    "artist": str,
    "tags": list,
    "duration_seconds": int,
    "upload_date": pd.Timestamp,
    "view_count": int,
    "like_count": int,
    "tag_count": int,
    "year_uploaded": pd.Timestamp,
}

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, df.columns.intersection(COLUMNS.keys())]
    # Convert columns to expected types
    df["upload_date"] = pd.to_datetime(df["upload_date"], format="%Y%m%d", errors="coerce")
    df["year_uploaded"] = pd.to_datetime(df["upload_date"].dt.year, format='%Y', errors="coerce")
    
    df = df.dropna(subset=['id', 'title'])  # Remove rows without id or title 
    df['view_count'] = df['view_count'].fillna(0)  # Fill missing counts with 0
    df['like_count'] = df['like_count'].fillna(0)
    df['tag_count'] = df['tag_count'].fillna(0)
    df['duration_seconds'] = df['duration_seconds'].fillna(0)
    df = df.drop_duplicates(subset=['id'], keep='first') # Remove duplicates based on 'id'
    return df

# def validate_columns(df: pd.DataFrame):
#     for col in df:
#         if col not in COLUMNS:
#             raise ValueError(f"Unexpected column: {col}")
#         expected_type = COLUMNS[col]
#         for idx, value in df[col].items():
#             if not isinstance(value, expected_type) and value is not None:
#                 print(f"Invalid type for column '{col}' at index {idx}: expected {expected_type.__name__}, got {type(value).__name__}. Changing to None.")
#                 df.at[idx, col] = None

#TODO currently not working properly 
def validate_columns(df: pd.DataFrame):
    for col in df:
        if col not in COLUMNS:
            raise ValueError(f"Unexpected column: {col}")
        expected_type = COLUMNS[col]
        
        for idx, value in df[col].items():
            if not isinstance(value, expected_type) and value is not None:
                try:
                    converted_value = expected_type(value)
                    # If conversion successful, update the value
                    df.at[idx, col] = converted_value
                    print(f"Successfully converted column '{col}' at index {idx}: {type(value).__name__} -> {expected_type.__name__}")
                    
                except (ValueError, TypeError, OverflowError, pd.errors.OutOfBoundsDatetime) as e:
                    # If conversion fails, set to None and print message
                    print(f"Failed to convert column '{col}' at index {idx}: expected {expected_type.__name__}, got {type(value).__name__} (value: {value}). Conversion failed: {str(e)}. Setting to None.")
                    df.at[idx, col] = None

def extract_metadata(directory: str) -> pd.DataFrame:
    json_files = glob.glob(f"{directory}/*.json")

    all_records = []
    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            records = data if isinstance(data, list) else [data]
            all_records.extend(records)

    df = pd.DataFrame(all_records)
    df = clean_data(df)
    validate_columns(df)
    df.to_csv(f"{OUTPUT_DIR}/combined_metadata.csv", index=False)
    print(f"Metadata extracted and saved to {OUTPUT_DIR}/combined_metadata.csv")
    return df
