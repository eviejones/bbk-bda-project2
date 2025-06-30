import pandas as pd
import glob
import json
import os
import logging

OUTPUT_DIR = "metadata_output"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)


logger = logging.getLogger("Data Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding="utf-8", level=logging.DEBUG)

COLUMNS = {
    "id": str,
    "title": str,
    "uploader": str,
    "artist": str,
    "tags": list,
    "duration_seconds": int,
    "view_count": int,
    "like_count": int,
    "tag_count": int,
    "upload_date": pd.Timestamp,  # Use pandas Timestamp for date handling
    "year_uploaded": int,
}


def log_and_print(message: str, level: str = "info"):
    """Log message and print to console simultaneously. The name of the logger is set above.

    Args:
        message (str): The message to log and print.
        level (str): The logging level (default is "info").
    """
    print(message)
    getattr(logger, level.lower())(message)


def shorten_title(title: str) -> str:
    """Shorten the title to a maximum of 5 words for visualisation.

    Args:
        title (str): The full YouTube video title to be shortened.

    Returns:
        str: The shortened title, containing at most 5 words.

    Example:
        shorten_title("This is a very long title that exceeds five words")
        # Returns: "This is a very long title"
    """
    if pd.isna(title):  # Handle potential NaN values
        return title
    words = title.split()
    if len(words) > 5:
        return " ".join(words[:5])
    return title  # Return the original title if it's 5 words or less


def validate_dataframe_schema(df: pd.DataFrame) -> bool:
    """
    Validate DataFrame columns and types against expected schema.

    Returns:
        bool: True if valid, False if any issues found.
    """
    log_and_print("Starting DataFrame validation...")
    is_valid = True

    # Check for missing columns
    missing_cols = set(COLUMNS.keys()) - set(df.columns)
    if missing_cols:
        log_and_print(f"Missing columns: {missing_cols}", "error")
        is_valid = False

    # Check column types for existing columns
    for col_name, expected_type in COLUMNS.items():
        if col_name in df.columns:
            actual_dtype = df[col_name].dtype
            if expected_type is str and not pd.api.types.is_object_dtype(actual_dtype):
                log_and_print(
                    f"Column '{col_name}' should be string, but is {actual_dtype}",
                    "error",
                )
                is_valid = False
            elif expected_type is int and not pd.api.types.is_integer_dtype(
                actual_dtype
            ):
                log_and_print(
                    f"Column '{col_name}' should be integer, but is {actual_dtype}",
                    "error",
                )
                is_valid = False
            elif (
                expected_type is list
                and len(df[col_name].dropna()) > 0
                and not isinstance(df[col_name].dropna().iloc[0], list)
            ):
                log_and_print(f"Column '{col_name}' should contain lists", "error")
                is_valid = False
            elif (
                expected_type == pd.Timestamp
                and not pd.api.types.is_datetime64_any_dtype(actual_dtype)
            ):
                log_and_print(
                    f"Column '{col_name}' should be datetime, but is {actual_dtype}",
                    "error",
                )
                is_valid = False

    log_and_print("Validation complete.")
    return is_valid


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by selecting relevant columns, converting types, and handling missing values.

    Args:
        df (pd.DataFrame): The DataFrame containing metadata to be cleaned.

    Returns:
        pd.DataFrame: The cleaned DataFrame with relevant columns and types.
    """
    log_and_print("Cleaning DataFrame...")
    # Select only the columns defined in COLUMNS
    df = df.loc[:, df.columns.intersection(COLUMNS.keys())]

    # Convert date column to pandas Timestamp
    try:
        df["upload_date"] = pd.to_datetime(
            df["upload_date"], format="%Y%m%d", errors="coerce"
        )
        log_and_print("Converted 'upload_date' to pandas Timestamp.")
    except Exception as e:
        log_and_print(f"Error converting 'upload_date': {e}", "error")

    is_valid = validate_dataframe_schema(
        df
    )  # Validate schema before further processing
    # Drop rows with missing data, fill in missing counts and drop duplicates
    log_and_print(
        "Dropping rows with missing 'id' or 'title' and filling missing counts..."
    )
    log_and_print("Filling missing counts with 0...")
    df = df.dropna(subset=["id", "title"])  # Remove rows without id or title
    df["view_count"] = df["view_count"].fillna(0)
    df["like_count"] = df["like_count"].fillna(0)
    df["tag_count"] = df["tag_count"].fillna(0)
    df["duration_seconds"] = df["duration_seconds"].fillna(0)
    df = df.drop_duplicates(
        subset=["id"], keep="first"
    )  # Remove duplicates based on 'id'

    # Split titles for legible title and shorten them. Isn't perfect but looks better on charts
    try:
        split_titles = df["title"].str.split(r"[-|:]", n=1, expand=True)
        df["legible_title"] = split_titles[0].str.strip()
        df["legible_title"] = df["legible_title"].apply(shorten_title)
    except Exception as e:
        log_and_print(f"Error processing 'legible_title': {e}", "error")
    return df, is_valid


def combine_metadata(directory: str) -> None:
    """Takes a directory containing JSON files and extracts metadata into a combined DataFrame."""
    json_files = glob.glob(f"{directory}/*.json")

    all_records = []
    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            records = data if isinstance(data, list) else [data]
            all_records.extend(records)

    df = pd.DataFrame(all_records)
    df, is_valid = clean_data(df)

    if is_valid:
        log_and_print("✅ DataFrame validation passed")
    else:
        log_and_print("❌ DataFrame validation failed - check logs", "error")

    df.to_csv(f"{OUTPUT_DIR}/combined_metadata.csv", index=False)
    log_and_print(
        f"✅ Metadata extracted and saved to {OUTPUT_DIR}/combined_metadata.csv"
    )
