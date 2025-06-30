import pandas as pd
import glob
import json
import os
import logging

OUTPUT_DIR = "metadata_output"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logger = logging.getLogger("Data Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)

def log_and_print(message: str, level: str = "info"):
    """Log message and print to console simultaneously."""
    print(message)
    getattr(logger, level.lower())(message)

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
    "legible_title": str, # Added for shortened title
}

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
    if pd.isna(title): # Handle potential NaN values
        return title
    words = title.split()
    if len(words) > 5:
        return ' '.join(words[:5])
    return title # Return the original title if it's 5 words or less

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
        df["upload_date"] = pd.to_datetime(df["upload_date"], format="%Y%m%d", errors="coerce")
        log_and_print("Converted 'upload_date' to pandas Timestamp.")
    except Exception as e:
        log_and_print(f"Error converting 'upload_date': {e}", "error")

    # Drop rows with missing data, fill in missing counts and drop duplicates
    log_and_print("Dropping rows with missing 'id' or 'title' and filling missing counts...")
    log_and_print("Filling missing counts with 0...")
    df = df.dropna(subset=['id', 'title'])  # Remove rows without id or title 
    df['view_count'] = df['view_count'].fillna(0)
    df['like_count'] = df['like_count'].fillna(0)
    df['tag_count'] = df['tag_count'].fillna(0)
    df['duration_seconds'] = df['duration_seconds'].fillna(0)
    df = df.drop_duplicates(subset=['id'], keep='first') # Remove duplicates based on 'id'
    
    # Split titles for legible title and shorten them. Isn't perfect but looks better on charts
    split_titles = df['title'].str.split(r'[-|:]', n=1, expand=True) 
    df['legible_title'] = split_titles[0].str.strip()
    df['legible_title'] = df['legible_title'].apply(shorten_title)
    return df

def validate_dataframe_schema(df: pd.DataFrame) -> bool:
    """
    Validate DataFrame columns and types against expected schema.
    
    Returns:
        bool: True if valid, False if any issues found
    """
    is_valid = True
    
    # Check for missing columns
    missing_cols = set(COLUMNS.keys()) - set(df.columns)
    if missing_cols:
        log_and_print(f"Missing columns: {missing_cols}", "error")
        is_valid = False
    
    # Check column types for existing columns
    for col_name, expected_type in COLUMNS.items():
        if col_name in df.columns:
            if expected_type is str and not pd.api.types.is_object_dtype(df[col_name].dtype):
                log_and_print(f"Column '{col_name}' should be string", "error")
                is_valid = False
            elif expected_type is int and not pd.api.types.is_integer_dtype(df[col_name].dtype):
                log_and_print(f"Column '{col_name}' should be integer", "error")
                is_valid = False
            elif expected_type is list and len(df[col_name].dropna()) > 0 and not isinstance(df[col_name].dropna().iloc[0], list):
                log_and_print(f"Column '{col_name}' should contain lists", "error")
                is_valid = False
            elif expected_type == pd.Timestamp and not pd.api.types.is_datetime64_any_dtype(df[col_name].dtype):
                log_and_print(f"Column '{col_name}' should be datetime", "error")
                is_valid = False
    
    return is_valid

# def validate_dataframe_schema(df: pd.DataFrame):
#     """
#     Validate DataFrame columns and types against expected schema.
    
#     Args:
#         df: DataFrame to validate.
    
#     Returns:
#         Dictionary with validation results.
#     """
#     results = {
#         'is_valid': True,
#         'missing_columns': [],
#         'extra_columns': [],
#         'type_mismatches': [],
#         'details': []
#     }
    
#     # Check for missing columns
#     expected_cols = set(COLUMNS.keys())
#     actual_cols = set(df.columns)
    
#     missing_cols = expected_cols - actual_cols
#     extra_cols = actual_cols - expected_cols
    
#     if missing_cols:
#         results['missing_columns'] = list(missing_cols)
#         results['is_valid'] = False
#         results['details'].append(f"Missing columns: {missing_cols}")
    
#     if extra_cols:
#         results['extra_columns'] = list(extra_cols)
#         results['details'].append(f"Extra columns: {extra_cols}")
    
#     # Check column types for existing columns
#     for col_name, expected_type in COLUMNS.items():
#         if col_name in df.columns:
#             actual_dtype = df[col_name].dtype
            
#             # Handle different type checking scenarios
#             if expected_type is str:
#                 if not pd.api.types.is_object_dtype(actual_dtype):
#                     results['type_mismatches'].append({
#                         'column': col_name,
#                         'expected': 'string/object',
#                         'actual': str(actual_dtype)
#                     })
#                     results['is_valid'] = False
            
#             elif expected_type is int:
#                 if not pd.api.types.is_integer_dtype(actual_dtype):
#                     results['type_mismatches'].append({
#                         'column': col_name,
#                         'expected': 'integer',
#                         'actual': str(actual_dtype)
#                     })
#                     results['is_valid'] = False
            
#             elif expected_type is list:
#                 if len(df[col_name].dropna()) > 0 and not isinstance(df[col_name].dropna().iloc[0], list):
#                     results['is_valid'] = False
#             elif expected_type == pd.Timestamp:
#                 if not pd.api.types.is_datetime64_any_dtype(actual_dtype):
#                     results['type_mismatches'].append({
#                         'column': col_name,
#                         'expected': 'datetime/timestamp',
#                         'actual': str(actual_dtype)
#                     })
#                     results['is_valid'] = False
    
#     return results

def validate_and_report(df: pd.DataFrame) -> bool:
    """Validate DataFrame and optionally print detailed report.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        bool: True if validation passed, False otherwise
    """
    results = validate_dataframe_schema(df)
    
    # Always log the overall result
    if results['is_valid']:
        log_and_print("✅ DataFrame validation passed")
    else:
        log_and_print("❌ DataFrame validation failed", "error")
    
    # Only log/print details if there are issues OR if verbose mode
    if not results['is_valid']:
        log_and_print("=== DataFrame Validation Report ===")
        if results['missing_columns']:
            log_and_print(f"Missing columns: {results['missing_columns']}", "error")
        
        if results['extra_columns']:
            log_and_print(f"Extra columns: {results['extra_columns']}", "warning")
        
        if results['type_mismatches']:
            log_and_print("Type mismatches:", "error")
            for mismatch in results['type_mismatches']:
                log_and_print(f"  - {mismatch['column']}: expected {mismatch['expected']}, got {mismatch['actual']}", "error")

def combine_metadata(directory: str) -> pd.DataFrame:
    """Takes a directory containing JSON files and extracts metadata into a combined DataFrame.
    
    Args:
        directory (str): The directory containing JSON files with metadata.
    
    Returns:
        pd.DataFrame: A DataFrame containing combined metadata from all JSON files.
    """
    json_files = glob.glob(f"{directory}/*.json")

    all_records = []
    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            records = data if isinstance(data, list) else [data]
            all_records.extend(records)

    df = pd.DataFrame(all_records)
    df = clean_data(df)
    validate_dataframe_schema(df)
    df.to_csv(f"{OUTPUT_DIR}/combined_metadata.csv", index=False)
    log_and_print(f"✅ Metadata extracted and saved to {OUTPUT_DIR}/combined_metadata.csv")
    return df


# #TODO currently not working properly 
# def validate_columns(df: pd.DataFrame):
#     for col in df:
#         if col not in COLUMNS:
#             raise ValueError(f"Unexpected column: {col}")
#         expected_type = COLUMNS[col]
        
#         for idx, value in df[col].items():
#             if not isinstance(value, expected_type) and value is not None:
#                 try:
#                     converted_value = expected_type(value)
#                     # If conversion successful, update the value
#                     df.at[idx, col] = converted_value
#                     print(f"Successfully converted column '{col}' at index {idx}: {type(value).__name__} -> {expected_type.__name__}")
                    
#                 except (ValueError, TypeError, OverflowError, pd.errors.OutOfBoundsDatetime) as e:
#                     # If conversion fails, set to None and print message
#                     print(f"Failed to convert column '{col}' at index {idx}: expected {expected_type.__name__}, got {type(value).__name__} (value: {value}). Conversion failed: {str(e)}. Setting to None.")
#                     df.at[idx, col] = None



# def validate_columns(df: pd.DataFrame):
#     for col in df:
#         if col not in COLUMNS:
#             raise ValueError(f"Unexpected column: {col}")
#         expected_type = COLUMNS[col]
#         for idx, value in df[col].items():
#             if not isinstance(value, expected_type) and value is not None:
#                 print(f"Invalid type for column '{col}' at index {idx}: expected {expected_type.__name__}, got {type(value).__name__}. Changing to None.")
#                 df.at[idx, col] = None

# def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Validates DataFrame columns against expected types, attempting vectorized conversions.
#     Unconvertible values are set to None/NaN/NaT.

#     Args:
#         df (pd.DataFrame): The input DataFrame.

#     Returns:
#         pd.DataFrame: The DataFrame with validated and potentially converted columns.

#     Raises:
#         ValueError: If an unexpected column is found in the DataFrame.
#     """
#     df_copy = df.copy()

#     for col in df_copy.columns:
#         if col not in COLUMNS:
#             raise ValueError(f"Unexpected column: '{col}' found in DataFrame.")

#     for col, expected_type in COLUMNS.items():
#         print(f"Processing column '{col}' (expected: {expected_type.__name__ if isinstance(expected_type, type) else expected_type})...")

#         try:
#             if expected_type in (int, float):
#                 df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
#                 if expected_type is int:
#                     # Convert to nullable Int64 to handle NaNs from coercion
#                     df_copy[col] = df_copy[col].astype('Int64')
#                 print(f"Non-numeric values found in {col}, converted to numeric type.")

#             elif expected_type is str:
#                 df_copy[col] = df_copy[col].astype(str)
#                 print(f"{col} onverted to string type.")

#             elif expected_type == pd.Timestamp:
#                 df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
#                 print(f"{col} onverted to string type.")

#             else:
#                 # Generic message for any other types not explicitly handled
#                 print(f"  Warning: No specific conversion rule for type {expected_type.__name__}. Column '{col}' type not validated or converted.")

#         except Exception as e:
#             # Catch any unexpected errors during the column-wide conversion.
#             print(f"Error processing column '{col}': {str(e)}. Column data may be inconsistent.")

#     return df_copy



# def validate_and_report(df: pd.DataFrame) -> None:
#     """Validate DataFrame and print detailed report."""
#     results = validate_dataframe_schema(df)
    
#     log_and_print("=== DataFrame Validation Report ===")
#     log_and_print(f"Overall valid: {results['is_valid']}")
    
#     if results['missing_columns']:
#         log_and_print(f"❌ Missing columns: {results['missing_columns']}")
    
#     if results['extra_columns']:
#         print(f"⚠️  Extra columns: {results['extra_columns']}")
    
#     if results['type_mismatches']:
#         print("❌ Type mismatches:")
#         for mismatch in results['type_mismatches']:
#             print(f"  - {mismatch['column']}: expected {mismatch['expected']}, got {mismatch['actual']}")

