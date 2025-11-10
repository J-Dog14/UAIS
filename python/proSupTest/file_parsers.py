"""
File parsing utilities for Pro-Sup Test.
Handles XML and ASCII file parsing.
"""
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict
import pandas as pd


def find_session_xml(folder_path: str) -> Optional[str]:
    """
    Find the Session XML file in a folder.
    
    Args:
        folder_path: Directory to search.
    
    Returns:
        Path to Session XML file or None if not found.
    """
    for r, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().startswith('session') and file.lower().endswith('.xml'):
                return os.path.join(r, file)
    return None


def extract_test_date_from_folder(folder_path: str) -> str:
    """
    Extract test date from folder name.
    
    Args:
        folder_path: Full path to folder (e.g., '2024-08-13_105_Growth Plate_').
    
    Returns:
        Test date in 'YYYY-MM-DD' format.
    """
    folder_name = os.path.basename(folder_path)
    test_date = folder_name.split('_', 1)[0]  # Extract '2024-08-13'
    return test_date


def extract_test_date_from_ascii(ascii_file_path: str) -> str:
    """
    Extract test date from ASCII file path structure.
    
    Args:
        ascii_file_path: Path to the ASCII file.
    
    Returns:
        Test date in 'YYYY-MM-DD' format.
    """
    with open(ascii_file_path, 'r') as file:
        lines = file.readlines()
        # Extract the first file path
        first_file_path = lines[0].strip().split('\t')[0]
        # Split the path to navigate the folder structure
        parts = first_file_path.split('\\')  # Split by folder structure
        if len(parts) > 4:  # Ensure we have enough subfolders
            date_folder = parts[4]  # Get the folder containing the date
            # Use regex to extract 'YYYY-MM-DD' format
            match = re.match(r'^\d{4}-\d{2}-\d{2}', date_folder)
            if match:
                return match.group(0)  # Return the matched date
            else:
                raise ValueError(f"Unable to extract test date from folder: {date_folder}")
        else:
            raise ValueError("Unexpected file path structure: Unable to extract test date.")


def parse_xml_file(xml_file_path: str, test_date: str) -> Dict:
    """
    Parse Session XML file and extract athlete information.
    
    Args:
        xml_file_path: Path to Session XML file.
        test_date: Test date string.
    
    Returns:
        Dictionary with parsed XML data.
    """
    tree = ET.parse(xml_file_path)
    root_xml = tree.getroot()
    
    # Extract required fields from XML
    name = root_xml.find(".//Name").text
    dob = root_xml.find(".//DOB").text
    height = root_xml.find(".//Height").text
    weight = root_xml.find(".//Weight").text
    injury_history = root_xml.find(".//Injury_History").text
    season_phase = root_xml.find(".//Season_Phase").text
    dynomometer_score = root_xml.find(".//Dynamometer_Score_Dominant").text
    comments = root_xml.find(".//Comments").text
    
    # Calculate age from DOB
    dob_date = datetime.strptime(dob, "%Y-%m-%d")
    today = datetime.today()
    age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
    
    # Create data dictionary with XML data and NULL for ASCII columns
    data = {
        'name': name,
        'test_date': test_date,
        'age': age,
        'height': height,
        'weight': weight,
        'injury_history': injury_history,
        'season_phase': season_phase,
        'dynomometer_score': dynomometer_score,
        'comments': comments,
        'forearm_rom_0to10': None,
        'forearm_rom_10to20': None,
        'forearm_rom_20to30': None,
        'forearm_rom': None,
        'tot_rom_0to10': None,
        'tot_rom_10to20': None,
        'tot_rom_20to30': None,
        'tot_rom': None,
        'num_of_flips_0_10': None,
        'num_of_flips_10_20': None,
        'num_of_flips_20_30': None,
        'num_of_flips': None,
        'avg_velo_0_10': None,
        'avg_velo_10_20': None,
        'avg_velo_20_30': None,
        'avg_velo': None
    }
    
    return data


def parse_ascii_file(ascii_file_path: str) -> Dict:
    """
    Parse ASCII data file and extract metrics.
    
    Args:
        ascii_file_path: Path to ASCII file.
    
    Returns:
        Dictionary with parsed ASCII data (column names as keys).
    """
    with open(ascii_file_path, 'r') as file:
        lines = file.readlines()
        data = [line.strip().split('\t') for line in lines]
    
    # Extract header and data rows
    header = data[1]
    df_data = [row[1:] for row in data[5:]]
    df_ascii = pd.DataFrame(df_data, columns=header)
    
    # Replace hyphens with underscores in column names
    df_ascii.columns = [col.replace('-', '_') for col in df_ascii.columns]
    
    # Get first row (assuming single row of data)
    row_ascii = df_ascii.iloc[0]
    
    # Extract ASCII data as dictionary
    values = {col: row_ascii.get(col) for col in df_ascii.columns}
    
    return values


def select_folder_dialog(initial_dir: str = 'D:/Pro-Sup Test/Data/') -> Optional[str]:
    """
    Open a folder selection dialog.
    
    Args:
        initial_dir: Initial directory for the dialog.
    
    Returns:
        Selected folder path or None if cancelled.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        
        root = tk.Tk()
        root.withdraw()  # Hide the root window
        selected_folder = filedialog.askdirectory(initialdir=initial_dir)
        root.destroy()
        return selected_folder
    except Exception as e:
        print(f"Error opening folder dialog: {e}")
        return None

