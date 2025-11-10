"""
File parsing utilities for Readiness Screen.
Handles XML and ASCII file parsing.
"""
import os
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional, Dict


# ASCII file mapping
ASCII_FILES = {
    "I": "i_data.txt",
    "Y": "y_data.txt",
    "T": "t_data.txt",
    "IR90": "ir90_data.txt",
    "CMJ": "cmj_data.txt",
    "PPU": "ppu_data.txt"
}

# Headers for different file types
CMJ_PPU_HEADERS = [
    "JH_IN", "LEWIS_PEAK_POWER", "Max_Force",
    "PP_W_per_kg", "PP_FORCEPLATE", "Force_at_PP", "Vel_at_PP"
]

FORCE_HEADERS = [
    "Max_Force", "Max_Force_Norm",
    "Avg_Force", "Avg_Force_Norm", "Time_to_Max"
]


def find_session_xml(folder_path: str) -> Optional[str]:
    """
    Find the Session XML file in a folder.
    
    Args:
        folder_path: Directory to search.
    
    Returns:
        Path to Session XML file or None if not found.
    """
    for root_dir, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().startswith('session') and file.lower().endswith('.xml'):
                return os.path.join(root_dir, file)
    return None


def find_text(element: ET.Element, tag: str) -> Optional[str]:
    """
    Safely find and return text from an XML element.
    
    Args:
        element: XML element to search.
        tag: Tag name to find.
    
    Returns:
        Text content or None if not found.
    """
    found = element.find(tag)
    return found.text if found is not None else None


def parse_xml_file(xml_file_path: str) -> Dict:
    """
    Parse Session XML file and extract participant information.
    
    Args:
        xml_file_path: Path to Session XML file.
    
    Returns:
        Dictionary with parsed XML data.
    """
    tree = ET.parse(xml_file_path)
    xml_root = tree.getroot()
    
    session_fields = xml_root.find(".//Session/Fields")
    if session_fields is None:
        raise ValueError("Session/Fields not found in XML file")
    
    name = find_text(session_fields, "Name")
    height = find_text(session_fields, "Height")
    weight = find_text(session_fields, "Weight")
    plyo_day = find_text(session_fields, "Plyo_Day")
    creation_date = find_text(session_fields, "Creation_date")
    
    if None in [name, height, weight, plyo_day, creation_date]:
        raise ValueError("Missing required data in XML file")
    
    return {
        'name': name,
        'height': float(height) if height else None,
        'weight': float(weight) if weight else None,
        'plyo_day': plyo_day,
        'creation_date': creation_date
    }


def parse_ascii_file(file_path: str, movement_type: str) -> pd.DataFrame:
    """
    Parse an ASCII data file for a specific movement type.
    
    Args:
        file_path: Path to ASCII file.
        movement_type: Movement type (I, Y, T, IR90, CMJ, or PPU).
    
    Returns:
        DataFrame with parsed data.
    """
    if movement_type in {"CMJ", "PPU"}:
        headers = CMJ_PPU_HEADERS
    else:
        headers = FORCE_HEADERS
    
    df = pd.read_csv(file_path, sep=r'\s+', skiprows=5, names=headers)
    return df


def select_folder_dialog(initial_dir: str = 'D:/Readiness Screen 3/Data/') -> Optional[str]:
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
        root.withdraw()
        selected_folder = filedialog.askdirectory(initialdir=initial_dir)
        root.destroy()
        return selected_folder
    except Exception as e:
        print(f"Error opening folder dialog: {e}")
        return None

