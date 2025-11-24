"""
Main orchestration script for Pro-Sup Test data processing.
Coordinates XML parsing, ASCII parsing, score calculation, and optional report generation.
"""
import os
import sys
from pathlib import Path

# Add python directory to path so imports work
project_root = Path(__file__).parent.parent.parent
python_dir = project_root / "python"
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

from common.config import get_raw_paths

from database import get_connection, insert_xml_data, update_ascii_data, load_all_data, save_dataframe
from file_parsers import (
    find_session_xml, extract_test_date_from_folder, extract_test_date_from_ascii,
    parse_xml_file, parse_ascii_file, select_folder_dialog
)
from score_calculation import calculate_all_scores, add_percentile_columns
from report_generation import generate_pdf_report


def process_xml_data(folder_path: str, db_path: str, 
                     use_dialog: bool = True) -> tuple:
    """
    Process XML data from a folder.
    
    Args:
        folder_path: Path to folder containing Session XML file.
        db_path: Path to database file.
        use_dialog: If True, show folder selection dialog.
    
    Returns:
        Tuple of (athlete_name, test_date) for use in subsequent steps.
    """
    print("=" * 60)
    print("Pro-Sup Test Data Processing")
    print("=" * 60)
    
    # Select folder if using dialog
    if use_dialog:
        selected_folder = select_folder_dialog()
        if not selected_folder:
            raise ValueError("No folder was selected.")
        folder_path = selected_folder
    
    # Extract test date from folder name
    test_date = extract_test_date_from_folder(folder_path)
    
    # Find Session XML file
    xml_file_path = find_session_xml(folder_path)
    if not xml_file_path:
        raise FileNotFoundError("No 'Session' XML file found in the selected folder.")
    
    print(f"\n1. Processing XML file: {xml_file_path}")
    
    # Parse XML
    xml_data = parse_xml_file(xml_file_path, test_date)
    athlete_name = xml_data['name']
    
    # Insert into database (will skip if already exists)
    conn = get_connection(db_path)
    inserted = insert_xml_data(conn, xml_data, skip_if_exists=True)
    conn.close()
    
    if inserted:
        print(f"   XML data inserted for {athlete_name} with test date {test_date}.")
    else:
        print(f"   XML data already exists for {athlete_name} with test date {test_date}.")
    
    return athlete_name, test_date


def process_all_folders(base_dir: str, db_path: str):
    """
    Process all folders containing Session XML files in a directory.
    
    Args:
        base_dir: Base directory containing folders with Session XML files.
        db_path: Path to database file.
    
    Returns:
        List of tuples (athlete_name, test_date) for processed folders.
    """
    print("=" * 60)
    print("Pro-Sup Test Batch Processing")
    print("=" * 60)
    print(f"\nScanning directory: {base_dir}")
    
    if not os.path.exists(base_dir):
        raise ValueError(f"Directory not found: {base_dir}")
    
    # Find all folders containing Session XML files
    folders_with_xml = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().startswith('session') and file.lower().endswith('.xml'):
                folders_with_xml.append(root)
                break
    
    if not folders_with_xml:
        print("No folders with Session XML files found.")
        return []
    
    print(f"Found {len(folders_with_xml)} folders with Session XML files")
    
    # Process each folder
    conn = get_connection(db_path)
    processed = []
    skipped = []
    errors = []
    
    for i, folder_path in enumerate(folders_with_xml, 1):
        try:
            # Extract test date from folder name
            test_date = extract_test_date_from_folder(folder_path)
            
            # Find Session XML file
            xml_file_path = find_session_xml(folder_path)
            if not xml_file_path:
                print(f"\n[{i}/{len(folders_with_xml)}] Skipping {folder_path} - no Session XML found")
                skipped.append(folder_path)
                continue
            
            # Parse XML
            xml_data = parse_xml_file(xml_file_path, test_date)
            athlete_name = xml_data['name']
            
            print(f"\n[{i}/{len(folders_with_xml)}] Processing: {athlete_name} ({test_date})")
            
            # Insert into database (will skip if already exists)
            inserted = insert_xml_data(conn, xml_data, skip_if_exists=True)
            
            if inserted:
                processed.append((athlete_name, test_date))
                print(f"   ✓ Inserted")
            else:
                skipped.append(f"{athlete_name} ({test_date})")
                print(f"   ⊘ Skipped (already exists)")
                
        except Exception as e:
            error_msg = f"{folder_path}: {str(e)}"
            errors.append(error_msg)
            print(f"   ✗ Error: {str(e)}")
    
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("Batch Processing Summary")
    print("=" * 60)
    print(f"Processed: {len(processed)} folders")
    print(f"Skipped (already exists): {len(skipped)} folders")
    print(f"Errors: {len(errors)} folders")
    
    if errors:
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
    
    return processed


def process_ascii_data(ascii_file_path: str, db_path: str, 
                       athlete_name: str, test_date: str):
    """
    Process ASCII data file and update database.
    
    Args:
        ascii_file_path: Path to ASCII data file.
        db_path: Path to database file.
        athlete_name: Athlete name (for WHERE clause).
        test_date: Test date (for WHERE clause).
    """
    print(f"\n2. Processing ASCII file: {ascii_file_path}")
    
    # Extract test date from ASCII file (for validation)
    ascii_test_date = extract_test_date_from_ascii(ascii_file_path)
    print(f"   Extracted test date: {ascii_test_date}")
    
    # Parse ASCII file
    ascii_values = parse_ascii_file(ascii_file_path)
    
    # Update database
    conn = get_connection(db_path)
    update_ascii_data(conn, ascii_values, athlete_name, test_date)
    conn.close()
    
    print(f"   ASCII data updated for {athlete_name} on test date {test_date}.")


def calculate_scores(db_path: str):
    """
    Calculate fatigue indices, consistency penalties, and total scores.
    
    Args:
        db_path: Path to database file.
    """
    print("\n3. Calculating scores...")
    
    # Load data
    conn = get_connection(db_path)
    df = load_all_data(conn)
    conn.close()
    
    # Calculate all scores
    df = calculate_all_scores(df)
    
    # Save back to database
    conn = get_connection(db_path)
    save_dataframe(conn, df, if_exists='replace')
    conn.close()
    
    print("   Total score calculation complete!")


def generate_report_for_athlete(athlete_name: str, test_date: str,
                                db_path: str, output_dir: str,
                                logo_path: str = None):
    """
    Generate PDF report for an athlete.
    
    Args:
        athlete_name: Name of the athlete.
        test_date: Test date string.
        db_path: Path to database file.
        output_dir: Directory for output report.
        logo_path: Optional path to logo image.
    
    Returns:
        Path to generated PDF file.
    """
    print(f"\n4. Generating report for {athlete_name}...")
    
    pdf_path = generate_pdf_report(
        athlete_name=athlete_name,
        test_date=test_date,
        db_path=db_path,
        output_dir=output_dir,
        logo_path=logo_path
    )
    
    print(f"   Report saved: {pdf_path}")
    return pdf_path


def main():
    """
    Main execution function.
    Configure paths and processing options here.
    """
    # Get paths from config (or use defaults)
    try:
        raw_paths = get_raw_paths()
        base_dir = raw_paths.get('pro_sup', 'D:/Pro-Sup Test/')
    except:
        base_dir = 'D:/Pro-Sup Test/'
    
    db_path = os.path.join(base_dir, 'pro-sup_data.sqlite')
    ascii_file_path = os.path.join(base_dir, 'pro-sup_data.txt')
    report_dir = os.path.join(base_dir, 'Reports')
    logo_path = None  # Update with your logo path if needed
    
    # Processing options
    BATCH_PROCESS = True  # Process all folders in directory (False = single folder)
    USE_FOLDER_DIALOG = True  # Show folder selection dialog (only if BATCH_PROCESS=False)
    PROCESS_ASCII = True  # Process ASCII file
    CALCULATE_SCORES = True  # Calculate fatigue indices and scores
    GENERATE_REPORT = False  # Generate PDF report
    
    if BATCH_PROCESS:
        # Step 1: Process all folders
        processed = process_all_folders(base_dir, db_path)
        
        if not processed:
            print("No folders were processed.")
            return
        
        # Step 2: Process ASCII data (if single file exists)
        if PROCESS_ASCII and os.path.exists(ascii_file_path):
            print("\n2. Processing ASCII file...")
            # Note: ASCII processing would need to be updated to handle multiple athletes
            # For now, this processes the single ASCII file
            print(f"   ASCII file found: {ascii_file_path}")
            print("   Note: ASCII processing for batch mode needs individual file matching")
        
        # Step 3: Calculate scores
        if CALCULATE_SCORES:
            calculate_scores(db_path)
        
        print("\n" + "=" * 60)
        print("All processing complete!")
        print("=" * 60)
        
    else:
        # Single folder processing (original behavior)
        # Step 1: Process XML data
        athlete_name, test_date = process_xml_data(
            folder_path=base_dir,
            db_path=db_path,
            use_dialog=USE_FOLDER_DIALOG
        )
        
        # Step 2: Process ASCII data
        if PROCESS_ASCII and os.path.exists(ascii_file_path):
            process_ascii_data(ascii_file_path, db_path, athlete_name, test_date)
        elif PROCESS_ASCII:
            print(f"\n2. ASCII file not found: {ascii_file_path}")
            print("   Skipping ASCII processing.")
        
        # Step 3: Calculate scores
        if CALCULATE_SCORES:
            calculate_scores(db_path)
        
        # Step 4: Generate report (optional)
        if GENERATE_REPORT:
            generate_report_for_athlete(
                athlete_name=athlete_name,
                test_date=test_date,
                db_path=db_path,
                output_dir=report_dir,
                logo_path=logo_path
            )
        
        print("\n" + "=" * 60)
        print("All processing complete!")
        print("=" * 60)
        
        return athlete_name, test_date


if __name__ == "__main__":
    main()

