#!/usr/bin/env python3
"""
Universal Age Management Utilities

This module provides standardized functions for:
- Calculating age from date of birth
- Calculating age at collection from DOB and session date
- Determining age groups (YOUTH, HIGH SCHOOL, COLLEGE, PRO)
- Standardizing age group values across all tables

Age Group Definitions:
- YOUTH: < 13 years
- HIGH SCHOOL: 14-18 years (inclusive)
- COLLEGE: 18-22 years (inclusive)
- PRO: 22+ years

Note: Age group is based on CURRENT age for d_athletes table.
      Age group is based on age_at_collection for fact tables (but we don't store it there).
"""

from typing import Optional
from datetime import date, datetime


def calculate_age(date_of_birth: Optional[date], reference_date: Optional[date] = None) -> Optional[float]:
    """
    Calculate age in years from date of birth.
    
    Args:
        date_of_birth: Date of birth
        reference_date: Reference date (defaults to today)
        
    Returns:
        Age in years as float, or None if DOB is missing
    """
    if not date_of_birth:
        return None
    
    if reference_date is None:
        reference_date = date.today()
    
    try:
        age_days = (reference_date - date_of_birth).days
        age_years = age_days / 365.25
        return age_years
    except Exception:
        return None


def calculate_age_at_collection(session_date: Optional[date], date_of_birth: Optional[date]) -> Optional[float]:
    """
    Calculate age at collection from session date and date of birth.
    
    Args:
        session_date: Date when data was collected
        date_of_birth: Athlete's date of birth
        
    Returns:
        Age in years as float, or None if DOB or session_date is missing
    """
    if not date_of_birth or not session_date:
        return None
    
    return calculate_age(date_of_birth, session_date)


def calculate_age_group(age: Optional[float]) -> Optional[str]:
    """
    Calculate age group based on age.
    
    Age Group Definitions:
    - YOUTH: < 13 years
    - HIGH SCHOOL: 14-18 years (inclusive)
    - COLLEGE: 18-22 years (inclusive)
    - PRO: 22+ years
    
    Args:
        age: Age in years
        
    Returns:
        "YOUTH", "HIGH SCHOOL", "COLLEGE", "PRO", or None
    """
    if age is None:
        return None
    
    if age < 13:
        return "YOUTH"
    elif 14 <= age <= 18:
        return "HIGH SCHOOL"
    elif 18 < age <= 22:
        return "COLLEGE"
    else:  # age > 22
        return "PRO"


def standardize_age_group(age_group: Optional[str]) -> Optional[str]:
    """
    Standardize age group value to one of: YOUTH, HIGH SCHOOL, COLLEGE, PRO.
    
    Handles various formats and case variations:
    - "youth", "Youth", "YOUTH" -> "YOUTH"
    - "high school", "High School", "HIGH SCHOOL", "HS" -> "HIGH SCHOOL"
    - "college", "College", "COLLEGE" -> "COLLEGE"
    - "pro", "Pro", "PRO", "Professional" -> "PRO"
    
    Args:
        age_group: Age group string (may be in various formats)
        
    Returns:
        Standardized age group or None
    """
    if not age_group:
        return None
    
    age_group_upper = str(age_group).strip().upper()
    
    # Map variations to standard values
    if age_group_upper in ("YOUTH", "Y"):
        return "YOUTH"
    elif age_group_upper in ("HIGH SCHOOL", "HIGH_SCHOOL", "HS", "HIGHSCHOOL"):
        return "HIGH SCHOOL"
    elif age_group_upper in ("COLLEGE", "C"):
        return "COLLEGE"
    elif age_group_upper in ("PRO", "PROFESSIONAL", "P"):
        return "PRO"
    else:
        # Try to parse as age and calculate group
        try:
            age = float(age_group_upper)
            return calculate_age_group(age)
        except (ValueError, TypeError):
            return None


def parse_date(date_str: Optional[str], formats: Optional[list] = None) -> Optional[date]:
    """
    Parse date string in various formats.
    
    Args:
        date_str: Date string
        formats: List of date formats to try (defaults to common formats)
        
    Returns:
        date object or None
    """
    if not date_str:
        return None
    
    if isinstance(date_str, date):
        return date_str
    
    if isinstance(date_str, datetime):
        return date_str.date()
    
    if formats is None:
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
        ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except (ValueError, TypeError):
            continue
    
    return None


def get_athlete_dob_for_age_calculation(athlete_uuid: str, conn=None) -> Optional[date]:
    """
    Get athlete's date of birth from d_athletes table for age calculation.
    
    This is a helper function for ETL scripts to get DOB when calculating age_at_collection.
    
    Args:
        athlete_uuid: Athlete UUID
        conn: Optional database connection (creates new if not provided)
        
    Returns:
        date_of_birth as date object, or None if not found
    """
    from python.common.athlete_manager import get_warehouse_connection
    
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date_of_birth
                FROM analytics.d_athletes
                WHERE athlete_uuid = %s
            """, (athlete_uuid,))
            result = cur.fetchone()
            if result and result[0]:
                dob = result[0]
                if isinstance(dob, date):
                    return dob
                return parse_date(str(dob))
            return None
    finally:
        if close_conn:
            conn.close()
