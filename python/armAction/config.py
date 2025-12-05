"""
Configuration file containing all file paths and database settings.
Database connection is now handled by common.config and common.athlete_manager.
"""

import os

# Input file paths (hardcoded to hard drive location)
APLUS_EVENTS_PATH = r"D:\Youth Pitch Design\Exports\aPlus_events.txt"
APLUS_DATA_PATH = r"D:\Youth Pitch Design\Exports\APlusData.txt"

# Text files for velocities:
AP_TORSO_V_FILE = r"D:\Youth Pitch Design\Exports\aPlus_torsoVelo.txt"
AP_ARM_V_FILE = r"D:\Youth Pitch Design\Exports\aPlus_armVelo.txt"

# Report output settings
OUTPUT_DIR = r"D:\Youth Pitch Design\Reports\Action+"
OUTPUT_DIR_TWO = r"G:\My Drive\Youth Pitch Reports\Reports\Action+"
LOGO_PATH = r"D:\Youth Pitch Design\Logo\8ctnae - Faded 8 to Blue.png"

# Image paths - located in Exports folder
EXPORTS_DIR = r"D:\Youth Pitch Design\Exports"
IMG_FRONT_FP = os.path.join(EXPORTS_DIR, "Front@FP.png")
IMG_SAG_FP = os.path.join(EXPORTS_DIR, "sag@FP.png")
IMG_SAG_MAXER = os.path.join(EXPORTS_DIR, "sag@MaxER.png")
IMG_SAG_REL = os.path.join(EXPORTS_DIR, "sag@Rel.png")

# Capture rate for frame calculations
CAPTURE_RATE = 300

