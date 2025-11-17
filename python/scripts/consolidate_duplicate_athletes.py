#!/usr/bin/env python3
"""
Consolidate Duplicate Athletes Script

This script identifies and merges duplicate athletes in analytics.d_athletes
based on normalized_name. It:
1. Finds all athletes with the same normalized_name
2. Selects the "best" UUID (preferring one with app_db_uuid, or oldest created_at)
3. Updates all fact tables to use the canonical UUID
4. Merges athlete metadata (non-destructive)
5. Deletes duplicate athlete records

Usage:
    python python/scripts/consolidate_duplicate_athletes.py [--dry-run] [--verbose]
"""

import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.common.athlete_manager import (
    get_warehouse_connection,
    normalize_name_for_matching,
    normalize_name_for_display
)
import psycopg2
from psycopg2.extras import RealDictCursor


def find_duplicate_athletes(conn) -> Dict[str, List[Dict[str, Any]]]:
    """
    Find all athletes with duplicate normalized names.
    
    IMPORTANT: Re-normalizes names to remove dates that may have been stored
    in normalized_name. This handles cases where dates weren't properly removed
    during initial normalization.
    
    Returns:
        Dictionary mapping truly normalized name (no dates) to list of athlete records
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('''
            SELECT 
                athlete_uuid,
                name,
                normalized_name,
                date_of_birth,
                app_db_uuid,
                created_at,
                source_system,
                source_athlete_id
            FROM analytics.d_athletes
            ORDER BY normalized_name, created_at
        ''')
        
        all_athletes = cur.fetchall()
    
    # Group by re-normalized name (removes dates that may be in normalized_name)
    # Use the original name field and normalize it properly
    duplicates = defaultdict(list)
    for athlete in all_athletes:
        # Re-normalize the name to ensure dates are removed
        # This handles cases where normalized_name still contains dates
        truly_normalized = normalize_name_for_matching(athlete['name'])
        if truly_normalized:  # Skip if normalization results in empty string
            duplicates[truly_normalized].append(dict(athlete))
    
    # Filter to only groups with duplicates
    return {k: v for k, v in duplicates.items() if len(v) > 1}


def select_canonical_uuid(athletes: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
    """
    Select the best UUID to keep as canonical.
    
    Priority:
    1. Has app_db_uuid (most authoritative)
    2. Oldest created_at (first created)
    3. First in list
    
    Returns:
        Tuple of (canonical_uuid, list_of_duplicate_uuids_to_merge)
    """
    # Sort by priority
    sorted_athletes = sorted(
        athletes,
        key=lambda a: (
            a['app_db_uuid'] is None,  # False (has app_db_uuid) comes before True
            a['created_at']  # Older comes first
        )
    )
    
    canonical = sorted_athletes[0]
    duplicates = [a['athlete_uuid'] for a in sorted_athletes[1:]]
    
    return canonical['athlete_uuid'], duplicates


def merge_athlete_data(canonical: Dict[str, Any], duplicates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge data from duplicate athletes into canonical record.
    
    Non-destructive: only fills in NULL values in canonical with values from duplicates.
    """
    merged = canonical.copy()
    
    for dup in duplicates:
        for key in ['name', 'date_of_birth', 'app_db_uuid', 'gender', 'height', 'weight', 
                   'email', 'phone', 'notes', 'source_system', 'source_athlete_id']:
            if merged.get(key) is None and dup.get(key) is not None:
                merged[key] = dup[key]
    
    return merged


def get_fact_tables_with_athlete_uuid(conn) -> List[str]:
    """
    Find all fact tables that reference athlete_uuid.
    
    Excludes the dimension table analytics.d_athletes itself.
    
    Returns:
        List of table names (schema.table format)
    """
    with conn.cursor() as cur:
        cur.execute('''
            SELECT 
                table_schema,
                table_name
            FROM information_schema.columns
            WHERE column_name = 'athlete_uuid'
              AND table_schema IN ('public', 'analytics')
              AND NOT (table_schema = 'analytics' AND table_name = 'd_athletes')
            ORDER BY table_schema, table_name
        ''')
        
        return [f"{row[0]}.{row[1]}" for row in cur.fetchall()]


def update_fact_tables(conn, canonical_uuid: str, duplicate_uuids: List[str], 
                       fact_tables: List[str], dry_run: bool = False) -> Dict[str, int]:
    """
    Update all fact tables to use canonical UUID instead of duplicate UUIDs.
    
    Returns:
        Dictionary mapping table name to number of rows updated
    """
    updates = {}
    
    for table in fact_tables:
        schema, table_name = table.split('.')
        
        # Build update query
        placeholders = ','.join(['%s'] * len(duplicate_uuids))
        query = f'''
            UPDATE {schema}."{table_name}"
            SET athlete_uuid = %s
            WHERE athlete_uuid IN ({placeholders})
        '''
        
        params = [canonical_uuid] + duplicate_uuids
        
        if not dry_run:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rowcount = cur.rowcount
                conn.commit()
                updates[table] = rowcount
        else:
            # Dry run: just count what would be updated
            with conn.cursor() as cur:
                count_query = f'''
                    SELECT COUNT(*) 
                    FROM {schema}."{table_name}"
                    WHERE athlete_uuid IN ({placeholders})
                '''
                cur.execute(count_query, duplicate_uuids)
                updates[table] = cur.fetchone()[0]
    
    return updates


def consolidate_duplicates(dry_run: bool = False, verbose: bool = False):
    """
    Main consolidation function.
    """
    conn = get_warehouse_connection()
    
    try:
        print("=" * 80)
        print("ATHLETE CONSOLIDATION SCRIPT")
        print("=" * 80)
        if dry_run:
            print("[DRY RUN MODE - No changes will be made]")
        print()
        
        # Find duplicates
        print("Step 1: Finding duplicate athletes...")
        duplicates = find_duplicate_athletes(conn)
        
        if not duplicates:
            print("✓ No duplicates found! All athletes have unique normalized names.")
            return
        
        print(f"Found {len(duplicates)} groups of duplicate athletes")
        print()
        
        # Get fact tables
        print("Step 2: Finding fact tables to update...")
        fact_tables = get_fact_tables_with_athlete_uuid(conn)
        print(f"Found {len(fact_tables)} fact tables: {', '.join(fact_tables)}")
        print()
        
        # Process each duplicate group
        total_merged = 0
        total_fact_updates = defaultdict(int)
        
        print("Step 3: Processing duplicate groups...")
        print("-" * 80)
        
        for normalized_name, athletes in sorted(duplicates.items()):
            if verbose:
                print(f"\nProcessing: {normalized_name}")
                print(f"  Found {len(athletes)} duplicate records:")
                for a in athletes:
                    print(f"    - {a['name']} ({a['athlete_uuid']}) - Created: {a['created_at']}")
            
            # Select canonical UUID
            canonical_uuid, duplicate_uuids = select_canonical_uuid(athletes)
            canonical_athlete = next(a for a in athletes if a['athlete_uuid'] == canonical_uuid)
            
            print(f"\n{normalized_name}:")
            print(f"  Keeping: {canonical_athlete['name']} ({canonical_uuid})")
            print(f"  Merging: {len(duplicate_uuids)} duplicates")
            
            if verbose:
                for dup_uuid in duplicate_uuids:
                    dup_athlete = next(a for a in athletes if a['athlete_uuid'] == dup_uuid)
                    print(f"    - {dup_athlete['name']} ({dup_uuid})")
            
            # Merge athlete data
            merged_data = merge_athlete_data(canonical_athlete, 
                                            [a for a in athletes if a['athlete_uuid'] in duplicate_uuids])
            
            # Prepare name updates (but don't apply yet - need to delete duplicates first)
            all_names = [canonical_athlete['name']] + [a['name'] for a in athletes if a['athlete_uuid'] in duplicate_uuids]
            normalized_names = [normalize_name_for_display(n) for n in all_names if n]
            best_name = None
            correct_normalized = None
            if normalized_names:
                best_name = max(normalized_names, key=len)
                correct_normalized = normalize_name_for_matching(best_name)
            
            # Step 1: Update fact tables FIRST (before deleting duplicates)
            fact_updates = update_fact_tables(conn, canonical_uuid, duplicate_uuids, 
                                             fact_tables, dry_run)
            
            for table, count in fact_updates.items():
                total_fact_updates[table] += count
                if count > 0:
                    print(f"  Updated {count} rows in {table}")
            
            # Step 2: Delete duplicate athlete records BEFORE updating normalized_name
            # This prevents unique constraint violations
            if not dry_run:
                placeholders = ','.join(['%s'] * len(duplicate_uuids))
                with conn.cursor() as cur:
                    cur.execute(f'''
                        DELETE FROM analytics.d_athletes
                        WHERE athlete_uuid IN ({placeholders})
                    ''', duplicate_uuids)
                    conn.commit()
                    print(f"  Deleted {len(duplicate_uuids)} duplicate athlete records")
            
            # Step 3: NOW update canonical athlete record (duplicates are gone, so no constraint violation)
            if not dry_run:
                updates = []
                params = []
                
                # Update other fields (non-destructive merge)
                for key in ['date_of_birth', 'app_db_uuid', 'gender', 'height', 'weight',
                           'email', 'phone', 'notes', 'source_system', 'source_athlete_id']:
                    if merged_data.get(key) is not None and canonical_athlete.get(key) != merged_data[key]:
                        # Only update if canonical is NULL and merged has a value
                        if canonical_athlete.get(key) is None:
                            updates.append(f"{key} = COALESCE({key}, %s)")
                            params.append(merged_data[key])
                
                # Always update name to "First Last" format (removes dates, converts Last, First)
                # Now safe to update normalized_name since duplicates are deleted
                if best_name and correct_normalized:
                    updates.append("name = %s")
                    params.append(best_name)
                    updates.append("normalized_name = %s")
                    params.append(correct_normalized)
                
                if updates:
                    params.append(canonical_uuid)
                    with conn.cursor() as cur:
                        cur.execute(f'''
                            UPDATE analytics.d_athletes
                            SET {', '.join(updates)}
                            WHERE athlete_uuid = %s
                        ''', params)
                        conn.commit()
                        if verbose:
                            print(f"  Updated canonical athlete with merged data")
                            if best_name:
                                print(f"    Name updated to: {best_name}")
                                print(f"    Normalized name updated to: {correct_normalized}")
            
            total_merged += len(duplicate_uuids)
        
        # Summary
        print()
        print("=" * 80)
        print("CONSOLIDATION SUMMARY")
        print("=" * 80)
        print(f"Total duplicate groups processed: {len(duplicates)}")
        print(f"Total duplicate records merged: {total_merged}")
        print()
        print("Fact table updates:")
        for table, count in sorted(total_fact_updates.items()):
            print(f"  {table}: {count} rows")
        
        if dry_run:
            print()
            print("This was a DRY RUN. No changes were made.")
            print("Run without --dry-run to apply changes.")
        
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Consolidate duplicate athletes in warehouse database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed information about each duplicate group'
    )
    
    args = parser.parse_args()
    
    consolidate_duplicates(dry_run=args.dry_run, verbose=args.verbose)

