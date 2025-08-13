#!/usr/bin/env python3
"""
Analyze correlation between data_procedimento and data_inicio_inducao in view_tratamentos
"""

import pandas as pd
import duckdb as db
import numpy as np
from datetime import datetime

def analyze_correlation():
    """Analyze correlation between data_procedimento and data_inicio_inducao"""
    
    print("="*80)
    print("ANALYZING CORRELATION BETWEEN data_procedimento AND data_inicio_inducao")
    print("="*80)
    
    # Connect to database
    conn = db.connect('database/clinisys_all.duckdb', read_only=True)
    
    try:
        # First, let's see the table structure
        print("\n1. TABLE STRUCTURE:")
        print("-" * 40)
        columns_query = "SELECT * FROM silver.view_tratamentos LIMIT 1"
        sample_data = conn.execute(columns_query).fetchdf()
        print(f"Available columns: {list(sample_data.columns)}")
        
        # Check if data_inicio_inducao exists
        if 'data_inicio_inducao' not in sample_data.columns:
            print("\n❌ Column 'data_inicio_inducao' not found in view_tratamentos")
            print("Available date-related columns:")
            date_columns = [col for col in sample_data.columns if 'data' in col.lower()]
            for col in date_columns:
                print(f"  - {col}")
            return
        
        # Get data with both columns
        print("\n2. DATA ANALYSIS:")
        print("-" * 40)
        
        query = """
        SELECT 
            id,
            prontuario,
            data_procedimento,
            data_inicio_inducao,
            tentativa,
            tipo_procedimento
        FROM silver.view_tratamentos 
        WHERE data_procedimento IS NOT NULL 
           OR data_inicio_inducao IS NOT NULL
        ORDER BY prontuario, tentativa
        """
        
        df = conn.execute(query).fetchdf()
        
        print(f"Total records: {len(df)}")
        print(f"Records with data_procedimento: {df['data_procedimento'].notna().sum()}")
        print(f"Records with data_inicio_inducao: {df['data_inicio_inducao'].notna().sum()}")
        print(f"Records with both dates: {(df['data_procedimento'].notna() & df['data_inicio_inducao'].notna()).sum()}")
        
        # Show sample data before conversion
        print("\n3. SAMPLE DATA (before conversion):")
        print("-" * 40)
        sample = df.head(10)
        print(sample.to_string(index=False))
        
        # Convert data_inicio_inducao from string to date
        print("\n4. CONVERTING data_inicio_inducao TO DATE:")
        print("-" * 40)
        
        # Function to safely convert string to date
        def convert_to_date(date_str):
            if pd.isna(date_str) or date_str is None:
                return None
            try:
                # Handle invalid dates like '00/00/0000'
                if date_str in ['00/00/0000', '0000-00-00', '00/00/00', '0000/00/00']:
                    return None
                
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']:
                    try:
                        parsed_date = pd.to_datetime(date_str, format=fmt)
                        # Check if the date is reasonable (not too far in past or future)
                        if parsed_date.year < 1900 or parsed_date.year > 2030:
                            return None
                        return parsed_date
                    except:
                        continue
                # If no specific format works, try pandas automatic parsing
                parsed_date = pd.to_datetime(date_str)
                # Check if the date is reasonable
                if parsed_date.year < 1900 or parsed_date.year > 2030:
                    return None
                return parsed_date
            except:
                return None
        
        # Convert data_inicio_inducao
        df['data_inicio_inducao_converted'] = df['data_inicio_inducao'].apply(convert_to_date)
        
        # Show conversion results
        print(f"Original data_inicio_inducao sample values:")
        print(df['data_inicio_inducao'].head(10).tolist())
        print(f"\nConverted data_inicio_inducao sample values:")
        print(df['data_inicio_inducao_converted'].head(10).tolist())
        
        # Count successful conversions
        successful_conversions = df['data_inicio_inducao_converted'].notna().sum()
        total_with_inducao = df['data_inicio_inducao'].notna().sum()
        print(f"\nConversion success rate: {successful_conversions}/{total_with_inducao} ({successful_conversions/total_with_inducao*100:.1f}%)")
        
        # Analyze correlation for records with both dates
        both_dates = df[
            df['data_procedimento'].notna() & 
            df['data_inicio_inducao_converted'].notna()
        ].copy()
        
        # Additional filtering to remove any problematic dates
        both_dates = both_dates[
            (both_dates['data_procedimento'].dt.year >= 1900) &
            (both_dates['data_procedimento'].dt.year <= 2030) &
            (both_dates['data_inicio_inducao_converted'].dt.year >= 1900) &
            (both_dates['data_inicio_inducao_converted'].dt.year <= 2030)
        ]
        
        if len(both_dates) > 0:
            print(f"\n5. CORRELATION ANALYSIS (for {len(both_dates)} records with both dates):")
            print("-" * 40)
            
            # Calculate date differences
            both_dates['date_diff_days'] = (both_dates['data_procedimento'] - both_dates['data_inicio_inducao_converted']).dt.days
            
            print(f"Date difference statistics (data_procedimento - data_inicio_inducao):")
            print(f"  Mean: {both_dates['date_diff_days'].mean():.2f} days")
            print(f"  Median: {both_dates['date_diff_days'].median():.2f} days")
            print(f"  Min: {both_dates['date_diff_days'].min()} days")
            print(f"  Max: {both_dates['date_diff_days'].max()} days")
            print(f"  Std: {both_dates['date_diff_days'].std():.2f} days")
            
            # Show distribution
            print(f"\nDate difference distribution:")
            print(f"  Same day (0 days): {(both_dates['date_diff_days'] == 0).sum()} records")
            print(f"  1-7 days: {((both_dates['date_diff_days'] >= 1) & (both_dates['date_diff_days'] <= 7)).sum()} records")
            print(f"  8-14 days: {((both_dates['date_diff_days'] >= 8) & (both_dates['date_diff_days'] <= 14)).sum()} records")
            print(f"  15+ days: {(both_dates['date_diff_days'] >= 15).sum()} records")
            print(f"  Negative (procedimento before inducao): {(both_dates['date_diff_days'] < 0).sum()} records")
            
            # Show examples
            print(f"\n6. EXAMPLES:")
            print("-" * 40)
            examples = both_dates[['prontuario', 'tentativa', 'data_inicio_inducao', 'data_inicio_inducao_converted', 'data_procedimento', 'date_diff_days']].head(10)
            print(examples.to_string(index=False))
            
            # Check for patterns by tentativa
            print(f"\n7. PATTERNS BY TENTATIVA:")
            print("-" * 40)
            tentativa_stats = both_dates.groupby('tentativa')['date_diff_days'].agg(['count', 'mean', 'std', 'min', 'max']).round(2)
            print(tentativa_stats)
            
            # Check for records with missing data_procedimento but having data_inicio_inducao
            missing_procedimento = df[
                df['data_procedimento'].isna() & 
                df['data_inicio_inducao_converted'].notna()
            ]
            
            if len(missing_procedimento) > 0:
                print(f"\n8. RECORDS WITH MISSING data_procedimento BUT HAVING data_inicio_inducao:")
                print("-" * 40)
                print(f"Found {len(missing_procedimento)} such records")
                print(missing_procedimento[['prontuario', 'tentativa', 'data_inicio_inducao', 'data_inicio_inducao_converted', 'tipo_procedimento']].head(10).to_string(index=False))
                
                # These could be candidates for using data_inicio_inducao as data_procedimento
                print(f"\n9. POTENTIAL CANDIDATES FOR DATE IMPUTATION:")
                print("-" * 40)
                print(f"Records with missing data_procedimento but valid data_inicio_inducao: {len(missing_procedimento)}")
                print("These could potentially use data_inicio_inducao as a fallback for data_procedimento")
            
        else:
            print("\n❌ No records found with both data_procedimento and data_inicio_inducao")
        
        # Show failed conversions
        failed_conversions = df[
            df['data_inicio_inducao'].notna() & 
            df['data_inicio_inducao_converted'].isna()
        ]
        
        if len(failed_conversions) > 0:
            print(f"\n10. FAILED CONVERSIONS:")
            print("-" * 40)
            print(f"Found {len(failed_conversions)} records with failed date conversions")
            print("Sample failed conversions:")
            failed_sample = failed_conversions[['prontuario', 'tentativa', 'data_inicio_inducao']].head(10)
            print(failed_sample.to_string(index=False))
        
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_correlation()
