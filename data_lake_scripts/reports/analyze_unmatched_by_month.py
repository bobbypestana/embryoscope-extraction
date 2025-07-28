import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def analyze_unmatched_by_month():
    """Analyze how many Embryoscope embryos didn't match per year-month."""
    
    logger.info("Starting unmatched embryos analysis by month")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("UNMATCHED EMBRYOS ANALYSIS BY MONTH")
        print("=" * 80)
        
        # Get all unmatched Embryoscope embryos with their dates
        logger.info("Fetching unmatched Embryoscope embryos...")
        unmatched_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            DATE_TRUNC('month', CAST(embryo_FertilizationTime AS DATE)) as year_month
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        ORDER BY year_month DESC, embryo_FertilizationTime DESC
        """
        
        unmatched_df = con.execute(unmatched_query).df()
        logger.info(f"Found {len(unmatched_df):,} unmatched Embryoscope embryos")
        
        # Get all Embryoscope embryos for comparison
        logger.info("Fetching all Embryoscope embryos for comparison...")
        all_embryos_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            DATE_TRUNC('month', CAST(embryo_FertilizationTime AS DATE)) as year_month
        FROM gold.embryoscope_embrioes
        ORDER BY year_month DESC, embryo_FertilizationTime DESC
        """
        
        all_embryos_df = con.execute(all_embryos_query).df()
        logger.info(f"Found {len(all_embryos_df):,} total Embryoscope embryos")
        
        # Get matched embryos for comparison
        logger.info("Fetching matched Embryoscope embryos...")
        matched_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            DATE_TRUNC('month', CAST(embryo_FertilizationTime AS DATE)) as year_month
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        ORDER BY year_month DESC, embryo_FertilizationTime DESC
        """
        
        matched_df = con.execute(matched_query).df()
        logger.info(f"Found {len(matched_df):,} matched Embryoscope embryos")
        
        # Analyze by month
        print(f"\n1. MONTHLY BREAKDOWN")
        print("-" * 80)
        
        # Count unmatched embryos by month
        unmatched_by_month = unmatched_df.groupby('year_month').size().reset_index(name='unmatched_count')
        unmatched_by_month = unmatched_by_month.sort_values('year_month', ascending=False)
        
        # Count all embryos by month
        all_by_month = all_embryos_df.groupby('year_month').size().reset_index(name='total_count')
        all_by_month = all_by_month.sort_values('year_month', ascending=False)
        
        # Count matched embryos by month
        matched_by_month = matched_df.groupby('year_month').size().reset_index(name='matched_count')
        matched_by_month = matched_by_month.sort_values('year_month', ascending=False)
        
        # Merge all data
        monthly_analysis = all_by_month.merge(
            matched_by_month, on='year_month', how='left'
        ).merge(
            unmatched_by_month, on='year_month', how='left'
        )
        
        # Fill NaN values with 0
        monthly_analysis['matched_count'] = monthly_analysis['matched_count'].fillna(0).astype(int)
        monthly_analysis['unmatched_count'] = monthly_analysis['unmatched_count'].fillna(0).astype(int)
        
        # Calculate percentages
        monthly_analysis['match_rate'] = (monthly_analysis['matched_count'] / monthly_analysis['total_count'] * 100).round(1)
        monthly_analysis['unmatch_rate'] = (monthly_analysis['unmatched_count'] / monthly_analysis['total_count'] * 100).round(1)
        
        # Format year_month for display
        monthly_analysis['year_month_display'] = monthly_analysis['year_month'].dt.strftime('%Y-%m')
        
        print("Monthly breakdown of Embryoscope embryos:")
        print(f"{'Year-Month':<12} {'Total':<8} {'Matched':<8} {'Unmatched':<10} {'Match%':<8} {'Unmatch%':<8}")
        print("-" * 80)
        
        for _, row in monthly_analysis.iterrows():
            print(f"{row['year_month_display']:<12} {row['total_count']:<8} {row['matched_count']:<8} {row['unmatched_count']:<10} {row['match_rate']:<8} {row['unmatch_rate']:<8}")
        
        # 2. Summary statistics
        print(f"\n2. SUMMARY STATISTICS")
        print("-" * 50)
        
        total_embryos = len(all_embryos_df)
        total_matched = len(matched_df)
        total_unmatched = len(unmatched_df)
        
        print(f"Total Embryoscope embryos: {total_embryos:,}")
        print(f"Total matched embryos: {total_matched:,} ({total_matched/total_embryos*100:.1f}%)")
        print(f"Total unmatched embryos: {total_unmatched:,} ({total_unmatched/total_embryos*100:.1f}%)")
        
        # 3. Monthly trends
        print(f"\n3. MONTHLY TRENDS")
        print("-" * 50)
        
        # Find months with highest unmatched rates
        high_unmatch_months = monthly_analysis[monthly_analysis['unmatch_rate'] > 50].sort_values('unmatch_rate', ascending=False)
        
        if len(high_unmatch_months) > 0:
            print("Months with >50% unmatched rate:")
            for _, row in high_unmatch_months.iterrows():
                print(f"  {row['year_month_display']}: {row['unmatch_rate']:.1f}% unmatched ({row['unmatched_count']:,} embryos)")
        else:
            print("No months with >50% unmatched rate")
        
        # Find months with lowest unmatched rates
        low_unmatch_months = monthly_analysis[monthly_analysis['unmatch_rate'] < 50].sort_values('unmatch_rate', ascending=True)
        
        if len(low_unmatch_months) > 0:
            print("\nMonths with <50% unmatched rate:")
            for _, row in low_unmatch_months.head(5).iterrows():
                print(f"  {row['year_month_display']}: {row['unmatch_rate']:.1f}% unmatched ({row['unmatched_count']:,} embryos)")
        
        # 4. Date range analysis
        print(f"\n4. DATE RANGE ANALYSIS")
        print("-" * 50)
        
        earliest_date = all_embryos_df['embryo_FertilizationTime'].min()
        latest_date = all_embryos_df['embryo_FertilizationTime'].max()
        
        earliest_matched = matched_df['embryo_FertilizationTime'].min()
        latest_matched = matched_df['embryo_FertilizationTime'].max()
        
        earliest_unmatched = unmatched_df['embryo_FertilizationTime'].min()
        latest_unmatched = unmatched_df['embryo_FertilizationTime'].max()
        
        print(f"Overall date range: {earliest_date} to {latest_date}")
        print(f"Matched embryos range: {earliest_matched} to {latest_matched}")
        print(f"Unmatched embryos range: {earliest_unmatched} to {latest_unmatched}")
        
        # 5. Yearly analysis
        print(f"\n5. YEARLY ANALYSIS")
        print("-" * 50)
        
        # Add year column for yearly analysis
        monthly_analysis['year'] = monthly_analysis['year_month'].dt.year
        
        # Yearly aggregation
        yearly_analysis = monthly_analysis.groupby('year').agg({
            'total_count': 'sum',
            'matched_count': 'sum',
            'unmatched_count': 'sum'
        }).reset_index()
        
        # Calculate yearly percentages
        yearly_analysis['match_rate'] = (yearly_analysis['matched_count'] / yearly_analysis['total_count'] * 100).round(1)
        yearly_analysis['unmatch_rate'] = (yearly_analysis['unmatched_count'] / yearly_analysis['total_count'] * 100).round(1)
        
        # Sort by year descending
        yearly_analysis = yearly_analysis.sort_values('year', ascending=False)
        
        print("Yearly breakdown of Embryoscope embryos:")
        print(f"{'Year':<8} {'Total':<8} {'Matched':<8} {'Unmatched':<10} {'Match%':<8} {'Unmatch%':<8}")
        print("-" * 60)
        
        for _, row in yearly_analysis.iterrows():
            print(f"{row['year']:<8} {row['total_count']:<8} {row['matched_count']:<8} {row['unmatched_count']:<10} {row['match_rate']:<8} {row['unmatch_rate']:<8}")
        
        # 6. Recent months analysis (last 6 months)
        print(f"\n6. RECENT MONTHS ANALYSIS (Last 6 months)")
        print("-" * 50)
        
        recent_months = monthly_analysis.head(6)
        recent_total = recent_months['total_count'].sum()
        recent_matched = recent_months['matched_count'].sum()
        recent_unmatched = recent_months['unmatched_count'].sum()
        
        print(f"Last 6 months - Total: {recent_total:,}, Matched: {recent_matched:,} ({recent_matched/recent_total*100:.1f}%), Unmatched: {recent_unmatched:,} ({recent_unmatched/recent_total*100:.1f}%)")
        
        print("\nRecent months breakdown:")
        for _, row in recent_months.iterrows():
            print(f"  {row['year_month_display']}: {row['total_count']:,} total, {row['matched_count']:,} matched ({row['match_rate']:.1f}%), {row['unmatched_count']:,} unmatched ({row['unmatch_rate']:.1f}%)")
        
        # 7. Full history summary
        print(f"\n7. FULL HISTORY SUMMARY")
        print("-" * 50)
        
        # Calculate overall statistics
        total_embryos = len(all_embryos_df)
        total_matched = len(matched_df)
        total_unmatched = len(unmatched_df)
        
        # Calculate by time periods
        recent_years = yearly_analysis[yearly_analysis['year'] >= 2022]
        historical_years = yearly_analysis[yearly_analysis['year'] < 2022]
        
        recent_total_embryos = recent_years['total_count'].sum()
        recent_matched_embryos = recent_years['matched_count'].sum()
        recent_unmatched_embryos = recent_years['unmatched_count'].sum()
        
        historical_total_embryos = historical_years['total_count'].sum()
        historical_matched_embryos = historical_years['matched_count'].sum()
        historical_unmatched_embryos = historical_years['unmatched_count'].sum()
        
        print(f"OVERALL STATISTICS:")
        print(f"  Total Embryoscope embryos: {total_embryos:,}")
        print(f"  Total matched embryos: {total_matched:,} ({total_matched/total_embryos*100:.1f}%)")
        print(f"  Total unmatched embryos: {total_unmatched:,} ({total_unmatched/total_embryos*100:.1f}%)")
        
        print(f"\nRECENT PERIOD (2022-2025):")
        print(f"  Total embryos: {recent_total_embryos:,}")
        print(f"  Matched embryos: {recent_matched_embryos:,} ({recent_matched_embryos/recent_total_embryos*100:.1f}%)")
        print(f"  Unmatched embryos: {recent_unmatched_embryos:,} ({recent_unmatched_embryos/recent_total_embryos*100:.1f}%)")
        
        print(f"\nHISTORICAL PERIOD (2000-2021):")
        print(f"  Total embryos: {historical_total_embryos:,}")
        print(f"  Matched embryos: {historical_matched_embryos:,} ({historical_matched_embryos/historical_total_embryos*100:.1f}%)")
        print(f"  Unmatched embryos: {historical_unmatched_embryos:,} ({historical_unmatched_embryos/historical_total_embryos*100:.1f}%)")
        
        # 8. Detailed year table
        print(f"\n8. DETAILED YEAR TABLE")
        print("-" * 80)
        
        print("Complete yearly breakdown with detailed statistics:")
        print(f"{'Year':<6} {'Total':<8} {'Matched':<8} {'Unmatched':<10} {'Match%':<8} {'Unmatch%':<8} {'Status':<15}")
        print("-" * 80)
        
        for _, row in yearly_analysis.iterrows():
            year = row['year']
            total = row['total_count']
            matched = row['matched_count']
            unmatched = row['unmatched_count']
            match_rate = row['match_rate']
            unmatch_rate = row['unmatch_rate']
            
            # Determine status
            if match_rate >= 80:
                status = "Excellent"
            elif match_rate >= 60:
                status = "Good"
            elif match_rate >= 40:
                status = "Fair"
            elif match_rate >= 20:
                status = "Poor"
            else:
                status = "Very Poor"
            
            print(f"{year:<6} {total:<8} {matched:<8} {unmatched:<10} {match_rate:<8} {unmatch_rate:<8} {status:<15}")
        
        # 9. Performance trends
        print(f"\n9. PERFORMANCE TRENDS")
        print("-" * 50)
        
        # Find best performing years
        best_years = yearly_analysis[yearly_analysis['match_rate'] >= 80].sort_values('match_rate', ascending=False)
        if len(best_years) > 0:
            print("Best performing years (≥80% match rate):")
            for _, row in best_years.iterrows():
                print(f"  {row['year']}: {row['match_rate']:.1f}% matched ({row['matched_count']:,} embryos)")
        
        # Find worst performing years
        worst_years = yearly_analysis[yearly_analysis['match_rate'] < 20].sort_values('match_rate', ascending=True)
        if len(worst_years) > 0:
            print("\nWorst performing years (<20% match rate):")
            for _, row in worst_years.iterrows():
                print(f"  {row['year']}: {row['match_rate']:.1f}% matched ({row['matched_count']:,} embryos)")
        
        # Find transition years
        transition_years = yearly_analysis[(yearly_analysis['match_rate'] >= 20) & (yearly_analysis['match_rate'] < 80)].sort_values('year', ascending=False)
        if len(transition_years) > 0:
            print("\nTransition years (20-80% match rate):")
            for _, row in transition_years.iterrows():
                print(f"  {row['year']}: {row['match_rate']:.1f}% matched ({row['matched_count']:,} embryos)")
        
        con.close()
        logger.info("Monthly analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    analyze_unmatched_by_month() 