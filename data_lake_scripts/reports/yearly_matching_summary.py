import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def yearly_matching_summary():
    """Analyze yearly matching summary across all years."""
    
    logger.info("Starting yearly matching summary analysis")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("YEARLY MATCHING SUMMARY ANALYSIS")
        print("=" * 80)
        
        # Get yearly statistics for all years
        logger.info("Fetching yearly statistics...")
        yearly_stats_query = """
        WITH embryoscope_yearly AS (
            SELECT 
                YEAR(CAST(embryo_FertilizationTime AS DATE)) as year,
                COUNT(DISTINCT embryo_EmbryoID) as total_embryos
            FROM gold.embryoscope_embrioes
            WHERE embryo_FertilizationTime IS NOT NULL
            GROUP BY YEAR(CAST(embryo_FertilizationTime AS DATE))
        ),
        matched_yearly AS (
            SELECT 
                YEAR(CAST(embryo_FertilizationTime AS DATE)) as year,
                COUNT(DISTINCT embryo_EmbryoID) as matched_embryos
            FROM gold.embryoscope_clinisys_combined
            WHERE embryo_FertilizationTime IS NOT NULL
            GROUP BY YEAR(CAST(embryo_FertilizationTime AS DATE))
        ),
        unmatched_yearly AS (
            SELECT 
                YEAR(CAST(embryo_FertilizationTime AS DATE)) as year,
                COUNT(DISTINCT embryo_EmbryoID) as unmatched_embryos
            FROM gold.embryoscope_embrioes
            WHERE embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            AND embryo_FertilizationTime IS NOT NULL
            GROUP BY YEAR(CAST(embryo_FertilizationTime AS DATE))
        )
        SELECT 
            e.year,
            e.total_embryos,
            COALESCE(m.matched_embryos, 0) as matched_embryos,
            COALESCE(u.unmatched_embryos, 0) as unmatched_embryos,
            CASE 
                WHEN e.total_embryos > 0 THEN 
                    ROUND(COALESCE(m.matched_embryos, 0) * 100.0 / e.total_embryos, 1)
                ELSE 0 
            END as match_rate_percent
        FROM embryoscope_yearly e
        LEFT JOIN matched_yearly m ON e.year = m.year
        LEFT JOIN unmatched_yearly u ON e.year = u.year
        ORDER BY e.year DESC
        """
        
        yearly_stats_df = con.execute(yearly_stats_query).df()
        logger.info(f"Found data for {len(yearly_stats_df)} years")
        
        # Display yearly summary
        print(f"\n1. YEARLY MATCHING SUMMARY")
        print("-" * 80)
        print(f"{'Year':<6} {'Total':<8} {'Matched':<10} {'Unmatched':<12} {'Match Rate':<12}")
        print("-" * 80)
        
        total_embryos_all = 0
        total_matched_all = 0
        total_unmatched_all = 0
        
        for _, row in yearly_stats_df.iterrows():
            year = row['year']
            total = row['total_embryos']
            matched = row['matched_embryos']
            unmatched = row['unmatched_embryos']
            match_rate = row['match_rate_percent']
            
            print(f"{year:<6} {total:<8,} {matched:<10,} {unmatched:<12,} {match_rate:<12.1f}%")
            
            total_embryos_all += total
            total_matched_all += matched
            total_unmatched_all += unmatched
        
        # Overall summary
        overall_match_rate = (total_matched_all / total_embryos_all * 100) if total_embryos_all > 0 else 0
        print("-" * 80)
        print(f"{'TOTAL':<6} {total_embryos_all:<8,} {total_matched_all:<10,} {total_unmatched_all:<12,} {overall_match_rate:<12.1f}%")
        
        # 2. Performance categorization
        print(f"\n2. PERFORMANCE CATEGORIZATION BY YEAR")
        print("-" * 50)
        
        def categorize_performance(match_rate):
            if match_rate >= 90:
                return "Excellent"
            elif match_rate >= 80:
                return "Good"
            elif match_rate >= 60:
                return "Fair"
            elif match_rate >= 40:
                return "Poor"
            else:
                return "Very Poor"
        
        yearly_stats_df['performance'] = yearly_stats_df['match_rate_percent'].apply(categorize_performance)
        
        performance_counts = yearly_stats_df['performance'].value_counts()
        print("Performance distribution:")
        for performance, count in performance_counts.items():
            print(f"  {performance}: {count} years")
        
        # 3. Recent vs Historical analysis
        print(f"\n3. RECENT VS HISTORICAL ANALYSIS")
        print("-" * 50)
        
        # Define recent years (2022-2025) vs historical (pre-2022)
        recent_years = yearly_stats_df[yearly_stats_df['year'] >= 2022]
        historical_years = yearly_stats_df[yearly_stats_df['year'] < 2022]
        
        if len(recent_years) > 0:
            recent_total = recent_years['total_embryos'].sum()
            recent_matched = recent_years['matched_embryos'].sum()
            recent_rate = (recent_matched / recent_total * 100) if recent_total > 0 else 0
            print(f"Recent years (2022-2025):")
            print(f"  Total embryos: {recent_total:,}")
            print(f"  Matched embryos: {recent_matched:,}")
            print(f"  Match rate: {recent_rate:.1f}%")
        
        if len(historical_years) > 0:
            historical_total = historical_years['total_embryos'].sum()
            historical_matched = historical_years['matched_embryos'].sum()
            historical_rate = (historical_matched / historical_total * 100) if historical_total > 0 else 0
            print(f"Historical years (pre-2022):")
            print(f"  Total embryos: {historical_total:,}")
            print(f"  Matched embryos: {historical_matched:,}")
            print(f"  Match rate: {historical_rate:.1f}%")
        
        # 4. Best and worst performing years
        print(f"\n4. BEST AND WORST PERFORMING YEARS")
        print("-" * 50)
        
        best_year = yearly_stats_df.loc[yearly_stats_df['match_rate_percent'].idxmax()]
        worst_year = yearly_stats_df.loc[yearly_stats_df['match_rate_percent'].idxmin()]
        
        print(f"Best performing year: {best_year['year']}")
        print(f"  Match rate: {best_year['match_rate_percent']:.1f}%")
        print(f"  Total embryos: {best_year['total_embryos']:,}")
        print(f"  Matched embryos: {best_year['matched_embryos']:,}")
        
        print(f"\nWorst performing year: {worst_year['year']}")
        print(f"  Match rate: {worst_year['match_rate_percent']:.1f}%")
        print(f"  Total embryos: {worst_year['total_embryos']:,}")
        print(f"  Matched embryos: {worst_year['matched_embryos']:,}")
        
        # 5. Trend analysis
        print(f"\n5. TREND ANALYSIS")
        print("-" * 50)
        
        if len(yearly_stats_df) >= 2:
            # Calculate year-over-year changes
            yearly_stats_df_sorted = yearly_stats_df.sort_values('year')
            yearly_stats_df_sorted['match_rate_change'] = yearly_stats_df_sorted['match_rate_percent'].diff()
            
            print("Year-over-year match rate changes:")
            for _, row in yearly_stats_df_sorted.iterrows():
                if pd.notna(row['match_rate_change']):
                    change = row['match_rate_change']
                    direction = "↑" if change > 0 else "↓" if change < 0 else "="
                    print(f"  {row['year']}: {change:+.1f}% {direction}")
        
        # 6. Detailed breakdown for recent years
        print(f"\n6. DETAILED BREAKDOWN FOR RECENT YEARS (2022-2025)")
        print("-" * 50)
        
        recent_detailed = yearly_stats_df[yearly_stats_df['year'] >= 2022].copy()
        if len(recent_detailed) > 0:
            print(f"{'Year':<6} {'Total':<8} {'Matched':<10} {'Unmatched':<12} {'Match Rate':<12} {'Performance':<12}")
            print("-" * 80)
            
            for _, row in recent_detailed.iterrows():
                year = row['year']
                total = row['total_embryos']
                matched = row['matched_embryos']
                unmatched = row['unmatched_embryos']
                match_rate = row['match_rate_percent']
                performance = row['performance']
                
                print(f"{year:<6} {total:<8,} {matched:<10,} {unmatched:<12,} {match_rate:<12.1f}% {performance:<12}")
        
        # 7. Summary insights
        print(f"\n7. SUMMARY INSIGHTS")
        print("-" * 50)
        
        print(f"• Overall match rate: {overall_match_rate:.1f}%")
        print(f"• Total embryos analyzed: {total_embryos_all:,}")
        print(f"• Total matched embryos: {total_matched_all:,}")
        print(f"• Total unmatched embryos: {total_unmatched_all:,}")
        
        if len(yearly_stats_df) > 0:
            avg_match_rate = yearly_stats_df['match_rate_percent'].mean()
            print(f"• Average yearly match rate: {avg_match_rate:.1f}%")
            
            if len(recent_years) > 0 and len(historical_years) > 0:
                print(f"• Recent years (2022-2025) average: {recent_years['match_rate_percent'].mean():.1f}%")
                print(f"• Historical years average: {historical_years['match_rate_percent'].mean():.1f}%")
        
        con.close()
        logger.info("Yearly matching summary analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    yearly_matching_summary() 