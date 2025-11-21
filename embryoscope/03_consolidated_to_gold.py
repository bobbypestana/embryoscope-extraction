import duckdb
import os
import logging
from datetime import datetime
import yaml

# Load config and logging level
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'params.yml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)
logging_level_str = config.get('logging_level', 'INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')
logging.basicConfig(
    level=logging_level,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f'Loaded logging level: {logging_level_str}')

# Database paths - read from huntington_data_lake, write to huntington_data_lake
source_db_path = r'G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'
target_db_path = r'G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'

def get_available_columns(con, schema, table):
    """Get list of available columns from a table"""
    try:
        result = con.execute(f"DESCRIBE {schema}.{table}").df()
        return result['column_name'].tolist()
    except Exception as e:
        logger.warning(f"Error getting columns from {schema}.{table}: {e}")
        return []

def quote_column_name(column_name):
    """Quote column name if it contains special characters"""
    if any(char in column_name for char in ['-', '+', ' ', '.', '/']):
        return f'"{column_name}"'
    return column_name

def build_dynamic_query(con):
    """Build SELECT query dynamically based on available columns"""
    logger.info('Building dynamic query based on available columns...')
    
    # Get available columns from each table
    patient_cols = get_available_columns(con, 'silver_embryoscope', 'patients')
    treatment_cols = get_available_columns(con, 'silver_embryoscope', 'treatments')
    embryo_cols = get_available_columns(con, 'silver_embryoscope', 'embryo_data')
    
    logger.info(f'Found {len(patient_cols)} columns in patients table')
    logger.info(f'Found {len(treatment_cols)} columns in treatments table')
    logger.info(f'Found {len(embryo_cols)} columns in embryo_data table')
    
    select_parts = []
    
    # Patient columns (with patient_ prefix, except prontuario)
    patient_prefix_map = {
        'prontuario': 'prontuario',  # No prefix for prontuario
        'PatientIDx': 'patient_PatientIDx',
        'PatientID': 'patient_PatientID',
        'FirstName': 'patient_FirstName',
        'LastName': 'patient_LastName',
        'DateOfBirth': 'patient_DateOfBirth',
        'unit_huntington': 'patient_unit_huntington'
    }
    
    for col in patient_cols:
        if col in patient_prefix_map:
            alias = patient_prefix_map[col]
            quoted_col = quote_column_name(col)
            select_parts.append(f'        p.{quoted_col} as {alias}')
        elif not col.startswith('_'):  # Skip metadata columns
            # Default prefix for other patient columns
            quoted_col = quote_column_name(col)
            select_parts.append(f'        p.{quoted_col} as patient_{col}')
    
    # Treatment columns (with treatment_ prefix)
    treatment_prefix_map = {
        'TreatmentName': 'treatment_TreatmentName'
    }
    
    for col in treatment_cols:
        if col in treatment_prefix_map:
            alias = treatment_prefix_map[col]
            quoted_col = quote_column_name(col)
            select_parts.append(f'        t.{quoted_col} as {alias}')
        elif not col.startswith('_'):  # Skip metadata columns
            # Default prefix for other treatment columns
            quoted_col = quote_column_name(col)
            select_parts.append(f'        t.{quoted_col} as treatment_{col}')
    
    # Embryo data columns - separate into regular columns and annotation columns
    # Regular embryo columns (non-annotation)
    regular_embryo_cols = [
        'EmbryoID', 'KIDDate', 'KIDScore', 'KIDUser', 'KIDVersion',
        'Description', 'EmbryoDescriptionID', 'EmbryoFate', 'FertilizationMethod',
        'FertilizationTime', 'InstrumentNumber', 'Position', 'WellNumber',
        'embryo_number', 'unit_huntington'
    ]
    
    # Add regular embryo columns first
    for col in regular_embryo_cols:
        if col in embryo_cols:
            quoted_col = quote_column_name(col)
            select_parts.append(f'        ed.{quoted_col} as embryo_{col}')
    
    # Annotation columns - group by annotation type
    # Pattern: Name_*, Time_*, Timestamp_*, Value_*
    annotation_types = set()
    for col in embryo_cols:
        if col.startswith('Name_'):
            # Extract annotation type (e.g., "Name_Comment" -> "Comment")
            ann_type = col.replace('Name_', '')
            annotation_types.add(ann_type)
    
    # Sort annotation types for consistent ordering
    sorted_annotation_types = sorted(annotation_types)
    
    # Add annotation columns grouped by type
    for ann_type in sorted_annotation_types:
        for prefix in ['Name', 'Time', 'Timestamp', 'Value']:
            col = f'{prefix}_{ann_type}'
            if col in embryo_cols:
                quoted_col = quote_column_name(col)
                # Clean alias name (replace special chars)
                clean_alias = ann_type.replace('-', '_').replace('+', '_')
                select_parts.append(f'        ed.{quoted_col} as embryo_{prefix}_{clean_alias}')
    
    # Add any remaining embryo columns that weren't caught above
    processed_embryo_cols = set(regular_embryo_cols)
    for ann_type in annotation_types:
        for prefix in ['Name', 'Time', 'Timestamp', 'Value']:
            processed_embryo_cols.add(f'{prefix}_{ann_type}')
    
    for col in embryo_cols:
        if col not in processed_embryo_cols and not col.startswith('_'):
            quoted_col = quote_column_name(col)
            select_parts.append(f'        ed.{quoted_col} as embryo_{col}')
    
    # Build the complete query
    if not select_parts:
        raise ValueError('No columns found to select. Check if source tables exist and have columns.')
    
    select_clause = ',\n'.join(select_parts)
    
    query = f'''
    SELECT 
{select_clause}
    FROM silver_embryoscope.patients p 
    LEFT JOIN silver_embryoscope.treatments t 
        ON p.PatientIDx = t.PatientIDx 
    LEFT JOIN silver_embryoscope.embryo_data ed 
        ON ed.PatientIDx = p.PatientIDx and ed.TreatmentName = t.TreatmentName
    WHERE ed.EmbryoID IS NOT NULL
    ORDER BY p.PatientIDx DESC
    '''
    
    logger.info(f'Built dynamic query with {len(select_parts)} columns')
    logger.debug(f'Query preview (first 500 chars): {query[:500]}...')
    return query

def main():
    logger.info('Starting embryoscope gold loader for embryoscope_embrioes')
    logger.info(f'Source database path: {source_db_path}')
    logger.info(f'Target database path: {target_db_path}')
    
    # Verify source database exists
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
    
    try:
        # Read data from source database
        logger.info('Connecting to source database to read data')
        with duckdb.connect(source_db_path) as source_con:
            logger.info('Connected to source DuckDB')
            
            # Build dynamic query based on available columns
            query = build_dynamic_query(source_con)
            
            # Execute query to get data
            df = source_con.execute(query).df()
            logger.info(f'Read {len(df)} rows from source database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in source database')
                return
            
            # Column filtering and cleanup
            logger.info('Applying column filtering and cleanup...')
            
            # Since we're explicitly selecting columns in the SQL query, 
            # we don't need to remove metadata columns as they're not included
            logger.info(f'Final column count: {len(df.columns)}')
            
        # Write data to target database
        logger.info('Connecting to target database to write data')
        with duckdb.connect(target_db_path) as target_con:
            logger.info('Connected to target DuckDB')
            
            # Create schema if not exists
            target_con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
            logger.info('Ensured gold schema exists')
            
            # Drop existing table if it exists
            target_con.execute('DROP TABLE IF EXISTS gold.embryoscope_embrioes;')
            logger.info('Dropped existing gold.embryoscope_embrioes table if it existed')
            
            # Register DataFrame and create table from it
            target_con.register('df', df)
            target_con.execute('CREATE TABLE gold.embryoscope_embrioes AS SELECT * FROM df')
            target_con.unregister('df')
            logger.info('Created gold.embryoscope_embrioes table')
            
            # Create indexes for better JOIN performance
            logger.info('Creating indexes for better JOIN performance...')
            
            # Get available columns to check if index columns exist
            available_cols = target_con.execute("DESCRIBE gold.embryoscope_embrioes").df()['column_name'].tolist()
            
            index_queries = []
            if 'embryo_FertilizationTime' in available_cols:
                index_queries.append("CREATE INDEX IF NOT EXISTS idx_embryoscope_fertilization_time ON gold.embryoscope_embrioes(embryo_FertilizationTime)")
            if 'patient_PatientID' in available_cols:
                index_queries.append("CREATE INDEX IF NOT EXISTS idx_embryoscope_patient_id ON gold.embryoscope_embrioes(patient_PatientID)")
            if 'embryo_EmbryoID' in available_cols:
                index_queries.append("CREATE INDEX IF NOT EXISTS idx_embryoscope_embryo_id ON gold.embryoscope_embrioes(embryo_EmbryoID)")
            
            for idx_query in index_queries:
                try:
                    target_con.execute(idx_query)
                except Exception as e:
                    logger.warning(f'Index creation failed (may already exist): {e}')
            
            logger.info('Indexes created successfully')
            
            # Validate row count
            row_count = target_con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            logger.info(f'gold.embryoscope_embrioes row count: {row_count}')
            
    except Exception as e:
        logger.error(f'Error in embryoscope gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Embryoscope gold loader finished successfully')

if __name__ == '__main__':
    main() 