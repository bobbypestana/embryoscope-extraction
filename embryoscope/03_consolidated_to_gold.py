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

def main():
    logger.info('Starting embryoscope gold loader for embryoscope_embrioes')
    logger.info(f'Source database path: {source_db_path}')
    logger.info(f'Target database path: {target_db_path}')
    
    # Verify source database exists
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
    
    query = '''
    SELECT 
        -- Patient columns (with prefix)
        p.prontuario as prontuario,
        p.PatientIDx as patient_PatientIDx,
        p.PatientID as patient_PatientID,
        p.FirstName as patient_FirstName,
        p.LastName as patient_LastName,
        p.DateOfBirth as patient_DateOfBirth,
        p.unit_huntington as patient_unit_huntington,
        
        -- Treatment columns (with prefix)
        t.TreatmentName as treatment_TreatmentName,
        
        -- Embryo data columns (with prefix) - all non-duplicated columns
        ed.EmbryoID as embryo_EmbryoID,
        ed.KIDDate as embryo_KIDDate,
        ed.KIDScore as embryo_KIDScore,
        ed.KIDUser as embryo_KIDUser,
        ed.KIDVersion as embryo_KIDVersion,
        ed.Description as embryo_Description,
        ed.EmbryoDescriptionID as embryo_EmbryoDescriptionID,
        ed.EmbryoFate as embryo_EmbryoFate,
        ed.FertilizationMethod as embryo_FertilizationMethod,
        ed.FertilizationTime as embryo_FertilizationTime,
        ed.InstrumentNumber as embryo_InstrumentNumber,
        ed.Position as embryo_Position,
        ed.WellNumber as embryo_WellNumber,
        ed.embryo_number as embryo_embryo_number,
        ed.unit_huntington as embryo_unit_huntington,
        
        -- Annotation columns grouped by annotation type
        
        -- BlastExpandLast annotation
        ed.Name_BlastExpandLast as embryo_Name_BlastExpandLast,
        ed.Time_BlastExpandLast as embryo_Time_BlastExpandLast,
        ed.Timestamp_BlastExpandLast as embryo_Timestamp_BlastExpandLast,
        ed.Value_BlastExpandLast as embryo_Value_BlastExpandLast,
        
        -- Comment annotation
        ed.Name_Comment as embryo_Name_Comment,
        ed.Time_Comment as embryo_Time_Comment,
        ed.Timestamp_Comment as embryo_Timestamp_Comment,
        ed.Value_Comment as embryo_Value_Comment,
        
        -- DynamicScore annotation
        ed.Name_DynamicScore as embryo_Name_DynamicScore,
        ed.Time_DynamicScore as embryo_Time_DynamicScore,
        ed.Timestamp_DynamicScore as embryo_Timestamp_DynamicScore,
        ed.Value_DynamicScore as embryo_Value_DynamicScore,
        
        -- EVEN annotations
        ed.Name_EVEN2 as embryo_Name_EVEN2,
        ed.Time_EVEN2 as embryo_Time_EVEN2,
        ed.Timestamp_EVEN2 as embryo_Timestamp_EVEN2,
        ed.Value_EVEN2 as embryo_Value_EVEN2,
        ed.Name_EVEN4 as embryo_Name_EVEN4,
        ed.Time_EVEN4 as embryo_Time_EVEN4,
        ed.Timestamp_EVEN4 as embryo_Timestamp_EVEN4,
        ed.Value_EVEN4 as embryo_Value_EVEN4,
        ed.Name_EVEN8 as embryo_Name_EVEN8,
        ed.Time_EVEN8 as embryo_Time_EVEN8,
        ed.Timestamp_EVEN8 as embryo_Timestamp_EVEN8,
        ed.Value_EVEN8 as embryo_Value_EVEN8,
        
        -- Ellipse annotation
        ed.Name_Ellipse as embryo_Name_Ellipse,
        ed.Time_Ellipse as embryo_Time_Ellipse,
        ed.Timestamp_Ellipse as embryo_Timestamp_Ellipse,
        ed.Value_Ellipse as embryo_Value_Ellipse,
        
        -- FRAG annotations
        ed.Name_FRAG2 as embryo_Name_FRAG2,
        ed.Time_FRAG2 as embryo_Time_FRAG2,
        ed.Timestamp_FRAG2 as embryo_Timestamp_FRAG2,
        ed.Value_FRAG2 as embryo_Value_FRAG2,
        ed.Name_FRAG2CAT as embryo_Name_FRAG2CAT,
        ed.Time_FRAG2CAT as embryo_Time_FRAG2CAT,
        ed.Timestamp_FRAG2CAT as embryo_Timestamp_FRAG2CAT,
        ed.Value_FRAG2CAT as embryo_Value_FRAG2CAT,
        ed.Name_FRAG4 as embryo_Name_FRAG4,
        ed.Time_FRAG4 as embryo_Time_FRAG4,
        ed.Timestamp_FRAG4 as embryo_Timestamp_FRAG4,
        ed.Value_FRAG4 as embryo_Value_FRAG4,
        ed.Name_FRAG8 as embryo_Name_FRAG8,
        ed.Time_FRAG8 as embryo_Time_FRAG8,
        ed.Timestamp_FRAG8 as embryo_Timestamp_FRAG8,
        ed.Value_FRAG8 as embryo_Value_FRAG8,
        
        -- ICM annotation
        ed.Name_ICM as embryo_Name_ICM,
        ed.Time_ICM as embryo_Time_ICM,
        ed.Timestamp_ICM as embryo_Timestamp_ICM,
        ed.Value_ICM as embryo_Value_ICM,
        
        -- Line annotation
        ed.Name_Line as embryo_Name_Line,
        ed.Time_Line as embryo_Time_Line,
        ed.Timestamp_Line as embryo_Timestamp_Line,
        ed.Value_Line as embryo_Value_Line,
        
        -- MN2Type annotation
        ed.Name_MN2Type as embryo_Name_MN2Type,
        ed.Time_MN2Type as embryo_Time_MN2Type,
        ed.Timestamp_MN2Type as embryo_Timestamp_MN2Type,
        ed.Value_MN2Type as embryo_Value_MN2Type,
        
        -- MN4Type annotation
        ed.Name_MN4Type as embryo_Name_MN4Type,
        ed.Time_MN4Type as embryo_Time_MN4Type,
        ed.Timestamp_MN4Type as embryo_Timestamp_MN4Type,
        ed.Value_MN4Type as embryo_Value_MN4Type,
        
        -- MorphologicalGrade annotations
        ed.Name_MorphologicalGrade as embryo_Name_MorphologicalGrade,
        ed.Time_MorphologicalGrade as embryo_Time_MorphologicalGrade,
        ed.Timestamp_MorphologicalGrade as embryo_Timestamp_MorphologicalGrade,
        ed.Value_MorphologicalGrade as embryo_Value_MorphologicalGrade,
        ed.Name_MorphologicalGradeD5 as embryo_Name_MorphologicalGradeD5,
        ed.Time_MorphologicalGradeD5 as embryo_Time_MorphologicalGradeD5,
        ed.Timestamp_MorphologicalGradeD5 as embryo_Timestamp_MorphologicalGradeD5,
        ed.Value_MorphologicalGradeD5 as embryo_Value_MorphologicalGradeD5,
        
        -- Nuclei annotations
        ed.Name_Nuclei2 as embryo_Name_Nuclei2,
        ed.Time_Nuclei2 as embryo_Time_Nuclei2,
        ed.Timestamp_Nuclei2 as embryo_Timestamp_Nuclei2,
        ed.Value_Nuclei2 as embryo_Value_Nuclei2,
        ed.Name_Nuclei4 as embryo_Name_Nuclei4,
        ed.Time_Nuclei4 as embryo_Time_Nuclei4,
        ed.Timestamp_Nuclei4 as embryo_Timestamp_Nuclei4,
        ed.Value_Nuclei4 as embryo_Value_Nuclei4,
        ed.Name_Nuclei8 as embryo_Name_Nuclei8,
        ed.Time_Nuclei8 as embryo_Time_Nuclei8,
        ed.Timestamp_Nuclei8 as embryo_Timestamp_Nuclei8,
        ed.Value_Nuclei8 as embryo_Value_Nuclei8,
        ed.Name_Nuclei as embryo_Name_Nuclei,
        ed.Time_Nuclei as embryo_Time_Nuclei,
        ed.Timestamp_Nuclei as embryo_Timestamp_Nuclei,
        ed.Value_Nuclei as embryo_Value_Nuclei,
        
        -- PN annotation
        ed.Name_PN as embryo_Name_PN,
        ed.Time_PN as embryo_Time_PN,
        ed.Timestamp_PN as embryo_Timestamp_PN,
        ed.Value_PN as embryo_Value_PN,
        
        -- Pulsing annotation
        ed.Name_Pulsing as embryo_Name_Pulsing,
        ed.Time_Pulsing as embryo_Time_Pulsing,
        ed.Timestamp_Pulsing as embryo_Timestamp_Pulsing,
        ed.Value_Pulsing as embryo_Value_Pulsing,
        
        -- Strings annotation
        ed.Name_Strings as embryo_Name_Strings,
        ed.Time_Strings as embryo_Time_Strings,
        ed.Timestamp_Strings as embryo_Timestamp_Strings,
        ed.Value_Strings as embryo_Value_Strings,
        
        -- TE annotation
        ed.Name_TE as embryo_Name_TE,
        ed.Time_TE as embryo_Time_TE,
        ed.Timestamp_TE as embryo_Timestamp_TE,
        ed.Value_TE as embryo_Value_TE,
        
        -- Text annotation
        ed.Name_Text as embryo_Name_Text,
        ed.Time_Text as embryo_Time_Text,
        ed.Timestamp_Text as embryo_Timestamp_Text,
        ed.Value_Text as embryo_Value_Text,
        
        -- ZScore annotation
        ed.Name_ZScore as embryo_Name_ZScore,
        ed.Time_ZScore as embryo_Time_ZScore,
        ed.Timestamp_ZScore as embryo_Timestamp_ZScore,
        ed.Value_ZScore as embryo_Value_ZScore,
        
        -- Time point annotations (t2-t9)
        ed.Name_t2 as embryo_Name_t2,
        ed.Time_t2 as embryo_Time_t2,
        ed.Timestamp_t2 as embryo_Timestamp_t2,
        ed.Value_t2 as embryo_Value_t2,
        ed.Name_t3 as embryo_Name_t3,
        ed.Time_t3 as embryo_Time_t3,
        ed.Timestamp_t3 as embryo_Timestamp_t3,
        ed.Value_t3 as embryo_Value_t3,
        ed.Name_t4 as embryo_Name_t4,
        ed.Time_t4 as embryo_Time_t4,
        ed.Timestamp_t4 as embryo_Timestamp_t4,
        ed.Value_t4 as embryo_Value_t4,
        ed.Name_t5 as embryo_Name_t5,
        ed.Time_t5 as embryo_Time_t5,
        ed.Timestamp_t5 as embryo_Timestamp_t5,
        ed.Value_t5 as embryo_Value_t5,
        ed.Name_t6 as embryo_Name_t6,
        ed.Time_t6 as embryo_Time_t6,
        ed.Timestamp_t6 as embryo_Timestamp_t6,
        ed.Value_t6 as embryo_Value_t6,
        ed.Name_t7 as embryo_Name_t7,
        ed.Time_t7 as embryo_Time_t7,
        ed.Timestamp_t7 as embryo_Timestamp_t7,
        ed.Value_t7 as embryo_Value_t7,
        ed.Name_t8 as embryo_Name_t8,
        ed.Time_t8 as embryo_Time_t8,
        ed.Timestamp_t8 as embryo_Timestamp_t8,
        ed.Value_t8 as embryo_Value_t8,
        ed.Name_t9 as embryo_Name_t9,
        ed.Time_t9 as embryo_Time_t9,
        ed.Timestamp_t9 as embryo_Timestamp_t9,
        ed.Value_t9 as embryo_Value_t9,
        
        -- Special time annotations
        ed.Name_tB as embryo_Name_tB,
        ed.Time_tB as embryo_Time_tB,
        ed.Timestamp_tB as embryo_Timestamp_tB,
        ed.Value_tB as embryo_Value_tB,
        ed.Name_tEB as embryo_Name_tEB,
        ed.Time_tEB as embryo_Time_tEB,
        ed.Timestamp_tEB as embryo_Timestamp_tEB,
        ed.Value_tEB as embryo_Value_tEB,
        ed.Name_tHB as embryo_Name_tHB,
        ed.Time_tHB as embryo_Time_tHB,
        ed.Timestamp_tHB as embryo_Timestamp_tHB,
        ed.Value_tHB as embryo_Value_tHB,
        ed.Name_tM as embryo_Name_tM,
        ed.Time_tM as embryo_Time_tM,
        ed.Timestamp_tM as embryo_Timestamp_tM,
        ed.Value_tM as embryo_Value_tM,
        ed.Name_tPB2 as embryo_Name_tPB2,
        ed.Time_tPB2 as embryo_Time_tPB2,
        ed.Timestamp_tPB2 as embryo_Timestamp_tPB2,
        ed.Value_tPB2 as embryo_Value_tPB2,
        ed.Name_tPNa as embryo_Name_tPNa,
        ed.Time_tPNa as embryo_Time_tPNa,
        ed.Timestamp_tPNa as embryo_Timestamp_tPNa,
        ed.Value_tPNa as embryo_Value_tPNa,
        ed.Name_tPNf as embryo_Name_tPNf,
        ed.Time_tPNf as embryo_Time_tPNf,
        ed.Timestamp_tPNf as embryo_Timestamp_tPNf,
        ed.Value_tPNf as embryo_Value_tPNf,
        ed.Name_tSB as embryo_Name_tSB,
        ed.Time_tSB as embryo_Time_tSB,
        ed.Timestamp_tSB as embryo_Timestamp_tSB,
        ed.Value_tSB as embryo_Value_tSB,
        ed.Name_tSC as embryo_Name_tSC,
        ed.Time_tSC as embryo_Time_tSC,
        ed.Timestamp_tSC as embryo_Timestamp_tSC,
        ed.Value_tSC as embryo_Value_tSC,
        
        -- Arrow annotation
        ed.Name_Arrow as embryo_Name_Arrow,
        ed.Time_Arrow as embryo_Time_Arrow,
        ed.Timestamp_Arrow as embryo_Timestamp_Arrow,
        ed.Value_Arrow as embryo_Value_Arrow,
        
        -- ReexpansionCount annotation
        ed.Name_ReexpansionCount as embryo_Name_ReexpansionCount,
        ed.Time_ReexpansionCount as embryo_Time_ReexpansionCount,
        ed.Timestamp_ReexpansionCount as embryo_Timestamp_ReexpansionCount,
        ed.Value_ReexpansionCount as embryo_Value_ReexpansionCount,
        
        -- BlastomereSize annotation
        ed.Name_BlastomereSize as embryo_Name_BlastomereSize,
        ed.Time_BlastomereSize as embryo_Time_BlastomereSize,
        ed.Timestamp_BlastomereSize as embryo_Timestamp_BlastomereSize,
        ed.Value_BlastomereSize as embryo_Value_BlastomereSize,
        
        -- Fragmentation annotation
        ed.Name_Fragmentation as embryo_Name_Fragmentation,
        ed.Time_Fragmentation as embryo_Time_Fragmentation,
        ed.Timestamp_Fragmentation as embryo_Timestamp_Fragmentation,
        ed.Value_Fragmentation as embryo_Value_Fragmentation,
        
        -- IrregularDivision annotation
        ed.Name_IrregularDivision as embryo_Name_IrregularDivision,
        ed.Time_IrregularDivision as embryo_Time_IrregularDivision,
        ed.Timestamp_IrregularDivision as embryo_Timestamp_IrregularDivision,
        ed.Value_IrregularDivision as embryo_Value_IrregularDivision,
        
        -- MultiNucleation annotation
        ed.Name_MultiNucleation as embryo_Name_MultiNucleation,
        ed.Time_MultiNucleation as embryo_Time_MultiNucleation,
        ed.Timestamp_MultiNucleation as embryo_Timestamp_MultiNucleation,
        ed.Value_MultiNucleation as embryo_Value_MultiNucleation,
        
        -- USRVAR annotations
        ed.Name_USRVAR_1_RC as embryo_Name_USRVAR_1_RC,
        ed.Time_USRVAR_1_RC as embryo_Time_USRVAR_1_RC,
        ed.Timestamp_USRVAR_1_RC as embryo_Timestamp_USRVAR_1_RC,
        ed.Value_USRVAR_1_RC as embryo_Value_USRVAR_1_RC,
        ed.Name_USRVAR_2_FD as embryo_Name_USRVAR_2_FD,
        ed.Time_USRVAR_2_FD as embryo_Time_USRVAR_2_FD,
        ed.Timestamp_USRVAR_2_FD as embryo_Timestamp_USRVAR_2_FD,
        ed.Value_USRVAR_2_FD as embryo_Value_USRVAR_2_FD,
        ed."Name_USRVAR_3_D1-3" as embryo_Name_USRVAR_3_D1_3,
        ed."Time_USRVAR_3_D1-3" as embryo_Time_USRVAR_3_D1_3,
        ed."Timestamp_USRVAR_3_D1-3" as embryo_Timestamp_USRVAR_3_D1_3,
        ed."Value_USRVAR_3_D1-3" as embryo_Value_USRVAR_3_D1_3,
        ed."Name_USRVAR_4_D2+" as embryo_Name_USRVAR_4_D2,
        ed."Time_USRVAR_4_D2+" as embryo_Time_USRVAR_4_D2,
        ed."Timestamp_USRVAR_4_D2+" as embryo_Timestamp_USRVAR_4_D2,
        ed."Value_USRVAR_4_D2+" as embryo_Value_USRVAR_4_D2,
        ed.Name_USRVAR_5_PULSING as embryo_Name_USRVAR_5_PULSING,
        ed.Time_USRVAR_5_PULSING as embryo_Time_USRVAR_5_PULSING,
        ed.Timestamp_USRVAR_5_PULSING as embryo_Timestamp_USRVAR_5_PULSING,
        ed.Value_USRVAR_5_PULSING as embryo_Value_USRVAR_5_PULSING,
        
        -- tDead annotation
        ed.Name_tDead as embryo_Name_tDead,
        ed.Time_tDead as embryo_Time_tDead,
        ed.Timestamp_tDead as embryo_Timestamp_tDead,
        ed.Value_tDead as embryo_Value_tDead
        
    FROM silver_embryoscope.patients p 
    LEFT JOIN silver_embryoscope.treatments t 
        ON p.PatientIDx = t.PatientIDx 
    LEFT JOIN silver_embryoscope.embryo_data ed 
        ON ed.PatientIDx = p.PatientIDx and ed.TreatmentName = t.TreatmentName
    ORDER BY p.PatientIDx DESC
    '''
    
    try:
        # Read data from source database
        logger.info('Connecting to source database to read data')
        with duckdb.connect(source_db_path) as source_con:
            logger.info('Connected to source DuckDB')
            # Execute query to get data
            df = source_con.execute(query).df()
            logger.info(f'Read {len(df)} rows from source database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in source database')
                return
                
            logger.info(f'Data columns: {list(df.columns)}')
            
            # Column filtering and cleanup
            logger.info('Applying column filtering and cleanup...')
            
            # Since we're explicitly selecting columns in the SQL query, 
            # we don't need to remove metadata columns as they're not included
            logger.info(f'Final column count: {len(df.columns)}')
            logger.info(f'Final columns: {list(df.columns)}')
            
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
            
            # Create table from DataFrame
            target_con.execute('CREATE TABLE gold.embryoscope_embrioes AS SELECT * FROM df')
            logger.info('Created gold.embryoscope_embrioes table')
            
            # Validate schema
            schema = target_con.execute("DESCRIBE gold.embryoscope_embrioes").fetchdf()
            logger.info(f'gold.embryoscope_embrioes schema:\n{schema}')
            
            # Validate row count
            row_count = target_con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            logger.info(f'gold.embryoscope_embrioes row count: {row_count}')
            
    except Exception as e:
        logger.error(f'Error in embryoscope gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Embryoscope gold loader finished successfully')

if __name__ == '__main__':
    main() 