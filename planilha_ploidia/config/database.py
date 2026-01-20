"""Database configuration and paths"""
import os

class DatabaseConfig:
    """Database paths configuration"""
    # Get the repository root (parent of planilha_ploidia)
    REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Database paths
    HUNTINGTON_DB = os.path.join(REPO_ROOT, 'database', 'huntington_data_lake.duckdb')
    CLINISYS_DB = os.path.join(REPO_ROOT, 'database', 'clinisys_all.duckdb')
    
    # Excel file path
    EXCEL_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        'planilhas_exemplo',
        'Planilha IA ploidia - 344 embrioes (1).xlsx'
    )
    
    # Export directory
    DATA_EXPORT_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        'data_export'
    )
    
    # Logs directory
    LOGS_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        'logs'
    )
