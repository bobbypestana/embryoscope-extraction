"""Language and normalization mappings for data comparison"""

# Diagnosis mappings (Portuguese <-> English)
DIAGNOSIS_MAP = {
    'fator masculino': 'male factor',
    'male factor': 'male factor',
    'fator feminino': 'female factor',
    'female factor': 'female factor',
    'fator misto': 'mixed factor',
    'mixed factor': 'mixed factor',
    'idiopático': 'idiopathic',
    'idiopathic': 'idiopathic',
    'outros': 'other',
    'other': 'other',
    'endometriose': 'endometriosis',
    'endometriosis': 'endometriosis',
    'inexplicado': 'unknown',
    'unknown': 'unknown',
}

# Oocyte source mappings (Portuguese <-> English)
OOCYTE_SOURCE_MAP = {
    'fresco': 'fresh',
    'fresh': 'fresh',
    'autólogo': 'autologous',
    'autologous': 'autologous',
    'homólogo': 'autologous',  # Portuguese for autologous
    'homologous': 'autologous',
    'heterólogo': 'heterologous',  # Portuguese for donor/heterologous
    'heterologous': 'heterologous',
    'donor': 'heterologous',
    'ambos': 'both',  # Portuguese for both
    'both': 'both',
}

# Unit mappings
UNIT_MAP = {
    'ibirapuera': 'ibi',
    'ibi': 'ibi',
}

# Combined language map for easy lookup
LANGUAGE_MAP = {
    **DIAGNOSIS_MAP,
    **OOCYTE_SOURCE_MAP,
    **UNIT_MAP,
}

# Helper columns to exclude from comparison reports
HELPER_COLUMNS = {'_excel_slide_id', '_normalized_patient_id', '_filter_key'}
