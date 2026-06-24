import os
import json
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Ensure directories exist
os.makedirs("data_lake_validation/published_reports/images", exist_ok=True)

# Connect to DuckDB
print("Connecting to database...")
db_path = "database/clinisys_all.duckdb"
conn = duckdb.connect(db_path, read_only=True)

# Query data
print("Fetching 2026 treatments...")
query = """
    SELECT 
        t.id as treatment_id,
        t.prontuario,
        t.data_procedimento,
        t.peso_paciente,
        t.altura_paciente,
        t.peso_conjuge,
        t.altura_conjuge,
        p.medico as medico_id,
        COALESCE(m.nome, 'SEM MEDICO') as medico_nome
    FROM silver.view_tratamentos t
    LEFT JOIN silver.view_pacientes p ON t.prontuario = p.codigo
    LEFT JOIN silver.view_medicos m ON p.medico = m.id
    WHERE t.data_procedimento >= '2026-01-01' AND t.data_procedimento < '2027-01-01'
"""
df = conn.execute(query).df()
conn.close()

# Parse month
df['month'] = pd.to_datetime(df['data_procedimento']).dt.strftime('%m - %B')

# Classification logic
def classify_weight(val):
    if pd.isna(val) or val is None or val <= 0:
        return 'Missing'
    if val > 150.0 or val < 40.0:
        return 'Typo'
    return 'Valid'

def classify_height(val):
    if pd.isna(val) or val is None or val <= 0:
        return 'Missing'
    if val > 2.2 or val < 1.0:
        return 'Typo'
    return 'Valid'

# Apply classifications
df['status_peso_paciente'] = df['peso_paciente'].apply(classify_weight)
df['status_altura_paciente'] = df['altura_paciente'].apply(classify_height)
df['status_peso_conjuge'] = df['peso_conjuge'].apply(classify_weight)
df['status_altura_conjuge'] = df['altura_conjuge'].apply(classify_height)

# Calculate statistics per doctor
print("Running statistical integrity checks on doctors...")
doctors = df['medico_nome'].unique()
doctor_stats = []

for doc in doctors:
    doc_df = df[df['medico_nome'] == doc]
    n_treatments = len(doc_df)
    
    doc_info = {
        'medico': doc,
        'treatments': n_treatments
    }
    
    for field, is_height in [
        ('peso_paciente', False), 
        ('altura_paciente', True),
        ('peso_conjuge', False),
        ('altura_conjuge', True)
    ]:
        # Filter valid/non-zero values
        valid_vals = doc_df[doc_df[f'status_{field}'] == 'Valid'][field].dropna()
        n_valid = len(valid_vals)
        n_missing = len(doc_df[doc_df[f'status_{field}'] == 'Missing'])
        n_typo = len(doc_df[doc_df[f'status_{field}'] == 'Typo'])
        
        # Quantity Metrics
        fill_rate = (n_valid + n_typo) / n_treatments if n_treatments > 0 else 0
        valid_rate = n_valid / n_treatments if n_treatments > 0 else 0
        
        # Repetition Metrics
        std_val = float(valid_vals.std()) if n_valid > 1 else 0.0
        unique_vals = valid_vals.nunique()
        unique_ratio = unique_vals / n_valid if n_valid > 0 else 0.0
        
        if n_valid > 0:
            mode_val = valid_vals.mode()[0]
            mode_pct = len(valid_vals[valid_vals == mode_val]) / n_valid
        else:
            mode_val = None
            mode_pct = 0.0
            
        # Repetition check flag
        # If doctor has >= 10 valid records, but standard deviation is extremely low, or mode concentration is high
        rep_flag = False
        if n_valid >= 10:
            if is_height:
                if std_val < 0.01 or mode_pct >= 0.5:
                    rep_flag = True
            else:
                if std_val < 1.0 or mode_pct >= 0.5:
                    rep_flag = True
                    
        doc_info[f'{field}_total'] = n_treatments
        doc_info[f'{field}_filled'] = n_valid + n_typo
        doc_info[f'{field}_valid'] = n_valid
        doc_info[f'{field}_typo'] = n_typo
        doc_info[f'{field}_missing'] = n_missing
        doc_info[f'{field}_fill_rate'] = fill_rate
        doc_info[f'{field}_valid_rate'] = valid_rate
        doc_info[f'{field}_std'] = std_val
        doc_info[f'{field}_mode_pct'] = mode_pct
        doc_info[f'{field}_rep_flag'] = rep_flag

    doctor_stats.append(doc_info)

df_doc_stats = pd.DataFrame(doctor_stats).sort_values(by='treatments', ascending=False)

# ----------------- Visualizations -----------------
print("Generating charts...")
sns.set_theme(style="whitegrid")

# Plot 1: Monthly fill rate trend
months_sorted = sorted(df['month'].unique())
monthly_fill = []
for m in months_sorted:
    m_df = df[df['month'] == m]
    monthly_fill.append({
        'Month': m,
        'Peso Paciente': (m_df['status_peso_paciente'] == 'Valid').mean() * 100,
        'Altura Paciente': (m_df['status_altura_paciente'] == 'Valid').mean() * 100,
        'Peso Cônjuge': (m_df['status_peso_conjuge'] == 'Valid').mean() * 100,
        'Altura Cônjuge': (m_df['status_altura_conjuge'] == 'Valid').mean() * 100,
    })
df_monthly = pd.DataFrame(monthly_fill)

plt.figure(figsize=(10, 5))
plt.plot(df_monthly['Month'], df_monthly['Peso Paciente'], marker='o', label='Peso Paciente', color='#2b5c8f')
plt.plot(df_monthly['Month'], df_monthly['Altura Paciente'], marker='s', label='Altura Paciente', color='#4682b4')
plt.plot(df_monthly['Month'], df_monthly['Peso Cônjuge'], marker='^', label='Peso Cônjuge', color='#e07a5f')
plt.plot(df_monthly['Month'], df_monthly['Altura Cônjuge'], marker='d', label='Altura Cônjuge', color='#f4a261')
plt.title('Monthly Valid Data Quality Filling Rate (%) - 2026', fontsize=14, pad=15)
plt.ylabel('Valid Fill Rate (%)')
plt.xlabel('Month')
plt.ylim(0, 105)
plt.xticks(rotation=45)
plt.legend(frameon=True)
plt.tight_layout()
plt.savefig("data_lake_validation/published_reports/images/monthly_quality_trend.png", dpi=150)
plt.close()

# Plot 2: Heatmap of Valid Fill Rates per Doctor (all doctors, tall figure)
# Keep it clear by using Seaborn Heatmap
heatmap_data = df_doc_stats.set_index('medico')[[
    'peso_paciente_valid_rate', 
    'altura_paciente_valid_rate', 
    'peso_conjuge_valid_rate', 
    'altura_conjuge_valid_rate'
]] * 100
heatmap_data.columns = ['Peso Paciente', 'Altura Paciente', 'Peso Cônjuge', 'Altura Cônjuge']

fig_height = max(8, len(df_doc_stats) * 0.4)
plt.figure(figsize=(10, fig_height))
sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="YlGnBu", cbar_kws={'label': 'Filling Rate (%)'}, vmin=0, vmax=100)
plt.title('Valid Data Quality Fill Rate per Doctor (%) - 2026', fontsize=14, pad=20)
plt.ylabel('Doctor')
plt.tight_layout()
plt.savefig("data_lake_validation/published_reports/images/doctor_quality_heatmap.png", dpi=150)
plt.close()

# Plot 3: Distribution histograms (excluding zeros and typos for clean display)
valid_pesos = df[df['status_peso_paciente'] == 'Valid']['peso_paciente']
valid_alturas = df[df['status_altura_paciente'] == 'Valid']['altura_paciente']

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
sns.histplot(valid_pesos, kde=True, ax=ax1, color='#2b5c8f')
ax1.set_title('Patient Weight Distribution (Valid Range)')
ax1.set_xlabel('Weight (kg)')

sns.histplot(valid_alturas, kde=True, ax=ax2, color='#4682b4')
ax2.set_title('Patient Height Distribution (Valid Range)')
ax2.set_xlabel('Height (m)')
plt.tight_layout()
plt.savefig("data_lake_validation/published_reports/images/value_distributions.png", dpi=150)
plt.close()

# Plot 4: Annual Patient Filling Rate
plt.figure(figsize=(6, 4))
patient_rates = [
    (df['status_peso_paciente'] == 'Valid').mean() * 100,
    (df['status_altura_paciente'] == 'Valid').mean() * 100
]
labels = ['Patient Weight', 'Patient Height']
bars = plt.bar(labels, patient_rates, color=['#2b5c8f', '#4682b4'], width=0.4)
plt.ylabel('Valid Fill Rate (%)')
plt.title('Annual Patient Data Quality Fill Rate - 2026', fontsize=12, pad=15)
plt.ylim(0, 105)
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 2, f"{yval:.1f}%", ha='center', va='bottom', fontweight='bold')
plt.tight_layout()
plt.savefig("data_lake_validation/published_reports/images/annual_patient_filling_rate.png", dpi=150)
plt.close()

# Query historical data (2018-2026) for patient only
print("Fetching historical treatments...")
conn_hist = duckdb.connect(db_path, read_only=True)
df_hist = conn_hist.execute("""
    SELECT 
        YEAR(data_procedimento) as year,
        peso_paciente,
        altura_paciente
    FROM silver.view_tratamentos
    WHERE data_procedimento >= '2018-01-01' AND data_procedimento < '2027-01-01'
""").df()
conn_hist.close()

# Classify historical data
df_hist = df_hist.dropna(subset=['year'])
df_hist['year'] = df_hist['year'].astype(int)
df_hist['status_peso'] = df_hist['peso_paciente'].apply(classify_weight)
df_hist['status_altura'] = df_hist['altura_paciente'].apply(classify_height)

# Group by year
hist_stats = df_hist.groupby('year').agg(
    valid_peso=('status_peso', lambda x: (x == 'Valid').mean() * 100),
    valid_altura=('status_altura', lambda x: (x == 'Valid').mean() * 100)
).reset_index()
hist_stats.columns = ['year', 'Peso Paciente', 'Altura Paciente']

# Plot 5: Historical Patient Trend (2018-2026)
plt.figure(figsize=(8, 4.5))
plt.plot(hist_stats['year'], hist_stats['Peso Paciente'], marker='o', label='Patient Weight', color='#2b5c8f', linewidth=2)
plt.plot(hist_stats['year'], hist_stats['Altura Paciente'], marker='s', label='Patient Height', color='#4682b4', linewidth=2)
plt.title('Historical Patient Data Quality Fill Rate (2018 - 2026)', fontsize=12, pad=15)
plt.ylabel('Valid Fill Rate (%)')
plt.xlabel('Year')
plt.ylim(0, 105)
plt.grid(True, linestyle='--', alpha=0.6)
for i, row in hist_stats.iterrows():
    plt.text(row['year'], row['Peso Paciente'] + 2, f"{row['Peso Paciente']:.1f}%", ha='center', va='bottom', fontsize=9, fontweight='bold', color='#2b5c8f')
    plt.text(row['year'], row['Altura Paciente'] - 4, f"{row['Altura Paciente']:.1f}%", ha='center', va='top', fontsize=9, fontweight='bold', color='#4682b4')
plt.legend(frameon=True)
plt.tight_layout()
plt.savefig("data_lake_validation/published_reports/images/historical_patient_quality_trend.png", dpi=150)
plt.close()



# ----------------- Write Markdown Report -----------------
print("Writing Markdown report...")
report_path = "data_lake_validation/published_reports/20260612_clinisys_peso_altura_quality_report.md"

# Global calculations
tot_records = len(df)
missing_p_peso = (df['status_peso_paciente'] == 'Missing').sum()
missing_p_alt = (df['status_altura_paciente'] == 'Missing').sum()
typo_p_peso = (df['status_peso_paciente'] == 'Typo').sum()
typo_p_alt = (df['status_altura_paciente'] == 'Typo').sum()

missing_c_peso = (df['status_peso_conjuge'] == 'Missing').sum()
missing_c_alt = (df['status_altura_conjuge'] == 'Missing').sum()
typo_c_peso = (df['status_peso_conjuge'] == 'Typo').sum()
typo_c_alt = (df['status_altura_conjuge'] == 'Typo').sum()

abs_img_dir = os.path.abspath("data_lake_validation/published_reports/images")
abs_img_dir_url = "file:///" + abs_img_dir.replace('\\', '/')

with open(report_path, "w", encoding="utf-8") as rf:
    rf.write("# Clinisys Data Quality: Weight and Height Filling Analysis (2026)\n\n")
    rf.write("This report presents the quantity (filling rate) and quality (validity & repetition) of patient and partner weight/height inputs in `silver.view_tratamentos` for the year 2026.\n\n")
    
    rf.write("## Executive Summary\n\n")
    rf.write(f"- **Total Treatments in 2026**: {tot_records:,}\n")
    rf.write(f"- **Patient Weight filling**: Valid = {tot_records - missing_p_peso - typo_p_peso:,} ({((tot_records - missing_p_peso - typo_p_peso)/tot_records)*100:.2f}%), Missing/Zero = {missing_p_peso:,} ({missing_p_peso/tot_records*100:.2f}%), Outliers/Typos = {typo_p_peso:,} ({typo_p_peso/tot_records*100:.2f}%)\n")
    rf.write(f"- **Patient Height filling**: Valid = {tot_records - missing_p_alt - typo_p_alt:,} ({((tot_records - missing_p_alt - typo_p_alt)/tot_records)*100:.2f}%), Missing/Zero = {missing_p_alt:,} ({missing_p_alt/tot_records*100:.2f}%), Outliers/Typos = {typo_p_alt:,} ({typo_p_alt/tot_records*100:.2f}%)\n")
    rf.write(f"- **Partner Weight filling**: Valid = {tot_records - missing_c_peso - typo_c_peso:,} ({((tot_records - missing_c_peso - typo_c_peso)/tot_records)*100:.2f}%), Missing/Zero = {missing_c_peso:,} ({missing_c_peso/tot_records*100:.2f}%), Outliers/Typos = {typo_c_peso:,} ({typo_c_peso/tot_records*100:.2f}%)\n")
    rf.write(f"- **Partner Height filling**: Valid = {tot_records - missing_c_alt - typo_c_alt:,} ({((tot_records - missing_c_alt - typo_c_alt)/tot_records)*100:.2f}%), Missing/Zero = {missing_c_alt:,} ({missing_c_alt/tot_records*100:.2f}%), Outliers/Typos = {typo_c_alt:,} ({typo_c_alt/tot_records*100:.2f}%)\n\n")

    rf.write("## Visualizations\n\n")
    rf.write("### 1. Monthly Trends\n")
    rf.write(f"![Monthly Filling Quality Trend]({abs_img_dir_url}/monthly_quality_trend.png)\n\n")
    
    rf.write("### 2. Valid Distributions\n")
    rf.write(f"![Value Distributions]({abs_img_dir_url}/value_distributions.png)\n\n")
    
    rf.write("### 3. Doctor Quality Heatmap\n")
    rf.write(f"![Doctor Quality Heatmap]({abs_img_dir_url}/doctor_quality_heatmap.png)\n\n")
    
    rf.write("### 4. Annual Patient Quality Fill Rate\n")
    rf.write(f"![Annual Patient Quality Fill Rate]({abs_img_dir_url}/annual_patient_filling_rate.png)\n\n")
    
    rf.write("### 5. Historical Patient Quality Trend (2018 - 2026)\n")
    rf.write(f"![Historical Patient Quality Trend]({abs_img_dir_url}/historical_patient_quality_trend.png)\n\n")
    
    rf.write("## Repetition and Default Inputs Statistical Check\n\n")
    rf.write("> [!IMPORTANT]\n")
    rf.write("> Doctors flagged below show **high mode concentration (>=50%)** or **extremely low standard deviation (weight std < 1.0kg, height std < 0.01m)** despite having 10 or more filled treatments. This statistically suggests they may be copy-pasting default values rather than recording actual patient measurements.\n\n")
    
    rep_flagged = df_doc_stats[
        df_doc_stats['peso_paciente_rep_flag'] | 
        df_doc_stats['altura_paciente_rep_flag'] |
        df_doc_stats['peso_conjuge_rep_flag'] | 
        df_doc_stats['altura_conjuge_rep_flag']
    ]
    
    if len(rep_flagged) > 0:
        rf.write("| Doctor | Treatments | Suspicious Fields | Mode Value & Conc. | Std Dev (Weight/Height) |\n")
        rf.write("| :--- | :--- | :--- | :--- | :--- |\n")
        for _, row in rep_flagged.iterrows():
            fields = []
            details = []
            stds = []
            for field, label in [
                ('peso_paciente', 'Peso Pac.'), 
                ('altura_paciente', 'Alt. Pac.'),
                ('peso_conjuge', 'Peso Conj.'),
                ('altura_conjuge', 'Alt. Conj.')
            ]:
                if row[f'{field}_rep_flag']:
                    fields.append(label)
                    # Find mode and percentage
                    valid_vals = df[(df['medico_nome'] == row['medico']) & (df[f'status_{field}'] == 'Valid')][field]
                    if not valid_vals.empty:
                        m_val = valid_vals.mode()[0]
                        m_pct = (valid_vals == m_val).mean() * 100
                        details.append(f"{label}: {m_val} ({m_pct:.0f}%)")
                    stds.append(f"{label}: {row[f'{field}_std']:.2f}")
            rf.write(f"| **{row['medico']}** | {row['treatments']} | {', '.join(fields)} | {'; '.join(details)} | {'; '.join(stds)} |\n")
    else:
        rf.write("*No doctors flagged with suspicious repetitive data entry.*\n")
    
    rf.write("\n## Complete Doctor Quality & Quantity Table\n\n")
    rf.write("| Doctor | N | Peso Pac Valid % | Alt Pac Valid % | Peso Conj Valid % | Alt Conj Valid % | Status |\n")
    rf.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
    for _, row in df_doc_stats.iterrows():
        status = ""
        if row['peso_paciente_rep_flag'] or row['altura_paciente_rep_flag']:
            status = "⚠️ Repetitive Pac."
        elif row['peso_conjuge_rep_flag'] or row['altura_conjuge_rep_flag']:
            status = "⚠️ Repetitive Conj."
            
        rf.write(f"| {row['medico']} | {row['treatments']} | {row['peso_paciente_valid_rate']*100:.1f}% | {row['altura_paciente_valid_rate']*100:.1f}% | {row['peso_conjuge_valid_rate']*100:.1f}% | {row['altura_conjuge_valid_rate']*100:.1f}% | {status} |\n")

# ----------------- Write HTML Dashboard -----------------
print("Writing Interactive HTML Dashboard...")
html_path = "data_lake_validation/published_reports/20260612_clinisys_peso_altura_dashboard.html"

# Compile JSON data for embedding inside HTML
dashboard_data = {
    'summary': {
        'total_treatments': int(tot_records),
        'peso_paciente_valid': int(tot_records - missing_p_peso - typo_p_peso),
        'peso_paciente_missing': int(missing_p_peso),
        'peso_paciente_typo': int(typo_p_peso),
        'altura_paciente_valid': int(tot_records - missing_p_alt - typo_p_alt),
        'altura_paciente_missing': int(missing_p_alt),
        'altura_paciente_typo': int(typo_p_alt),
        'peso_conjuge_valid': int(tot_records - missing_c_peso - typo_c_peso),
        'peso_conjuge_missing': int(missing_c_peso),
        'peso_conjuge_typo': int(typo_c_peso),
        'altura_conjuge_valid': int(tot_records - missing_c_alt - typo_c_alt),
        'altura_conjuge_missing': int(missing_c_alt),
        'altura_conjuge_typo': int(typo_c_alt),
    },
    'monthly': df_monthly.to_dict(orient='records'),
    'doctors': df_doc_stats.to_dict(orient='records'),
    'historical': hist_stats.to_dict(orient='records')
}

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Clinisys Data Quality Dashboard (2026)</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-slate-50 text-slate-800 font-sans">
    <div class="max-w-7xl mx-auto px-4 py-8">
        <header class="mb-8">
            <h1 class="text-3xl font-bold text-slate-900">Clinisys Data Quality Dashboard (2026)</h1>
            <p class="text-slate-600 mt-2">Analysis of quantity and quality of patient and partner weight/height measurements.</p>
        </header>

        <!-- KPI Summary Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <div class="text-sm font-medium text-slate-500">Total Treatments</div>
                <div class="text-3xl font-bold text-slate-900 mt-1">{tot_records:,}</div>
            </div>
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <div class="text-sm font-medium text-slate-500">Patient Weight (Valid %)</div>
                <div class="text-3xl font-bold text-indigo-600 mt-1">{((tot_records - missing_p_peso - typo_p_peso)/tot_records)*100:.1f}%</div>
                <div class="text-xs text-slate-500 mt-1">Typos: {typo_p_peso} | Zeros/Missing: {missing_p_peso}</div>
            </div>
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <div class="text-sm font-medium text-slate-500">Patient Height (Valid %)</div>
                <div class="text-3xl font-bold text-blue-600 mt-1">{((tot_records - missing_p_alt - typo_p_alt)/tot_records)*100:.1f}%</div>
                <div class="text-xs text-slate-500 mt-1">Typos: {typo_p_alt} | Zeros/Missing: {missing_p_alt}</div>
            </div>
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <div class="text-sm font-medium text-slate-500">Flagged Repetitive Entries</div>
                <div class="text-3xl font-bold text-rose-600 mt-1">{len(rep_flagged)}</div>
                <div class="text-xs text-slate-500 mt-1">Doctors with suspicious variance</div>
            </div>
        </div>

        <!-- Trend Chart -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <h2 class="text-xl font-bold text-slate-900 mb-4">Monthly Valid Quality Trends (2026)</h2>
                <div class="h-80">
                    <canvas id="trendChart"></canvas>
                </div>
            </div>
            <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
                <h2 class="text-xl font-bold text-slate-900 mb-4">Historical Patient Trends (2018 - 2026)</h2>
                <div class="h-80">
                    <canvas id="historicalChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Doctor Quality Table -->
        <div class="bg-white p-6 rounded-xl shadow-sm border border-slate-100">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
                <h2 class="text-xl font-bold text-slate-900">Data Quality by Doctor</h2>
                <div class="flex gap-4">
                    <input type="text" id="searchInput" placeholder="Search doctor..." class="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500">
                    <select id="flagFilter" class="px-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500">
                        <option value="all">All Doctors</option>
                        <option value="flagged">⚠️ Repetitive/Flagged Only</option>
                        <option value="clean">✅ Clean Only</option>
                    </select>
                </div>
            </div>

            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="border-b border-slate-100 text-slate-400 text-sm font-medium uppercase">
                            <th class="py-3 px-4">Doctor</th>
                            <th class="py-3 px-4 text-center">N</th>
                            <th class="py-3 px-4 text-center">Peso Pac Valid %</th>
                            <th class="py-3 px-4 text-center">Alt Pac Valid %</th>
                            <th class="py-3 px-4 text-center">Peso Conj Valid %</th>
                            <th class="py-3 px-4 text-center">Alt Conj Valid %</th>
                            <th class="py-3 px-4 text-center">Status</th>
                        </tr>
                    </thead>
                    <tbody id="tableBody">
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        const data = {json.dumps(dashboard_data)};
        
        // Render Trend Chart
        const ctx = document.getElementById('trendChart').getContext('2d');
        const months = data.monthly.map(m => m.Month.split(' - ')[1]);
        
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: months,
                datasets: [
                    {{
                        label: 'Peso Paciente',
                        data: data.monthly.map(m => m['Peso Paciente']),
                        borderColor: '#4f46e5',
                        backgroundColor: 'transparent',
                        tension: 0.1
                    }},
                    {{
                        label: 'Altura Paciente',
                        data: data.monthly.map(m => m['Altura Paciente']),
                        borderColor: '#2563eb',
                        backgroundColor: 'transparent',
                        tension: 0.1
                    }},
                    {{
                        label: 'Peso Cônjuge',
                        data: data.monthly.map(m => m['Peso Cônjuge']),
                        borderColor: '#ea580c',
                        backgroundColor: 'transparent',
                        tension: 0.1
                    }},
                    {{
                        label: 'Altura Cônjuge',
                        data: data.monthly.map(m => m['Altura Cônjuge']),
                        borderColor: '#f59e0b',
                        backgroundColor: 'transparent',
                        tension: 0.1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        min: 0,
                        max: 100,
                        title: {{
                            display: true,
                            text: 'Valid Fill Rate (%)'
                        }}
                    }}
                }}
            }}
        }});

        // Render Historical Trend Chart
        const ctxHist = document.getElementById('historicalChart').getContext('2d');
        const years = data.historical.map(h => h.year);
        
        new Chart(ctxHist, {{
            type: 'line',
            data: {{
                labels: years,
                datasets: [
                    {{
                        label: 'Peso Paciente',
                        data: data.historical.map(h => h['Peso Paciente']),
                        borderColor: '#4f46e5',
                        backgroundColor: 'transparent',
                        tension: 0.1,
                        borderWidth: 2,
                        pointRadius: 4
                    }},
                    {{
                        label: 'Altura Paciente',
                        data: data.historical.map(h => h['Altura Paciente']),
                        borderColor: '#2563eb',
                        backgroundColor: 'transparent',
                        tension: 0.1,
                        borderWidth: 2,
                        pointRadius: 4
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        min: 0,
                        max: 100,
                        title: {{
                            display: true,
                            text: 'Valid Fill Rate (%)'
                        }}
                    }}
                }}
            }}
        }});

        // Render Table Rows
        function renderTable(filterText = '', flagFilter = 'all') {{
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            
            data.doctors.forEach(doc => {{
                const nameMatch = doc.medico.toLowerCase().includes(filterText.toLowerCase());
                const isFlagged = doc.peso_paciente_rep_flag || doc.altura_paciente_rep_flag || doc.peso_conjuge_rep_flag || doc.altura_conjuge_rep_flag;
                
                let flagMatch = true;
                if (flagFilter === 'flagged') flagMatch = isFlagged;
                if (flagFilter === 'clean') flagMatch = !isFlagged;
                
                if (nameMatch && flagMatch) {{
                    const statusText = isFlagged ? '⚠️ Suspicious Repetition' : '';
                    const statusClass = isFlagged ? 'text-amber-600 bg-amber-50 px-2 py-1 rounded-full text-xs font-semibold' : '';
                    
                    const row = document.createElement('tr');
                    row.className = 'border-b border-slate-100 hover:bg-slate-50 transition-colors text-sm';
                    row.innerHTML = `
                        <td class="py-4 px-4 font-semibold text-slate-900">${{doc.medico}}</td>
                        <td class="py-4 px-4 text-center font-medium">${{doc.treatments}}</td>
                        <td class="py-4 px-4 text-center">${{(doc.peso_paciente_valid_rate*100).toFixed(1)}}%</td>
                        <td class="py-4 px-4 text-center">${{(doc.altura_paciente_valid_rate*100).toFixed(1)}}%</td>
                        <td class="py-4 px-4 text-center">${{(doc.peso_conjuge_valid_rate*100).toFixed(1)}}%</td>
                        <td class="py-4 px-4 text-center">${{(doc.altura_conjuge_valid_rate*100).toFixed(1)}}%</td>
                        <td class="py-4 px-4 text-center">
                            <span class="${{statusClass}}">${{statusText}}</span>
                        </td>
                    `;
                    tbody.appendChild(row);
                }}
            }});
        }}

        // Event Listeners
        document.getElementById('searchInput').addEventListener('input', (e) => {{
            renderTable(e.target.value, document.getElementById('flagFilter').value);
        }});
        
        document.getElementById('flagFilter').addEventListener('change', (e) => {{
            renderTable(document.getElementById('searchInput').value, e.target.value);
        }});

        // Initial Table Render
        renderTable();
    </script>
</body>
</html>
"""

with open(html_path, "w", encoding="utf-8") as hf:
    hf.write(html_content)

print("Finished successfully! Outputs written to:")
print(" - Markdown Report:", report_path)
print(" - HTML Dashboard:", html_path)
