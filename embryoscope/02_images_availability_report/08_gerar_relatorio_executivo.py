
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import os

# Configuration
DB_PATH = Path('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')
OUTPUT_DIR = Path('G:/My Drive/projetos_individuais/Huntington/embryoscope/report/exports')
ASSETS_DIR = OUTPUT_DIR / 'assets'
os.makedirs(ASSETS_DIR, exist_ok=True)

# Brand Colors (Huntington-style)
COLOR_PRIMARY = "#003366"  # Deep Navy
COLOR_SECONDARY = "#666666" # Gray
COLOR_SUCCESS = "#28a745"
COLOR_ERROR = "#dc3545"
COLOR_BG = "#f4f7f6"

# Apply Seaborn Theme
sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.labelcolor'] = COLOR_SECONDARY
plt.rcParams['axes.titlecolor'] = COLOR_PRIMARY
plt.rcParams['xtick.color'] = COLOR_SECONDARY
plt.rcParams['ytick.color'] = COLOR_SECONDARY

def load_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT 
            *,
            CAST(embryo_EmbryoDate AS DATE) as date_partition,
            STRFTIME(embryo_EmbryoDate, '%Y-%m') as month_partition,
            YEAR(CAST(embryo_EmbryoDate AS DATE)) as year_partition
        FROM silver.embryo_image_availability_latest
    """).df()
    conn.close()
    return df

def generate_charts(df, timestamp):
    charts = {}
    
    # 1. KPI Gauge (Refined horizontal bar with Seaborn)
    total = len(df)
    success = df[df['image_available'] == True].shape[0]
    rate = (success / total * 100) if total > 0 else 0
    
    plt.figure(figsize=(12, 3))
    # Background bar
    sns.barplot(x=[100], y=['Disponibilidade'], color='#e9ecef', alpha=0.5)
    # Success bar
    color = COLOR_SUCCESS if rate > 90 else "#ffc107" if rate > 70 else COLOR_ERROR
    sns.barplot(x=[rate], y=['Disponibilidade'], color=color)
    
    plt.title(f"Taxa de Disponibilidade Geral: {rate:.1f}%", fontsize=16, fontweight='bold', pad=20)
    plt.xlabel("Percentual (%)", fontsize=12)
    plt.xlim(0, 100)
    sns.despine(left=True)
    
    gauge_path = ASSETS_DIR / f"kpi_gauge_{timestamp}.png"
    plt.tight_layout()
    plt.savefig(gauge_path, dpi=150)
    plt.close()
    charts['gauge_path'] = gauge_path

    # 2. Status Bar Chart (Seaborn Barplot)
    def label_status(row):
        code = row['api_response_code']
        if code == 200:
            return 'Sucesso (Imagens)' if row['image_available'] else 'Sucesso (Sem Img)'
        elif code == 204: return '204 (Sem Contexto)'
        elif code == 500: return '500 (Erro Servidor)'
        return f'Outro ({code})'

    df['Status'] = df.apply(label_status, axis=1)
    status_counts = df['Status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Total']
    
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=status_counts, x='Status', y='Total', color=COLOR_PRIMARY, palette="Blues_d")
    plt.title("Distribuição Técnica de Status", fontsize=16, fontweight='bold', pad=20)
    plt.xticks(rotation=30, ha='right')
    plt.ylabel("Quantidade de Embriões")
    
    # Values on top
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height()):,}", 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points',
                   fontweight='bold', color=COLOR_PRIMARY)
    
    plt.ylim(0, status_counts['Total'].max() * 1.2)
    sns.despine()
    
    status_path = ASSETS_DIR / f"status_dist_{timestamp}.png"
    plt.tight_layout()
    plt.savefig(status_path, dpi=150)
    plt.close()
    charts['status_path'] = status_path

    # 3. Unit Performance (Seaborn Barplot)
    unit_stats = df.groupby('patient_unit_huntington').agg(
        total=('embryo_EmbryoID', 'count'),
        disponivel=('image_available', 'sum')
    ).reset_index()
    unit_stats['Taxa %'] = (unit_stats['disponivel'] / unit_stats['total'] * 100).round(1)
    unit_stats = unit_stats.sort_values('Taxa %', ascending=False)
    
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=unit_stats, x='patient_unit_huntington', y='Taxa %', color=COLOR_PRIMARY, palette="Blues_r")
    plt.title("Taxa de Disponibilidade por Unidade", fontsize=16, fontweight='bold', pad=20)
    plt.xticks(rotation=30, ha='right')
    plt.ylabel("Disponibilidade (%)")
    plt.ylim(0, 115)
    
    # Values on top
    for p in ax.patches:
        ax.annotate(f"{p.get_height()}%", 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points',
                   fontweight='bold', color=COLOR_PRIMARY)

    sns.despine()
    unit_path = ASSETS_DIR / f"unit_dist_{timestamp}.png"
    plt.tight_layout()
    plt.savefig(unit_path, dpi=150)
    plt.close()
    charts['unit_path'] = unit_path

    # 4. Temporal Trend (Seaborn Lineplot)
    monthly = df.groupby(['month_partition', 'patient_unit_huntington']).agg(
        total=('embryo_EmbryoID', 'count'),
        disponivel=('image_available', 'sum')
    ).reset_index()
    monthly['Taxa %'] = (monthly['disponivel'] / monthly['total'] * 100).round(1)
    monthly['Data'] = pd.to_datetime(monthly['month_partition'])
    monthly = monthly.sort_values('Data')
    
    plt.figure(figsize=(14, 8))
    sns.lineplot(data=monthly, x='Data', y='Taxa %', hue='patient_unit_huntington', 
                marker='o', palette="tab10", linewidth=2.5)
    
    plt.title("Evolução Mensal de Disponibilidade", fontsize=16, fontweight='bold', pad=20)
    plt.ylabel("Taxa de Sucesso (%)")
    plt.ylim(0, 105)
    
    plt.legend(title="Unidade", loc='lower left', frameon=True, framealpha=0.9, fontsize=10)
    plt.xticks(rotation=30, ha='right')
    sns.despine()

    trend_path = ASSETS_DIR / f"trend_line_{timestamp}.png"
    plt.tight_layout()
    plt.savefig(trend_path, dpi=150)
    plt.close()
    charts['trend_path'] = trend_path
    
    return charts

def create_markdown(df, charts, timestamp):
    unit_summary = df.groupby('patient_unit_huntington').agg(
        total_embrioes=('embryo_EmbryoID', 'count'),
        sem_dados_204=('api_response_code', lambda x: (x == 204).sum()),
        erro_servidor_500=('api_response_code', lambda x: (x == 500).sum())
    ).reset_index().sort_values('total_embrioes', ascending=False)
    unit_summary.columns = ['Unidade', 'Total', 'Sem Dados (204)', 'Erro (500)']

    ibi_2025 = df[(df['patient_unit_huntington'] == 'Ibirapuera') & (df['year_partition'] == 2025)].copy()
    sem_dados_ibi = ibi_2025[ibi_2025['image_available'] == False].copy()
    ibi_resumo = sem_dados_ibi.groupby('month_partition').agg(
        pacientes_afetados=('patient_PatientID', 'nunique'),
        embrioes_sem_dados=('embryo_EmbryoID', 'count')
    ).reset_index().sort_values('month_partition', ascending=False)
    ibi_resumo.columns = ['Mês', 'Pacientes Afetados', 'Embriões sem Dados']

    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    md_content = f"""# Relatório Executivo Huntington - Disponibilidade de Imagens
**Data de Geração:** {now}

---

<div style="page-break-inside: avoid;">

## 1. Indicadores Chave de Desempenho (KPIs)

![Taxa de Disponibilidade Geral](assets/{charts['gauge_path'].name})

*Métrica consolidada de sucesso da integração entre API e banco de dados.*

![Distribuição Técnica de Status](assets/{charts['status_path'].name})

*Visão técnica dos retornos do servidor. Erros 500 ou Sucesso Sem Imagens indicam pontos de atenção.*

</div>

---

<div style="page-break-inside: avoid;">

## 2. Análise Regional e Temporal

![Disponibilidade por Unidade](assets/{charts['unit_path'].name})

### Resumo de Dados por Unidade
| Unidade | Total | Sem Dados (204) | Erro (500) |
|:---|---:|---:|---:|
"""
    for _, row in unit_summary.iterrows():
        md_content += f"| {row['Unidade']} | {int(row['Total']):,} | {int(row['Sem Dados (204)']):,} | {int(row['Erro (500)']):,} |\n"

    md_content += f"""
![Evolução Mensal por Unidade](assets/{charts['trend_path'].name})

*Acompanhamento da estabilidade de conexão por unidade ao longo dos meses.*

</div>

---

<div style="page-break-inside: avoid;">

## 3. Resumo de Atenção: Ibirapuera (2025)

**Métricas Gerais Ibirapuera 2025:**
- **Total de Embriões:** {len(ibi_2025):,}
- **Embriões sem Dados:** {len(sem_dados_ibi):,}
- **Pacientes Afetados:** {sem_dados_ibi['patient_PatientID'].nunique():,}

### Tabela Mensal de Falhas (Ibirapuera 2025)
| Mês | Pacientes Afetados | Embriões sem Dados |
|:---|---:|---:|
"""
    for _, row in ibi_resumo.iterrows():
        md_content += f"| {row['Mês']} | {int(row['Pacientes Afetados']):,} | {int(row['Embriões sem Dados']):,} |\n"

    md_content += """
</div>

---
*Huntington Medicina Reprodutiva*
"""
    
    filename = f"relatorio_executivo_{timestamp}.md"
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(md_content)
    return filepath

def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    print("Iniciando geração de relatório executivo (MARKDOWN + SEABORN/PREMIUM VIEW)...")
    
    df = load_data()
    print("Dados carregados com sucesso.")
    
    charts = generate_charts(df, timestamp)
    print("Gráficos gerados com Seaborn (Estilo Executivo).")
    
    report_path = create_markdown(df, charts, timestamp)
    print(f"\n✅ Relatório Markdown gerado com sucesso!")
    print(f"📍 Localização: {report_path.resolve()}")
    print("\n---")
    print("Próximos passos:")
    print("1. Abra o arquivo .md no VS Code.")
    print("2. Pressione Ctrl+Shift+V para ver o preview.")
    print("3. Com o preview aberto, use o comando 'Markdown: Export to PDF' ou execute npx md-to-pdf.")

if __name__ == "__main__":
    main()
