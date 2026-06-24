# Clinisys Data Quality: Weight and Height Filling Analysis (2026)

This report presents the quantity (filling rate) and quality (validity & repetition) of patient and partner weight/height inputs in `silver.view_tratamentos` for the year 2026.

## Executive Summary

- **Total Treatments in 2026**: 2,977
- **Patient Weight filling**: Valid = 2,498 (83.91%), Missing/Zero = 452 (15.18%), Outliers/Typos = 27 (0.91%)
- **Patient Height filling**: Valid = 2,525 (84.82%), Missing/Zero = 452 (15.18%), Outliers/Typos = 0 (0.00%)
- **Partner Weight filling**: Valid = 500 (16.80%), Missing/Zero = 2,466 (82.84%), Outliers/Typos = 11 (0.37%)
- **Partner Height filling**: Valid = 509 (17.10%), Missing/Zero = 2,468 (82.90%), Outliers/Typos = 0 (0.00%)

## Visualizations

### 1. Monthly Trends
![Monthly Filling Quality Trend](file:///G:/My Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/images/monthly_quality_trend.png)

### 2. Valid Distributions
![Value Distributions](file:///G:/My Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/images/value_distributions.png)

### 3. Doctor Quality Heatmap
![Doctor Quality Heatmap](file:///G:/My Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/images/doctor_quality_heatmap.png)

### 4. Annual Patient Quality Fill Rate
![Annual Patient Quality Fill Rate](file:///G:/My Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/images/annual_patient_filling_rate.png)

### 5. Historical Patient Quality Trend (2018 - 2026)
![Historical Patient Quality Trend](file:///G:/My Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/images/historical_patient_quality_trend.png)

## Repetition and Default Inputs Statistical Check

> [!IMPORTANT]
> Doctors flagged below show **high mode concentration (>=50%)** or **extremely low standard deviation (weight std < 1.0kg, height std < 0.01m)** despite having 10 or more filled treatments. This statistically suggests they may be copy-pasting default values rather than recording actual patient measurements.

*No doctors flagged with suspicious repetitive data entry.*

## Complete Doctor Quality & Quantity Table

| Doctor | N | Peso Pac Valid % | Alt Pac Valid % | Peso Conj Valid % | Alt Conj Valid % | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Matheus Teixeira Roque | 264 | 48.5% | 49.2% | 3.4% | 3.4% |  |
| Arnaldo Schizzi Cambiaghi | 256 | 89.5% | 89.8% | 0.8% | 0.8% |  |
| Daniella Spilborghs Castellotti | 209 | 99.0% | 100.0% | 80.4% | 81.3% |  |
| Eduardo Leme Alves da Motta | 155 | 62.6% | 62.6% | 8.4% | 8.4% |  |
| Thais Sanches Domingues | 145 | 84.1% | 84.8% | 17.9% | 20.0% |  |
| Claudia Gomes Padilla | 123 | 82.9% | 85.4% | 0.8% | 0.8% |  |
| Leci Veiga Caetano Amorim | 106 | 99.1% | 100.0% | 50.0% | 50.9% |  |
| Mauricio Chehin | 101 | 51.5% | 55.4% | 3.0% | 3.0% |  |
| Sofia Andrade de Oliveira | 86 | 55.8% | 55.8% | 12.8% | 12.8% |  |
| Erica Becker de Sousa Xavier | 82 | 73.2% | 73.2% | 7.3% | 7.3% |  |
| João Pedro Junqueira Caetano | 72 | 95.8% | 98.6% | 27.8% | 27.8% |  |
| Fernanda de Paula Rodrigues | 71 | 80.3% | 80.3% | 1.4% | 4.2% |  |
| Michele Quaranta Panzan  | 71 | 97.2% | 97.2% | 26.8% | 26.8% |  |
| Luciana Campomizzi Calazans | 65 | 100.0% | 100.0% | 12.3% | 12.3% |  |
| Fábio Costa Peixoto | 59 | 93.2% | 94.9% | 54.2% | 54.2% |  |
| Ana Paula Aquino | 58 | 67.2% | 67.2% | 0.0% | 0.0% |  |
| Beatriz Passaro Biscaro | 55 | 96.4% | 96.4% | 0.0% | 0.0% |  |
| Herica Cristina Mendonça | 52 | 100.0% | 100.0% | 1.9% | 1.9% |  |
| Livia Munhoz | 52 | 98.1% | 100.0% | 1.9% | 1.9% |  |
| SEM MEDICO | 48 | 97.9% | 97.9% | 0.0% | 0.0% |  |
| Gersia Araujo Viana | 46 | 100.0% | 100.0% | 54.3% | 54.3% |  |
| Hanna Park | 46 | 91.3% | 91.3% | 2.2% | 2.2% |  |
| Layza Borges | 46 | 97.8% | 97.8% | 2.2% | 2.2% |  |
| Ana Luiza Nunes | 44 | 79.5% | 79.5% | 4.5% | 4.5% |  |
| Laura Maria Almeida Maia | 42 | 97.6% | 97.6% | 2.4% | 2.4% |  |
| Raphaela Menin Franco Martins | 40 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Marcos Eiji Shiroma | 39 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Rogerio de Barros Ferreira Leao | 35 | 82.9% | 82.9% | 0.0% | 0.0% |  |
| Rafael Lacordia | 34 | 82.4% | 82.4% | 0.0% | 0.0% |  |
| Gabriella de Oliveira Ferreira | 33 | 100.0% | 100.0% | 3.0% | 3.0% |  |
| Joaquim Roberto Costa Lopes | 31 | 96.8% | 100.0% | 35.5% | 35.5% |  |
| Ricardo Mello Marinho | 31 | 74.2% | 74.2% | 12.9% | 12.9% |  |
| Frederico Jose Silva Correa | 28 | 92.9% | 100.0% | 3.6% | 3.6% |  |
| Carla Martins | 27 | 100.0% | 100.0% | 55.6% | 59.3% |  |
| Barbara Souza Melo | 22 | 100.0% | 100.0% | 63.6% | 63.6% |  |
| Dayana Couto | 20 | 95.0% | 95.0% | 0.0% | 0.0% |  |
| Bruna Costa Queiroz | 20 | 90.0% | 100.0% | 50.0% | 50.0% |  |
| Ana Luiza Mattos Tavares | 19 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Luciana Ferreira Potiguara Amador Sousa | 19 | 94.7% | 94.7% | 21.1% | 21.1% |  |
| Luana Lopes Toledo | 18 | 100.0% | 100.0% | 11.1% | 11.1% |  |
| Paula Bortolai Martins Araujo | 17 | 88.2% | 88.2% | 0.0% | 0.0% |  |
| Carolina de Andrade Melo e Souza | 16 | 93.8% | 93.8% | 6.2% | 6.2% |  |
| Camila Campos | 15 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Marjorie Fasolin  | 14 | 78.6% | 78.6% | 0.0% | 0.0% |  |
| Valentina Nascimento Cotrim | 13 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Fabyanne Mazutti da Silva  | 11 | 81.8% | 100.0% | 0.0% | 0.0% |  |
| Paula Vieira Nunes Brito | 11 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Bruna Cristina Lobo Santos | 10 | 100.0% | 100.0% | 30.0% | 30.0% |  |
| Pró-FIV | 10 | 100.0% | 100.0% | 60.0% | 60.0% |  |
| Simone Portugal Silva Lima | 9 | 100.0% | 100.0% | 88.9% | 88.9% |  |
| Alexander Kopelman | 8 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Renata Fioravanti Schaal | 7 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Josenice de Araujo SIlva Gomes | 6 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Marcelo Afonso Gonçalves | 6 | 100.0% | 100.0% | 66.7% | 66.7% |  |
| Melissa Cavagnoli | 6 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Giuliana Gatto | 6 | 100.0% | 100.0% | 33.3% | 33.3% |  |
| Gustavo Teles | 5 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Ana Claudia Moura Trigo | 4 | 75.0% | 75.0% | 75.0% | 75.0% |  |
| Lauriane G Schmidt De Abreu | 4 | 100.0% | 100.0% | 75.0% | 75.0% |  |
| Paula Beatriz Tavares Fettback | 4 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Victoria Furquim Werneck Marinho | 4 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Beatriz Pavin de Toledo | 3 | 66.7% | 66.7% | 0.0% | 0.0% |  |
| Renato Fraietta | 2 | 100.0% | 100.0% | 100.0% | 100.0% |  |
| Ricardo Barini | 2 | 50.0% | 50.0% | 0.0% | 0.0% |  |
| Gustavo Comodo | 2 | 0.0% | 100.0% | 0.0% | 0.0% |  |
| Vitoria Guelli Ambrosio | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Nina Rotsen Santos Ferreira | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Priscilla Lopes Caldeira | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Pedro Paulo Bastos Filho | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Felipe Lazar | 1 | 100.0% | 100.0% | 100.0% | 100.0% |  |
| Médico Externo | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Beatriz Cabral Pires | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Tatiana Magalhães Aguiar  | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Patricia Santos Marques | 1 | 100.0% | 100.0% | 100.0% | 100.0% |  |
| Fernando Barboza De Lima  | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Amanda Oliveira Cutalo Prates | 1 | 100.0% | 100.0% | 0.0% | 0.0% |  |
| Geraldo Caldeira | 1 | 0.0% | 0.0% | 0.0% | 0.0% |  |
