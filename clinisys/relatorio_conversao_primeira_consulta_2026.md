# Relatório de Conversão de Primeiras Consultas (2026)
### Foco: Consulta de Reprodução Humana e Consulta de Preservação da Fertilidade
### Ordenação: Por Taxa de Conversão Decrescente

**Tabela Analisada:** `gold.extrato_atendimento_central`  
**Data de Extração:** 02/09/2026 20:35:47  

---

## 1. Regras de Negócio e Metodologia
1. **Foco Temporal:** Ano de 2026 (`EXTRACT(year FROM data) = 2026`).
2. **Procedimentos Executados:** Apenas agendamentos com atendimento confirmado e realizado (`chegou = 'Atendido'`).
3. **Escopo de Especialidades:** Foco exclusivo em **Consulta de Reprodução Humana** e **Consulta de Preservação da Fertilidade** (`1ª Consulta de Reprodução Humana` e `1ª Consulta Preservação da Fertilidade`).
4. **Critério de Conversão:**
   - **Convertido (Regra Geral):** O paciente (`prontuario`) realizou ao menos um procedimento executado em **data estritamente posterior** à primeira consulta (`data > data_primeira_consulta`).
   - **Não é Conversão:** Se o procedimento ocorreu no **mesmo dia** da consulta ou se os únicos procedimentos posteriores foram **ultrassons (US / USG / ecografia)**.
   - **Conversão para Tratamento (Métrica Adicional):** Paciente que avançou para procedimentos de ciclo (FIV, FET, aspiração folicular/punção, transferência embrionária, congelamento, biópsia, etc.).
5. **Ordenação das Tabelas:** Estritamente por **Taxa de Conversão (%)** decrescente.
6. **Tratamento de Lock:** O script monitora o banco DuckDB e aguarda ativamente caso o arquivo esteja bloqueado por rotina de ingestão.

---

## 2. Indicadores Gerais (2026)

| Indicador | Volume | Taxa (%) |
| :--- | :---: | :---: |
| **Total de Primeiras Consultas Executadas** | **3,266** | 100,00% |
| **Convertidas (Procedimentos Posteriores Não-US)** | **1,695** | **51.90%** |
| **Não Convertidas** | **1,571** | **48.10%** |
| *Conversão Específica para Tratamento (FIV/FET/Punção)* | *693* | *21.22%* |

---

## 3. Conversão por Profissional / Médico (Ordenado por Taxa de Conversão Decrescente)

| Médico | Total 1ªs Consultas | Convertidas | Não Convertidas | Taxa de Conversão Geral (%) | Conv. Tratamento (%) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| Manuela Sanches de Aragão Pinheiro | 1 | 1 | 0 | **100.00%** | 0.00% |
| Barbara Souza Melo | 40 | 31 | 9 | **77.50%** | 17.50% |
| Josenice de Araujo Silva Gomes | 8 | 6 | 2 | **75.00%** | 0.00% |
| Paula Vieira Nunes Brito | 8 | 6 | 2 | **75.00%** | 62.50% |
| Camila Campos | 17 | 11 | 6 | **64.71%** | 29.41% |
| Ana Nunes | 28 | 18 | 10 | **64.29%** | 25.00% |
| Mauricio B. Chehin | 148 | 95 | 53 | **64.19%** | 34.46% |
| Claudia Gomes | 145 | 93 | 52 | **64.14%** | 24.83% |
| Ana Aquino | 78 | 50 | 28 | **64.10%** | 29.49% |
| Gersia Araújo Viana | 73 | 46 | 27 | **63.01%** | 32.88% |
| Luciana Ferreira Potiguara Amador Sousa | 35 | 22 | 13 | **62.86%** | 8.57% |
| Ana Tavares | 45 | 28 | 17 | **62.22%** | 35.56% |
| João Pedro Junqueira Caetano | 80 | 49 | 31 | **61.25%** | 26.25% |
| Raphaela Menin Franco Martins | 38 | 23 | 15 | **60.53%** | 26.32% |
| Carla Martins Silva | 54 | 32 | 22 | **59.26%** | 22.22% |
| Hanna Park | 51 | 30 | 21 | **58.82%** | 21.57% |
| Livia Munhoz | 121 | 71 | 50 | **58.68%** | 21.49% |
| Matheus Teixeira Roque | 358 | 203 | 155 | **56.70%** | 29.05% |
| Valentina Nascimento Cotrim | 36 | 20 | 16 | **55.56%** | 13.89% |
| Giuliana Guimaraes Gatto | 13 | 7 | 6 | **53.85%** | 30.77% |
| Érica Becker de Sousa Xavier | 123 | 66 | 57 | **53.66%** | 25.20% |
| Thais Domingues | 155 | 83 | 72 | **53.55%** | 18.71% |
| Joaquim Roberto Costa Lopes | 15 | 8 | 7 | **53.33%** | 20.00% |
| Sofia Andrade De Oliveira | 144 | 75 | 69 | **52.08%** | 14.58% |
| Beatriz Passaro | 33 | 17 | 16 | **51.52%** | 12.12% |
| Frederico Jose Silva Correa | 33 | 17 | 16 | **51.52%** | 18.18% |
| Luciana Campomizzi Calazans de Paula | 146 | 73 | 73 | **50.00%** | 20.55% |
| Gabriella de Oliveira Ferreira | 25 | 12 | 13 | **48.00%** | 20.00% |
| Michele Panzan | 130 | 62 | 68 | **47.69%** | 23.08% |
| Rafael Lacordia | 70 | 33 | 37 | **47.14%** | 27.14% |
| Dayana Couto | 67 | 31 | 36 | **46.27%** | 5.97% |
| Fábio Costa Peixoto | 141 | 65 | 76 | **46.10%** | 10.64% |
| Marjorie Fasolin | 29 | 13 | 16 | **44.83%** | 20.69% |
| Ricardo Mello Marinho | 34 | 15 | 19 | **44.12%** | 17.65% |
| Laura Maria Almeida Maia | 48 | 21 | 27 | **43.75%** | 16.67% |
| Victoria Furquim Werneck Marinho | 16 | 7 | 9 | **43.75%** | 31.25% |
| Ana Cláudia Moura Trigo | 30 | 13 | 17 | **43.33%** | 13.33% |
| Bruna Costa Queiroz | 37 | 16 | 21 | **43.24%** | 16.22% |
| Marcos Shiroma | 19 | 8 | 11 | **42.11%** | 15.79% |
| Leci Veiga Caetano Amorim | 165 | 69 | 96 | **41.82%** | 16.97% |
| Pró-FIV Pró-FIV | 22 | 9 | 13 | **40.91%** | 9.09% |
| Carolina de Andrade Melo e Souza | 47 | 18 | 29 | **38.30%** | 14.89% |
| Hérica Cristina Mendonça | 143 | 51 | 92 | **35.66%** | 5.59% |
| Eduardo Motta | 137 | 48 | 89 | **35.04%** | 24.82% |
| Luana Lopes de Toledo | 35 | 12 | 23 | **34.29%** | 8.57% |
| Beatriz Pavin de Toledo | 7 | 2 | 5 | **28.57%** | 0.00% |
| Renata Fioravanti Schaal | 7 | 2 | 5 | **28.57%** | 0.00% |
| Fernanda Rodrigues | 29 | 7 | 22 | **24.14%** | 20.69% |
| Beatriz Cabral Pires | 1 | 0 | 1 | **0.00%** | 0.00% |
| Fabyanne Mazutti da Silva | 1 | 0 | 1 | **0.00%** | 0.00% |

---

## 4. Conversão por Unidade / Centro de Custos (Ordenado por Taxa de Conversão Decrescente)

| Unidade | Total 1ªs Consultas | Convertidas | Não Convertidas | Taxa Conversão Geral (%) | Conv. Tratamento (%) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| 2. HTT SP - Itaim | 257 | 160 | 97 | **62.26%** | 31.13% |
| 5. HTT Brasília | 280 | 163 | 117 | **58.21%** | 20.00% |
| 10. HTT SP - Bauru | 23 | 13 | 10 | **56.52%** | 26.09% |
| 8. HTT Salvador | 338 | 189 | 149 | **55.92%** | 18.93% |
| 3. HTT SP - ProFIV | 92 | 49 | 43 | **53.26%** | 19.57% |
| 1. HTT SP - Ibirapuera | 979 | 511 | 468 | **52.20%** | 25.74% |
| 11. HTT Goiânia | 4 | 2 | 2 | **50.00%** | 0.00% |
| 9. HTT SP - Alphaville | 29 | 14 | 15 | **48.28%** | 10.34% |
| 4. HTT SP - Campinas | 187 | 90 | 97 | **48.13%** | 13.37% |
| 7. HTT Belo Horizonte | 1,073 | 504 | 569 | **46.97%** | 17.61% |

---

## 5. Conversão por Tipo de Procedimento de 1ª Consulta

| Procedimento | Categoria | Total 1ªs Consultas | Convertidas | Taxa Conversão Geral (%) | Conv. Tratamento (%) |
| :--- | :--- | :---: | :---: | :---: | :---: |
| 1ª Consulta de Reprodução Humana | Reprodução Humana | 3,156 | 1,653 | **52.38%** | 21.13% |
| 1ª Consulta Preservação da Fertilidade | Preservação da Fertilidade | 110 | 42 | **38.18%** | 23.64% |
