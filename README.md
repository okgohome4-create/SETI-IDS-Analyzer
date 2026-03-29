# SETI-IDS-Analyzer
SETI signal analysis pipeline based on Intelligence Density (IDS) and TSDC framework. Includes RFI filtering and anomaly detection on real Breakthrough Listen data.

# SETI-IDS Analyzer

SETI signal analysis pipeline based on **Intelligence Density (IDS)** and **TSDC framework**.  
Includes RFI filtering and anomaly detection on real Breakthrough Listen data.

---

## Background

Traditional SETI approaches assume that advanced civilizations produce high-energy signatures (e.g., Dyson spheres).  
However, this work explores an alternative hypothesis:

> Advanced civilizations may minimize energy consumption while maximizing computational efficiency.

This leads to the concept of **Intelligence Density (IDS)**.

---

## Methodology

The pipeline processes real Breakthrough Listen data through the following steps:

1. Raw signal ingestion  
2. Data transformation into structured format  
3. IDS-based scoring  
4. RFI cluster detection and removal  
5. Extraction of non-repeating anomaly candidates  

---

## Key Signal Features

The model evaluates signals based on:

- Narrow bandwidth  
- Doppler drift rate  
- Signal structure complexity  
- Stability and periodicity  
- RFI likelihood  

---

## Results

From over **1.1 million signals**, the pipeline reduced:

- → 37 candidates (initial filtering)  
- → 28 candidates (after RFI cluster removal)  
- → **5 final anomaly candidates**  

These candidates exhibit:

- Non-repeating structure  
- High signal-to-noise ratio  
- Significant Doppler drift  
- Extremely narrow bandwidth  

---

## Final Candidates

The final anomaly candidates are stored in:

- `results/final_5_candidates.csv`

Intermediate filtered candidates are available in:

- `results/true_anomaly_candidates.csv`

---

## How to Run

Example workflow:

```bash

python convert_bl_data.py
python bl_ids_scanner.py rank --input real_bl_data.csv
python filter_candidates.py
python rfi_filter_candidates.py
python final_candidates.py

Repository Structure

src/        → analysis pipeline code  
results/    → anomaly detection outputs  
README.md   → project description

Data Source

Breakthrough Listen Open Data
https://seti.berkeley.edu/opendata

Interpretation

These signals do not confirm extraterrestrial origin,
but represent high-priority technosignature candidates.

They satisfy key characteristics expected from:

Artificial, efficiency-optimized signal systems

Author

Minjun Kim
Independent Researcher
