import pandas as pd

input_file = "AAA_candidates.v4_1492476400.csv"
output_file = "real_bl_data.csv"

df = pd.read_csv(input_file, low_memory=False)

print("컬럼 목록:")
print(df.columns.tolist())

new_df = pd.DataFrame()

# target_name: Hit_ID를 그대로 사용
new_df["target_name"] = df["Hit_ID"].astype(str)

# signal_id도 동일하게 사용
new_df["signal_id"] = df["Hit_ID"].astype(str)

# 실측값
new_df["snr"] = pd.to_numeric(df["SNR"], errors="coerce").fillna(0)
new_df["drift_rate_hz_s"] = pd.to_numeric(df["DriftRate"], errors="coerce").fillna(0)
new_df["center_frequency_hz"] = pd.to_numeric(df["Freq"], errors="coerce").fillna(0)

freq_end = pd.to_numeric(df["FreqEnd"], errors="coerce").fillna(0)
freq_start = pd.to_numeric(df["FreqStart"], errors="coerce").fillna(0)
new_df["bandwidth_hz"] = (freq_end - freq_start).abs()

# 0 이하 bandwidth 방지
new_df.loc[new_df["bandwidth_hz"] <= 0, "bandwidth_hz"] = 1.0

# rfi_score 자동 탐색
rfi_col = None
for col in df.columns:
    if "rfi" in col.lower():
        rfi_col = col
        break

if rfi_col is not None:
    new_df["rfi_score"] = pd.to_numeric(df[rfi_col], errors="coerce").fillna(0.5)
else:
    new_df["rfi_score"] = 0.5

# 보조값
new_df["complexity_score"] = 0.5
new_df["distance_pc"] = 10.0
new_df["modulation_score"] = 0.5
new_df["periodicity_score"] = 0.5
new_df["stability_score"] = 0.5

new_df.to_csv(output_file, index=False)

print("\n변환 완료:", output_file)
print(new_df.head())
print("\n총 행 수:", len(new_df))