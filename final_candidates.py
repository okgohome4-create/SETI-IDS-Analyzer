import pandas as pd

input_file = "true_anomaly_candidates.csv"
output_file = "final_5_candidates.csv"

df = pd.read_csv(input_file)

print("원본 anomaly 후보 수:", len(df))
print("컬럼 목록:")
print(df.columns.tolist())

# 컬럼 자동 탐색
score_col = None
drift_col = None
freq_col = None
target_col = None
cluster_col = None
snr_col = None

for c in df.columns:
    cl = c.lower()
    if score_col is None and (cl == "score" or "overall" in cl):
        score_col = c
    if drift_col is None and "drift" in cl:
        drift_col = c
    if freq_col is None and ("center_frequency" in cl or cl == "freq" or "frequency" in cl):
        freq_col = c
    if target_col is None and ("target_name" in cl or cl == "target"):
        target_col = c
    if cluster_col is None and "cluster_size" in cl:
        cluster_col = c
    if snr_col is None and cl == "snr":
        snr_col = c

if score_col is None:
    raise ValueError("점수 컬럼을 찾지 못했습니다.")
if drift_col is None:
    raise ValueError("drift 컬럼을 찾지 못했습니다.")
if freq_col is None:
    raise ValueError("frequency 컬럼을 찾지 못했습니다.")
if target_col is None:
    raise ValueError("target 컬럼을 찾지 못했습니다.")
if cluster_col is None:
    raise ValueError("cluster_size 컬럼을 찾지 못했습니다.")

print("점수 컬럼:", score_col)
print("drift 컬럼:", drift_col)
print("freq 컬럼:", freq_col)
print("target 컬럼:", target_col)
print("cluster 컬럼:", cluster_col)

# 숫자형 보정
df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
df[drift_col] = pd.to_numeric(df[drift_col], errors="coerce")
df[freq_col] = pd.to_numeric(df[freq_col], errors="coerce")
df[cluster_col] = pd.to_numeric(df[cluster_col], errors="coerce")

if snr_col is not None:
    df[snr_col] = pd.to_numeric(df[snr_col], errors="coerce")

# drift 절대값
df["abs_drift"] = df[drift_col].abs()

# 1차: cluster_size == 1 우선
single_df = df[df[cluster_col] == 1].copy()

print("\ncluster_size == 1 후보 수:", len(single_df))

# 너무 drift가 0에 가까운 건 제외하고 싶으면 기준 추가
# 여기선 abs(drift) >= 0.1 만 우선 사용
strong_df = single_df[single_df["abs_drift"] >= 0.1].copy()

print("abs(drift) >= 0.1 후보 수:", len(strong_df))

# strong_df가 너무 적으면 single_df로 fallback
candidate_df = strong_df if len(strong_df) >= 5 else single_df.copy()

# 정렬 기준:
# 1) overall_score 높은 순
# 2) abs_drift 큰 순
# 3) snr 높은 순 (있으면)
sort_cols = [score_col, "abs_drift"]
ascending = [False, False]

if snr_col is not None:
    sort_cols.append(snr_col)
    ascending.append(False)

candidate_df = candidate_df.sort_values(sort_cols, ascending=ascending)

# 같은 target이 여러 번 있으면 최고 하나만 남김
candidate_df = (
    candidate_df.groupby(target_col, dropna=False)
    .head(1)
    .copy()
)

# 최종 5개
final_df = candidate_df.head(5).copy()

final_df.to_csv(output_file, index=False)

print("\n최종 후보 저장:", output_file)
show_cols = [target_col]
for c in ["signal_id", score_col, drift_col, freq_col, cluster_col, "abs_drift"]:
    if c in final_df.columns:
        show_cols.append(c)
if snr_col is not None and snr_col not in show_cols:
    show_cols.append(snr_col)

print("\n최종 5개 후보:")
print(final_df[show_cols].to_string(index=False))