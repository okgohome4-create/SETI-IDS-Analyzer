import pandas as pd

input_file = "ranked_candidates.csv"
output_file = "true_anomaly_candidates.csv"

df = pd.read_csv(input_file)

print("원본 후보 수:", len(df))
print("컬럼 목록:")
print(df.columns.tolist())

# 컬럼 자동 탐색
score_col = None
drift_col = None
freq_col = None
target_col = None

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

if score_col is None:
    raise ValueError("점수 컬럼을 찾지 못했습니다.")
if drift_col is None:
    raise ValueError("drift 컬럼을 찾지 못했습니다.")
if freq_col is None:
    raise ValueError("frequency 컬럼을 찾지 못했습니다.")
if target_col is None:
    raise ValueError("target 컬럼을 찾지 못했습니다.")

print("사용 점수 컬럼:", score_col)
print("사용 drift 컬럼:", drift_col)
print("사용 frequency 컬럼:", freq_col)
print("사용 target 컬럼:", target_col)

# 숫자형 변환
df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
df[drift_col] = pd.to_numeric(df[drift_col], errors="coerce")
df[freq_col] = pd.to_numeric(df[freq_col], errors="coerce")

# 그룹화용 컬럼
df["drift_group"] = df[drift_col].round(2)
df["freq_group"] = df[freq_col].round(0)   # 1 Hz 단위
df["cluster_key"] = df["freq_group"].astype(str) + "_" + df["drift_group"].astype(str)

# 클러스터 크기 계산
cluster_counts = df["cluster_key"].value_counts()
df["cluster_size"] = df["cluster_key"].map(cluster_counts)

print("\n상위 클러스터:")
print(cluster_counts.head(10))

# 1차 RFI 제거:
# 너무 큰 클러스터는 버림
filtered = df[df["cluster_size"] <= 3].copy()

print("\n1차 필터 후 후보 수:", len(filtered))

# 2차:
# 같은 target + 같은 freq_group + 같은 drift_group 에서는 최고 점수 하나만 남김
filtered = (
    filtered.sort_values(score_col, ascending=False)
            .groupby([target_col, "freq_group", "drift_group"], dropna=False)
            .head(1)
            .copy()
)

print("2차 필터 후 후보 수:", len(filtered))

# 최종 정렬
filtered = filtered.sort_values(score_col, ascending=False)

# 저장
filtered.to_csv(output_file, index=False)

print("\n최종 후보 저장:", output_file)
print("\n상위 20개 후보:")
show_cols = [target_col, "signal_id", score_col, drift_col, freq_col, "cluster_size"]
show_cols = [c for c in show_cols if c in filtered.columns]
print(filtered[show_cols].head(20).to_string(index=False))