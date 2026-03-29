import pandas as pd

input_file = "ranked_candidates.csv"
output_file = "filtered_candidates.csv"

df = pd.read_csv(input_file)

print("원본 후보 수:", len(df))
print("컬럼 목록:")
print(df.columns.tolist())

# 점수 컬럼 자동 탐색
score_col = None
for c in df.columns:
    cl = c.lower()
    if cl == "score" or "overall" in cl:
        score_col = c
        break

if score_col is None:
    raise ValueError("점수 컬럼을 찾지 못했습니다. 위 컬럼 목록을 확인하세요.")

print("사용할 점수 컬럼:", score_col)

# drift / frequency 컬럼 자동 탐색
drift_col = None
freq_col = None
target_col = None

for c in df.columns:
    cl = c.lower()
    if drift_col is None and "drift" in cl:
        drift_col = c
    if freq_col is None and ("center_frequency" in cl or cl == "freq" or "frequency" in cl):
        freq_col = c
    if target_col is None and ("target_name" in cl or cl == "target"):
        target_col = c

if drift_col is None:
    raise ValueError("drift 컬럼을 찾지 못했습니다.")
if freq_col is None:
    raise ValueError("frequency 컬럼을 찾지 못했습니다.")
if target_col is None:
    raise ValueError("target 컬럼을 찾지 못했습니다.")

print("target 컬럼:", target_col)
print("drift 컬럼:", drift_col)
print("frequency 컬럼:", freq_col)

# 그룹용 보조 컬럼 생성
df["drift_group"] = pd.to_numeric(df[drift_col], errors="coerce").round(2)
df["freq_group"] = pd.to_numeric(df[freq_col], errors="coerce").round(0)

group_cols = [target_col, "drift_group", "freq_group"]

# 그룹 내 최고 점수만 남김
filtered_df = (
    df.sort_values(score_col, ascending=False)
      .groupby(group_cols, dropna=False)
      .head(1)
      .sort_values(score_col, ascending=False)
)

filtered_df.to_csv(output_file, index=False)

print("필터링 후 후보 수:", len(filtered_df))
print("\n상위 20개:")
print(filtered_df.head(20))