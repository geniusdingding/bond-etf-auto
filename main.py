import os
import io
import re
import pandas as pd
from datetime import datetime

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ETF_PATH = os.path.join("config", "科创债名单.xlsx")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "科创债ETF_累计结果.xlsx")


def extract_date_from_filename(filename: str) -> str:
    """
    从文件名中提取8位数字日期，例如 20251124 -> 2025/11/24
    """
    basename = os.path.basename(filename)
    m = re.search(r"(\d{8})", basename)
    if not m:
        raise ValueError(f"❌ 文件名中未找到日期: {basename}")

    date_str = m.group(1)
    return f"{date_str[0:4]}/{date_str[4:6]}/{date_str[6:8]}"


def load_all_input_files():
    """
    获取 input 目录下所有 Excel 文件，按文件名日期排序返回
    """
    files = [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.lower().endswith((".xls", ".xlsx"))
    ]

    if not files:
        raise FileNotFoundError("❌ input 目录没有任何 Excel 文件，请先放入文件")

    # 提取日期并排序
    sorted_files = sorted(files, key=lambda x: extract_date_from_filename(x))
    print("✅ 将按以下顺序处理文件:")
    for f in sorted_files:
        print("   →", os.path.basename(f))

    return sorted_files


def load_or_init_result(df_template):
    """
    如果已有累计结果文件，则读取；
    否则用科创债名单初始化
    """
    if os.path.exists(OUTPUT_FILE):
        print(f"✅ 已找到累计结果，将加载: {OUTPUT_FILE}")
        return pd.read_excel(OUTPUT_FILE)
    else:
        print("✅ 未找到累计结果，将新建文件")
        return df_template.copy()


def process_single_file(file_path, df_result):
    """
    处理单个 input 文件并更新累计结果
    """
    date_col = extract_date_from_filename(file_path)
    print(f"✅ 开始处理 {os.path.basename(file_path)} → 日期列: {date_col}")

    with open(file_path, "rb") as f:
        file_stream = io.BytesIO(f.read())

    try:
        df_sh = pd.read_excel(file_stream, header=2)
    except:
        df_sh = pd.read_csv(file_stream, header=2, sep=None, engine="python")

    col_code = df_sh.columns[0]
    col_rate = df_sh.columns[2]

    df_sh = df_sh.dropna(subset=[col_code])
    df_sh[col_code] = pd.to_numeric(df_sh[col_code], errors="coerce").astype("Int64")
    df_result["基金代码"] = pd.to_numeric(df_result["基金代码"], errors="coerce").astype("Int64")

    rate_map = dict(zip(df_sh[col_code], df_sh[col_rate]))
    df_result[date_col] = df_result["基金代码"].map(rate_map)

    return df_result


def sort_columns(df):
    fixed_cols = ["基金代码", "基金简称"]
    date_cols = sorted([c for c in df.columns if c not in fixed_cols])
    return df[fixed_cols + date_cols]



if __name__ == "__main__":
    # 读取ETF名单
    df_template = pd.read_excel(ETF_PATH)
    df_template = df_template[["基金代码", "基金简称"]]


    # 初始化或读取累计文件
    df_result = load_or_init_result(df_template)

    # 依次处理所有 input 文件
    files = load_all_input_files()
    for f in files:
        df_result = process_single_file(f, df_result)

    # 排序列
    df_result = sort_columns(df_result)

    # 保存输出
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_result.to_excel(OUTPUT_FILE, index=False)

    print(f"\n🎉 全部处理完成！累计结果已更新 → {OUTPUT_FILE}\n")
