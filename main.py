import os
import io
import re
import pandas as pd
from datetime import datetime
import requests
import json
import collections

# ================= 配置区域 =================
INPUT_DIR = "input"       # 放入每天下载的 xls 文件的目录
OUTPUT_DIR = "output"     # 结果保存目录
ETF_PATH = os.path.join("config", "科创债名单.xlsx")  # 您的名单模板路径
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "科创债ETF_累计结果.xlsx")
# 请替换为您的真实飞书 Webhook
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/fc7e6de2-fa45-4c14-96ac-c7bda5874732"
# ===========================================

def load_push_config():
    cfg_path = "config.json"
    if not os.path.exists(cfg_path):
        return False
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            return cfg.get("push_enabled", False)
    except:
        return False

def extract_date_from_filename(filename: str) -> str:
    """
    从文件名中提取8位数字日期，例如 20251124 -> 2025/11/24
    """
    basename = os.path.basename(filename)
    m = re.search(r"(\d{8})", basename)
    if not m:
        return None
    date_str = m.group(1)
    return f"{date_str[0:4]}/{date_str[4:6]}/{date_str[6:8]}"

def group_files_by_date():
    """
    扫描 input 目录，按日期将文件分组
    返回格式: { '2025/11/26': ['input/上海...xls', 'input/深圳...xls'], ... }
    """
    files_map = collections.defaultdict(list)
    
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
        print(f"⚠️ 目录 {INPUT_DIR} 不存在，已自动创建，请放入 xls 文件。")
        return {}

    raw_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith((".xls", ".xlsx", ".csv"))]
    
    if not raw_files:
        print("⚠️ input 目录没有任何 Excel 文件")
        return {}

    for f in raw_files:
        date_str = extract_date_from_filename(f)
        if date_str:
            full_path = os.path.join(INPUT_DIR, f)
            files_map[date_str].append(full_path)

    # 按日期排序
    sorted_dates = sorted(files_map.keys())
    print(f"✅ 扫描到 {len(sorted_dates)} 个日期的文件待处理")
    
    return {date: files_map[date] for date in sorted_dates}

def read_file_data(file_path):
    """
    读取单个文件，返回 {代码: 折算率} 的字典
    自动判断是上海还是深圳格式，并统一单位为整数
    """
    filename = os.path.basename(file_path)
    
    # === 1. 判断交易所格式 ===
    if "深圳" in filename:
        header_row = 4
        print(f"   → 读取深圳文件 (Header=5): {filename}")
    else:
        header_row = 2
        print(f"   → 读取上海文件 (Header=3): {filename}")

    # === 2. 读取文件内容 ===
    with open(file_path, "rb") as f:
        file_stream = io.BytesIO(f.read())

    try:
        # 尝试读 Excel
        df = pd.read_excel(file_stream, header=header_row)
    except:
        # 失败则尝试读 CSV
        file_stream.seek(0)
        try:
            df = pd.read_csv(file_stream, header=header_row, sep=None, engine="python", encoding='gbk')
        except:
            df = pd.read_csv(file_stream, header=header_row, sep=None, engine="python", encoding='utf-8')

    # === 3. 动态寻找列名 ===
    cols = df.columns.tolist()
    col_code = next((c for c in cols if '代码' in str(c)), None)
    col_rate = next((c for c in cols if '折算' in str(c)), None)

    if not col_code or not col_rate:
        # 兜底策略
        col_code = cols[0]
        col_rate = cols[2] if len(cols) > 2 else cols[1]

    # === 4. 清洗与格式统一 ===
    df = df.dropna(subset=[col_code])
    
    # 转换代码为数字
    df[col_code] = pd.to_numeric(df[col_code], errors="coerce")
    df = df.dropna(subset=[col_code])
    df[col_code] = df[col_code].astype("Int64")
    
    # 转换折算率为数字
    df[col_rate] = pd.to_numeric(df[col_rate], errors="coerce")
    
    # ⚡️⚡️⚡️ 核心修正：深圳数据 x100 ⚡️⚡️⚡️
    if "深圳" in filename:
        print(f"     ⚡️ 检测到深圳数据，执行 x100 修正")
        df[col_rate] = df[col_rate] * 100
    
    # 四舍五入并转为整数
    df[col_rate] = df[col_rate].round(0).astype("Int64")
    
    return dict(zip(df[col_code], df[col_rate]))

def process_date_group(date_str, file_list, df_result):
    print(f"📅 开始处理日期: {date_str}")
    combined_map = {}
    
    for file_path in file_list:
        try:
            file_map = read_file_data(file_path)
            combined_map.update(file_map)
        except Exception as e:
            print(f"❌ 读取文件失败 {os.path.basename(file_path)}: {e}")

    df_result["基金代码"] = pd.to_numeric(df_result["基金代码"], errors="coerce").astype("Int64")
    df_result[date_str] = df_result["基金代码"].map(combined_map)
    return df_result

def sort_columns(df):
    fixed_cols = ["基金代码", "基金简称"]
    date_cols = sorted([c for c in df.columns if c not in fixed_cols])
    return df[fixed_cols + date_cols]

def send_to_feishu(file_name, summary_text=None, df_preview=None):
    """
    发送飞书消息，增加数据预览 (df_preview)
    """
    raw_url = f"https://raw.githubusercontent.com/geniusdingding/bond-etf-auto/auto-updates/output/{file_name}"
    
    # 构造预览文本 (抽查几个关键ETF)
    preview_text = ""
    if df_preview is not None:
        cols = df_preview.columns.tolist()
        date_cols = [c for c in cols if c not in ["基金代码", "基金简称"]]
        if date_cols:
            latest_date = date_cols[-1]
            # 这里的代码对应: 511120(广发), 159400(平安), 159200(富国)
            target_codes = [511120, 159400, 159200]
            
            # 筛选
            preview_rows = df_preview[df_preview['基金代码'].isin(target_codes)]
            
            if not preview_rows.empty:
                preview_text = "\n\n🔎 关键ETF抽查 (验证整数):\n"
                for _, row in preview_rows.iterrows():
                    val = row[latest_date]
                    val_str = str(val) if pd.notna(val) else "无数据"
                    preview_text += f"• {row['基金简称']}: {val_str}\n"

    # 构造完整文案
    full_content = (summary_text or "✅ 数据已更新") + preview_text
    
    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "📊 科创债折算率自动更新",
                    "content": [
                        [{"tag": "text", "text": full_content}],
                        [{"tag": "a", "text": "📎 点击下载最新累计表格 (Git同步中)", "href": raw_url}]
                    ]
                }
            }
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, data=json.dumps(data), headers={"Content-Type": "application/json"})
        print("✅ 飞书推送结果:", resp.text)
    except Exception as e:
        print("❌ 飞书推送失败:", e)

if __name__ == "__main__":
    # 1. 读取开关配置
    push_enabled = load_push_config()

    # 2. 读取名单模板
    if not os.path.exists(ETF_PATH):
        if os.path.exists("科创债名单.xlsx"):
             ETF_PATH = "科创债名单.xlsx"
        else:
             raise FileNotFoundError(f"❌ 找不到配置文件: {ETF_PATH}")
    
    df_template = pd.read_excel(ETF_PATH)[["基金代码", "基金简称"]]

    # 3. 加载或新建结果表
    if os.path.exists(OUTPUT_FILE):
        print(f"✅ 加载历史文件: {OUTPUT_FILE}")
        df_result = pd.read_excel(OUTPUT_FILE)
    else:
        print("✅ 初始化新文件")
        df_result = df_template.copy()

    # 4. 扫描文件并处理
    grouped_files = group_files_by_date()
    
    # 如果没有文件，脚本结束，不发消息
    if not grouped_files:
        print("⚠️ 没有需要处理的文件，脚本结束")
    else:
        for date_str, files in grouped_files.items():
            df_result = process_date_group(date_str, files, df_result)

        # 5. 保存结果
        df_result = sort_columns(df_result)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df_result.to_excel(OUTPUT_FILE, index=False)
        print(f"🎉 累计结果已保存: {OUTPUT_FILE}")

        # 6. 生成摘要并推送 (这里不再做 Git 提交，只负责通知)
        cols = df_result.columns.tolist()
        date_cols = [c for c in cols if c not in ["基金代码", "基金简称"]]
        
        if date_cols:
            latest_date = date_cols[-1]
            valid_data = df_result[latest_date].dropna()
            count = len(valid_data)
            avg_rate = round(valid_data.mean(), 2) if count > 0 else 0
            
            summary = (f"📅 更新日期: {latest_date}\n"
                       f"📈 可质押ETF数量: {count} 只\n"
                       f"💰 平均折算率: {avg_rate}")
            
            print(f"\n摘要信息:\n{summary}\n")

            if push_enabled:
                # 传入 df_result 以生成预览
                send_to_feishu("科创债ETF_累计结果.xlsx", summary, df_result)
                print("🚀 已执行飞书推送")
            else:
                print("✅ push_enabled=False → 跳过飞书推送")

    # 7. Git 操作已完全移除，交由 YAML 接管