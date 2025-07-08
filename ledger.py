import os
import pandas as pd
from datetime import datetime


def parse_mixed_date(series):
    """Parse various date formats including Excel serials and yyyymmdd strings."""
    import re

    def convert(value):
        if pd.isna(value) or value == "":
            return pd.NaT

        text = str(value).strip()

        if re.fullmatch(r"\d{8}", text):
            try:
                return pd.to_datetime(text, format="%Y%m%d")
            except Exception:
                pass

        if re.fullmatch(r"\d{4,6}", text):
            try:
                return pd.to_datetime(float(text), unit="D", origin="1899-12-30")
            except Exception:
                pass

        try:
            return pd.to_datetime(text)
        except Exception:
            return pd.NaT

    return series.apply(convert).dt.floor("D")

# ========== 路径和文件识别 ==========
root_path = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else "./"
files = [f for f in os.listdir(root_path) if f.endswith(('.xls', '.xlsx')) and not f.startswith('~$')]

product_file = next((f for f in files if '产品查询' in f), None)
nv_files = [f for f in files if '产品查询' not in f]

assert product_file, "未找到包含“产品查询”的文件"
assert nv_files, "未找到净值数据文件"

# ========== 读取产品查询表 ==========
df_product = pd.read_excel(os.path.join(root_path, product_file), sheet_name="产品列表", header=8)
df_product.columns = df_product.columns.str.strip()

df_product = df_product.rename(columns={
    '发行机构销售代码': '产品代码',
    '首次募集开始日期': '募集开始日期',
    '首次募集结束日期': '募集结束日期',
    '最早实际成立日期': '成立日',
    '最早实际结束日期': '到期日'
})

# 处理产品表中的日期格式
for _col in ['募集开始日期', '募集结束日期', '成立日', '到期日']:
    if _col in df_product.columns:
        df_product[_col] = parse_mixed_date(df_product[_col])

# ========== 读取净值数据 ==========
nv_dfs = []
for f in nv_files:
    path = os.path.join(root_path, f)
    df_dict = pd.read_excel(path, sheet_name=None, header=2)
    for sheet in df_dict.values():
        sheet.columns = sheet.columns.str.strip()
        sheet = sheet.loc[:, ~sheet.columns.duplicated()]
        # 标准化字段名
        if '产品代码' not in sheet.columns:
            sheet.rename(columns={sheet.columns[0]: '产品代码'}, inplace=True)
        if '最新单位净值' not in sheet.columns:
            alt = [col for col in sheet.columns if '单位净值' in col]
            if alt:
                sheet.rename(columns={alt[0]: '最新单位净值'}, inplace=True)
        if '汇总日期' in sheet.columns:
            sheet.rename(columns={'汇总日期': '规模计算日期'}, inplace=True)
        nv_dfs.append(sheet)

df_nv_all = pd.concat(nv_dfs, ignore_index=True)

# 处理净值表中的日期格式
if '规模计算日期' in df_nv_all.columns:
    df_nv_all['规模计算日期'] = parse_mixed_date(df_nv_all['规模计算日期'])

# ========== 合并产品数据 ==========
df_merged = pd.merge(df_nv_all, df_product, how='left', on='产品代码')
# 保留净值表字段，重命名为标准名称
df_merged.rename(columns={
    '最新单位净值_x': '最新单位净值',
    '最新单位净值_y': '备用单位净值',
}, inplace=True)



# ========== 字段清理和日期转换 ==========
for _col in ['规模计算日期', '成立日', '募集开始日期', '募集结束日期', '到期日']:
    if _col in df_merged.columns:
        df_merged[_col] = parse_mixed_date(df_merged[_col])

# ========== 年化收益率计算 ==========
def calc_annualized_return(row):
    try:
        days = (row['规模计算日期'] - row['成立日']).days
        if days <= 0 or pd.isna(row['最新单位净值']):
            return None
        return (pow(float(row['最新单位净值']), 365 / days) - 1) * 100
    except:
        return None

df_merged['成立以来年化收益率（%）'] = df_merged.apply(calc_annualized_return, axis=1)
df_merged['最新累计净值'] = df_merged['最新单位净值']

# ========== 披露日期识别 ==========
def assign_nav_disclosure_dates(df: pd.DataFrame) -> pd.Series:
    """Return a Series with the most recent NAV disclosure date for each row.

    For closed-end funds the date corresponds to the latest scale date where the
    unit NAV changed.  For open-end funds it is simply the scale calculation
    date.  The logic assumes ``df`` contains ``产品代码``, ``运作模式``, ``规模计算日期``
    and ``最新单位净值`` columns.
    """
    result = pd.Series(index=df.index, dtype="datetime64[ns]")
    for code, group in df.groupby('产品代码'):
        group_sorted = group.sort_values('规模计算日期')
        is_open = group_sorted['运作模式'].astype(str).str.contains('开放式', na=False)
        last_nav = None
        last_date = None
        dates = []
        for idx, row in group_sorted.iterrows():
            if is_open.loc[idx]:
                last_nav = row['最新单位净值']
                last_date = row['规模计算日期']
            else:
                current_nav = row['最新单位净值']
                if pd.isna(current_nav):
                    current_nav = last_nav
                if last_nav is None or current_nav != last_nav:
                    last_nav = current_nav
                    last_date = row['规模计算日期']
            dates.append(last_date)
        result.loc[group_sorted.index] = dates
    return result

df_merged['最新净值日期'] = assign_nav_disclosure_dates(df_merged)

# ========== 字段选择与重命名 ==========
final_columns = [
    '规模计算日期', '产品代码', '产品名称', '运作模式', '开放类型', '风险等级', '投资性质二级',
    '募集开始日期', '募集结束日期', '成立日', '到期日', '投资周期（天）',
    '业绩比较基准（%）', '当前业绩比较基准下限（%）', '当前业绩比较基准上限（%）',
    '最新销售费(%)', '最新固定管理费(%)',
    '实际募集总规模', '折合人民币实际募集总规模', '产品市值', '产品市值',
    '成立以来年化收益率（%）', '最新单位净值', '最新累计净值', '最新净值日期',
    '销售商名称', '销售对象', '产品系列', '募集方式', '募集币种'
]
df_merged = df_merged[final_columns]

df_merged.columns = [
    '规模计算日期', '产品代码（发行机构销售代码）', '产品名称', '运作模式', '开放类型', '风险等级', '投资类型（投资性质二级）',
    '募集开始日期', '募集结束日期', '成立日', '到期日', '期限（天）',
    '业绩比较基准（%）', '当前业绩比较基准下限（%）', '当前业绩比较基准上限（%）',
    '最新销售费（%）', '最新固定管理费（%）',
    '实际募集总规模（亿元）', '折合人民币实际募集总规模（亿元）', '计算日存续规模（亿元）', '折合人民币计算日存续规模（亿元）',
    '成立以来年化收益率（%）', '最新单位净值', '最新累计净值', '最新净值日期',
    '代销机构', '销售对象', '产品系列', '募集方式', '募集币种'
]

# 按照字段名将规模相关数据转换为亿元
for _col in ['实际募集总规模（亿元）', '折合人民币实际募集总规模（亿元）', '计算日存续规模（亿元）', '折合人民币计算日存续规模（亿元）']:
    if _col in df_merged.columns:
        df_merged[_col] = pd.to_numeric(df_merged[_col], errors='coerce') / 1e8

# ========== 按年份输出 ==========
df_merged['年份'] = df_merged['规模计算日期'].dt.year
year_groups = df_merged.groupby('年份')

# 日期统一为 yyyy-mm-dd 格式
for _col in ['规模计算日期', '募集开始日期', '募集结束日期', '成立日', '到期日', '最新净值日期']:
    if _col in df_merged.columns:
        df_merged[_col] = df_merged[_col].dt.strftime('%Y-%m-%d')

output_path = os.path.join(root_path, "产品达标分析结果.xlsx")
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    if len(year_groups) == 1:
        df_merged.drop(columns='年份').to_excel(writer, sheet_name="产品达标分析结果", index=False)
    else:
        for year, group in year_groups:
            group.drop(columns='年份').to_excel(writer, sheet_name=str(year), index=False)

print(f"✅ 分析结果已保存至：{output_path}")
