#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
定期统计易链申请数据。
功能：
1. 扫描仓库根目录及子目录下的所有 CSV 文件（排除 results 目录），支持 GBK/UTF‑8 编码。
2. 根据表头 “项目名”、“基于哪条manifest分支”、“申请时间” 进行汇总。
3. 统计上一自然月每个项目在不同 manifest 分支上的访问次数，并记录最近一次访问时间。
4. 将结果保存到 results/YYYY‑MM_summary.csv（UTF‑8 编码）。
"""

import os
from datetime import datetime, timedelta
import pandas as pd

def find_csv_files(repo_dir: str, skip_dir: str = "results"):
    csv_list = []
    for root, dirs, files in os.walk(repo_dir):
        # 跳过 results 目录，防止重复处理输出文件
        if skip_dir and os.path.basename(root) == skip_dir:
            continue
        for f in files:
            if f.lower().endswith(".csv"):
                csv_list.append(os.path.join(root, f))
    return csv_list

def load_and_concat(csv_paths):
    """读取多个 CSV 为 DataFrame，自动处理编码并统一列名。"""
    frames = []
    for path in csv_paths:
        # 尝试 GBK 和 UTF-8 读取
        for encoding in ["gbk", "utf-8"]:
            try:
                df = pd.read_csv(path, encoding=encoding)
                break
            except Exception:
                continue
        else:
            # 全部失败则跳过
            continue
        # 去掉列名中的换行/空格
        df.columns = [c.strip() for c in df.columns]
        # 保留我们关心的列
        if {"项目名", "基于哪条manifest分支", "申请时间"}.issubset(df.columns):
            frames.append(df[["项目名", "基于哪条manifest分支", "申请时间"]].copy())
        # 有些表头可能带有换行符 \n
        elif {"项目名", "基于哪条manifest分支\n", "申请时间"}.issubset(df.columns):
            frames.append(
                df[["项目名", "基于哪条manifest分支\n", "申请时间"]]
                .rename(columns={"基于哪条manifest分支\n": "基于哪条manifest分支"})
                .copy()
            )
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def get_previous_month_range(now_local: datetime):
    """根据当前日期返回上一自然月的开始和结束时间。"""
    this_month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = this_month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return prev_month_start, prev_month_end

def main():
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(repo_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    csv_files = find_csv_files(repo_dir, skip_dir="results")
    if not csv_files:
        print("No CSV files found.")
        return

    df_all = load_and_concat(csv_files)
    if df_all.empty:
        print("No valid CSV data with required columns.")
        return

    # 解析申请时间
    df_all["申请时间"] = pd.to_datetime(df_all["申请时间"], errors="coerce")

    # 获取上一自然月时间区间
    now_local = datetime.now()
    prev_start, prev_end = get_previous_month_range(now_local)

    # 过滤上一月数据
    mask = (df_all["申请时间"] >= prev_start) & (df_all["申请时间"] <= prev_end)
    recent_df = df_all[mask]

    # 汇总访问次数和最近访问时间
    summary = (
        recent_df.groupby(["项目名", "基于哪条manifest分支"])
        .agg(访问次数=("申请时间", "count"),
             最近一次访问时间=("申请时间", "max"))
        .reset_index()
        .sort_values(["项目名", "访问次数"], ascending=[True, False])
    )
    summary["最近一次访问时间"] = summary["最近一次访问时间"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 输出 CSV，文件名使用上一月年月
    output_name = f"{prev_start.strftime('%Y-%m')}_summary.csv"
    output_path = os.path.join(results_dir, output_name)
    summary.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved summary to {output_path}")

if __name__ == "__main__":
    main()
