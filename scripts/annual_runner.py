# -*- coding: utf-8 -*-
"""
Yilian 年报脚本（年度/全量）
- 递归扫描仓库内 CSV（默认排除 results/、.github/ 等），合并去重
- 兼容两种表头：{"项目名","基于哪条manifest分支","申请时间"} 及其“带换行”的版本
- 支持 --year 参数（如 2025）；默认 ALL（分析 CSV 中的全部信息）
- 生成:
    results/<scope>_annual_summary.csv            # 按项目汇总
    results/<scope>_annual_by_branch.csv          # 按项目-分支汇总
    results/<scope>_annual_monthly.csv            # 月度分布（用于审视全年结构）
    results/<scope>_annual_report.xlsx            # 上述三张表合并成一个 Excel

- 可选：若设置 FEISHU_WEBHOOK_URL，则发送一个简要卡片通知（不强依赖签名）
- 可选：若存在 GITHUB_TOKEN，则自动 git 提交产物并 push（用于 Actions）

环境变量：
- FEISHU_WEBHOOK_URL    选填，飞书“自定义机器人”Webhook
- FEISHU_WEBHOOK_SECRET 选填，若机器人启用签名校验
- GITHUB_TOKEN          Actions 会默认注入；本地运行时可不设
"""

from __future__ import annotations
import argparse
import base64
import csv
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# -----------------------
# 基本配置
# -----------------------
ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "gbk", "gb2312"]
EXCLUDE_DIRS = {"results", ".github", ".git", ".venv", "venv", "__pycache__", ".idea", ".vscode"}

# 原始表头可能含换行，这里做标准化后的目标列名
CANON_COLS = {
    "项目名": ["项目名", "項目名", "项目", "项目名称", "項目名稱"],
    "基于哪条manifest分支": [
        "基于哪条manifest分支", "基于哪条manifest\n分支",
        "manifest分支", "分支", "所用分支"
    ],
    "申请时间": ["申请时间", "申请\n时间", "申請時間", "时间", "时间戳"]
}

DATE_FORMATS_HINT = [
    "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
    "%Y年%m月%d日", "%Y年%m月%d日 %H:%M:%S"
]


def find_repo_root(start: Path) -> Path:
    p = start.resolve()
    for _ in range(8):
        if (p / ".git").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start.resolve()


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # 去除表头中的换行/空白
    new_cols = []
    for c in df.columns:
        nc = str(c).replace("\n", "").replace("\r", "")
        nc = re.sub(r"\s+", "", nc)
        new_cols.append(nc)
    df.columns = new_cols

    # 字段对齐：把各种别名映射为标准名
    mapping = {}
    for std, aliases in CANON_COLS.items():
        for col in df.columns:
            if col in aliases:
                mapping[col] = std
    df = df.rename(columns=mapping)
    return df


def read_csv_any(path: Path) -> Optional[pd.DataFrame]:
    for enc in ENCODINGS_TO_TRY:
        try:
            df = pd.read_csv(path, encoding=enc)
            df = normalize_headers(df)
            return df
        except Exception:
            continue
    return None


def pick_date_series(df: pd.DataFrame) -> Optional[pd.Series]:
    if "申请时间" in df.columns:
        return df["申请时间"]
    # 兜底：尝试寻找最像日期的列
    date_like = [c for c in df.columns if re.search(r"时间|date|日期|time", c, re.I)]
    if date_like:
        return df[date_like[0]]
    return None


def to_datetime_safe(series: pd.Series) -> pd.Series:
    # 先试 pandas 自动解析
    s = pd.to_datetime(series, errors="coerce", utc=False, infer_datetime_format=True)

    # 再试手动常见格式
    mask = s.isna()
    if mask.any():
        raw = series[mask].astype(str)
        parsed = pd.Series([pd.NaT] * raw.shape[0], index=raw.index)
        for fmt in DATE_FORMATS_HINT:
            try:
                parsed2 = pd.to_datetime(raw, format=fmt, errors="coerce")
                parsed = parsed.fillna(parsed2)
            except Exception:
                pass
        s[mask] = parsed
    return s


def collect_csvs(repo_root: Path) -> List[Path]:
    csvs: List[Path] = []
    for p in repo_root.rglob("*.csv"):
        parts = set(p.relative_to(repo_root).parts)
        if parts & EXCLUDE_DIRS:
            continue
        # 排除明显的产物/历史/缓存
        if any(seg.startswith(".") for seg in parts):
            continue
        csvs.append(p)
    return csvs


def unify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # 需要的列：项目名、分支、申请时间
    # 若缺列，则补空列以便后续 groupby 不报错
    for need in ["项目名", "基于哪条manifest分支"]:
        if need not in df.columns:
            df[need] = pd.NA

    date_series = pick_date_series(df)
    if date_series is None:
        df["申请时间"] = pd.NaT
    else:
        df["申请时间"] = to_datetime_safe(date_series)

    # 仅保留三列 + 原始全量（以备后续扩展）
    keep = [c for c in df.columns if c in {"项目名", "基于哪条manifest分支", "申请时间"}]
    df = df[keep].copy()
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    # 去重依据：项目名-分支-时间 三元
    return df.drop_duplicates(subset=["项目名", "基于哪条manifest分支", "申请时间"])


def filter_scope(df: pd.DataFrame, scope: str) -> Tuple[pd.DataFrame, str]:
    """
    scope: "ALL" 或 "2025" 这样的年份
    返回 (过滤后的 df, 实际用于文件名的 scope_str)
    """
    if scope.upper() == "ALL":
        return df.copy(), "all"
    try:
        year = int(scope)
    except Exception:
        # 无法解析年份就当 ALL
        return df.copy(), "all"
    mask = df["申请时间"].dt.year == year
    return df[mask].copy(), str(year)


def group_summaries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # 1) 按项目汇总
    by_proj = (
        df.groupby("项目名", dropna=False)
        .agg(
            申请次数=("申请时间", "count"),
            首次申请时间=("申请时间", "min"),
            最近申请时间=("申请时间", "max"),
            分支种类数=("基于哪条manifest分支", "nunique"),
            最常用分支=("基于哪条manifest分支", lambda s: s.value_counts(dropna=True).idxmax() if s.dropna().shape[0] else pd.NA),
        )
        .sort_values(["申请次数", "最近申请时间"], ascending=[False, False])
        .reset_index()
    )

    # 2) 按项目-分支汇总
    by_proj_branch = (
        df.groupby(["项目名", "基于哪条manifest分支"], dropna=False)
        .agg(
            次数=("申请时间", "count"),
            首次时间=("申请时间", "min"),
            最近时间=("申请时间", "max"),
        )
        .sort_values(["次数", "最近时间"], ascending=[False, False])
        .reset_index()
    )

    # 3) 月度分布（用于看全年分布结构）
    month_col = df["申请时间"].dt.to_period("M").astype(str)
    by_month = (
        df.assign(月份=month_col)
        .groupby("月份")
        .size()
        .rename("条目数")
        .reset_index()
        .sort_values("月份")
    )

    return by_proj, by_proj_branch, by_month


def ensure_results_dir(repo_root: Path) -> Path:
    out = repo_root / "results"
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_outputs(out_dir: Path, scope_str: str,
                 by_proj: pd.DataFrame,
                 by_proj_branch: pd.DataFrame,
                 by_month: pd.DataFrame) -> List[Path]:
    csv1 = out_dir / f"{scope_str}_annual_summary.csv"
    csv2 = out_dir / f"{scope_str}_annual_by_branch.csv"
    csv3 = out_dir / f"{scope_str}_annual_monthly.csv"
    xlsx = out_dir / f"{scope_str}_annual_report.xlsx"

    by_proj.to_csv(csv1, index=False, encoding="utf-8-sig")
    by_proj_branch.to_csv(csv2, index=False, encoding="utf-8-sig")
    by_month.to_csv(csv3, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx, engine="openpyxl") as writer:
        by_proj.to_excel(writer, index=False, sheet_name="按项目汇总")
        by_proj_branch.to_excel(writer, index=False, sheet_name="按项目-分支")
        by_month.to_excel(writer, index=False, sheet_name="月度分布")

    return [csv1, csv2, csv3, xlsx]


# -----------------------
# 可选：飞书通知
# -----------------------
def feishu_sign(secret: str, timestamp: str) -> str:
    # 飞书机器人签名（若启用），签名算法版本可能有差异；此实现覆盖 v2 常见用法
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    h = hmac.new(string_to_sign, b"", digestmod=hashlib.sha256)
    return base64.b64encode(h.digest()).decode("utf-8")


def send_feishu_card(total_rows: int, proj_cnt: int, scope_str: str, files: List[Path]) -> None:
    url = os.getenv("FEISHU_WEBHOOK_URL")
    if not url:
        print("[Feishu] FEISHU_WEBHOOK_URL 未设置，跳过发送。")
        return
    secret = os.getenv("FEISHU_WEBHOOK_SECRET")
    ts = str(int(time.time()))

    file_lines = "\n".join([f"- {p.as_posix()}" for p in files])

    content = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": f"Yilian 年度统计（{scope_str}）"}},
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md",
                             "content": f"**合并总行数**：{total_rows}\n**项目数**：{proj_cnt}\n**产物**：\n{file_lines}"}
                }
            ]
        }
    }

    payload = content
    if secret:
        sign = feishu_sign(secret, ts)
        payload.update({"timestamp": ts, "sign": sign})

    import urllib.request
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print("[Feishu] 状态：", resp.status)
    except Exception as e:
        print("[Feishu] 发送失败：", e)


# -----------------------
# 可选：Git 提交
# -----------------------
def git_commit_and_push(repo_root: Path, message: str) -> None:
    # 若在 Actions 内，GITHUB_TOKEN 已注入，'actions/checkout' 会配置好权限；本地无 token 也可提交到本地分支
    def run(cmd: List[str]) -> None:
        print("+", " ".join(cmd))
        subprocess.check_call(cmd, cwd=str(repo_root))

    try:
        run(["git", "config", "user.name", "actions-user"])
        run(["git", "config", "user.email", "actions@github.com"])
        run(["git", "add", "results"])
        run(["git", "commit", "-m", message])
        run(["git", "push"])
    except subprocess.CalledProcessError as e:
        print("[Git] 提交/推送失败（可能无变更或本地环境缺失 git 凭据）：", e)


# -----------------------
# 主流程
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Yilian 年度/全量统计")
    parser.add_argument("--year", default="ALL",
                        help="年份（如 2025），默认 ALL=不按年筛选、对 CSV 全量统计")
    args = parser.parse_args()

    here = Path(__file__).resolve()
    repo_root = find_repo_root(here)
    results_dir = ensure_results_dir(repo_root)

    csv_paths = collect_csvs(repo_root)
    if not csv_paths:
        print("[Err] 未发现 CSV 数据。")
        sys.exit(1)

    frames = []
    for p in csv_paths:
        df = read_csv_any(p)
        if df is None or df.empty:
            print(f"[Skip] 无法读取或空文件: {p}")
            continue
        df = unify_dataframe(df)
        frames.append(df)

    if not frames:
        print("[Err] 无有效数据。")
        sys.exit(2)

    merged = pd.concat(frames, ignore_index=True)
    merged = deduplicate(merged)
    # 清掉没有时间戳的行（按需保留也可以，这里默认丢弃）
    merged = merged[merged["申请时间"].notna()].copy()

    scoped, scope_str = filter_scope(merged, args.year)
    if scoped.empty:
        print(f"[Warn] 在范围 {args.year} 内没有数据。输出空表。")
        # 仍然输出空产物以便流程完整
        by_proj = pd.DataFrame(columns=["项目名","申请次数","首次申请时间","最近申请时间","分支种类数","最常用分支"])
        by_pb = pd.DataFrame(columns=["项目名","基于哪条manifest分支","次数","首次时间","最近时间"])
        by_month = pd.DataFrame(columns=["月份","条目数"])
    else:
        by_proj, by_pb, by_month = group_summaries(scoped)

    files = save_outputs(results_dir, scope_str, by_proj, by_pb, by_month)

    # 飞书通知（可选）
    total_rows = int(merged.shape[0])
    proj_cnt = int(scoped["项目名"].nunique(dropna=True)) if not scoped.empty else 0
    send_feishu_card(total_rows, proj_cnt, scope_str, files)

    # Git 提交（可选）
    git_commit_and_push(repo_root, f"chore: Yilian 年度统计产物（{scope_str}）")

    print("[Done] OK.")


if __name__ == "__main__":
    main()
