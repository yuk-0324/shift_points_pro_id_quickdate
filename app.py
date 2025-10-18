
import os
import sqlite3
from pathlib import Path
from datetime import date, timedelta
import datetime as _dt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
ADMIN_PIN = os.getenv("ADMIN_PIN", "1234")

st.set_page_config(page_title="シフトポイント（ID・日付自動化）", layout="wide")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "app.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS roster (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT UNIQUE,
            name TEXT NOT NULL UNIQUE,
            grp TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,
            shift TEXT NOT NULL,
            emp_id TEXT NOT NULL,
            grp TEXT NOT NULL,
            points REAL NOT NULL CHECK(points >= 0),
            memo TEXT,
            UNIQUE(d, shift, emp_id)
        );
        CREATE TABLE IF NOT EXISTS locks (
            ym TEXT PRIMARY KEY
        );
        """)
        if conn.execute("SELECT COUNT(*) FROM shifts").fetchone()[0] == 0:
            conn.executemany("INSERT INTO shifts(name) VALUES(?)", [("早番",),("中番",),("遅番",)])

def ensure_onboarding():
    with get_conn() as conn:
        if conn.execute("SELECT COUNT(*) FROM roster").fetchone()[0] == 0:
            conn.executemany("INSERT INTO roster(emp_id, name, grp) VALUES(?,?,?)", [
                ("E0001","山田 太郎","A"),
                ("E0002","佐藤 花子","A"),
                ("E0003","鈴木 次郎","B"),
                ("E0004","田中 三奈","B"),
                ("E0005","高橋 四郎","C"),
            ])

def is_locked(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        return conn.execute("SELECT 1 FROM locks WHERE ym=?", (ym,)).fetchone() is not None

def lock_month(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO locks(ym) VALUES(?)", (ym,))

def unlock_month(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        conn.execute("DELETE FROM locks WHERE ym=?", (ym,))

init_db(); ensure_onboarding()

def get_roster_df():
    with get_conn() as conn:
        return pd.read_sql_query("SELECT emp_id AS 社員ID, name AS 名前, grp AS グループ FROM roster ORDER BY emp_id", conn)

def get_shifts():
    with get_conn() as conn:
        return [r[0] for r in conn.execute("SELECT name FROM shifts ORDER BY name").fetchall()]

def add_record(d: date, shift: str, emp_id: str, points: float, memo: str=""):
    if is_locked(d):
        st.error("この月は締め済みのため入力できません。")
        return False
    roster = get_roster_df().set_index("社員ID")
    if emp_id not in roster.index:
        st.error("社員IDが名簿に存在しません。名簿ページで登録してください。")
        return False
    grp = roster.loc[emp_id, "グループ"]
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO records(d, shift, emp_id, grp, points, memo) VALUES(?,?,?,?,?,?)",
                (d.isoformat(), shift, emp_id, grp, float(points), memo or "")
            )
        return True
    except sqlite3.IntegrityError:
        st.warning("同じ日付×シフト×社員IDの記録がすでにあります。")
        return False

def query_range(start: _dt.date, end: _dt.date):
    with get_conn() as conn:
        df = pd.read_sql_query(
            "SELECT d, shift, emp_id, grp, points, memo FROM records WHERE d >= ? AND d < ?",
            conn, params=(start.isoformat(), end.isoformat())
        )
    return df

def month_df(base_month: date):
    start = date(base_month.year, base_month.month, 1)
    end = date(base_month.year + (base_month.month==12), (base_month.month % 12) + 1, 1)
    return query_range(start, end)

st.sidebar.title("シフトポイント")
page = st.sidebar.radio("メニュー", ["入力","ダッシュボード","名簿","管理"], index=0)

if page == "入力":
    st.header("クイック入力（社員ID）")
    roster = get_roster_df()
    shifts = get_shifts()
    if roster.empty:
        st.info("名簿が空です。まずは「名簿」ページで登録してください。")
    else:
        options = {f"{r['社員ID']} - {r['名前']}": r["社員ID"] for _, r in roster.iterrows()}

        st.caption("日付は既定で『今日』。必要があれば切り替え可。")
        c_top1, c_top2, _ = st.columns([1,1,1])
        with c_top1:
            use_today = st.toggle("今日の日付で記録する", value=True)
        with c_top2:
            yesterday_click = st.button("昨日で記録", use_container_width=True)

        with st.form("entry", clear_on_submit=True):
            c1, c2, c3 = st.columns([1,1,1.2])
            with c1:
                if yesterday_click:
                    d = date.today() - timedelta(days=1)
                elif use_today:
                    d = date.today()
                else:
                    d = st.date_input("日付 *", value=date.today())
                shift = st.selectbox("シフト *", options=shifts, index=0)
            with c2:
                label = st.selectbox("社員ID *（表示：ID - 名前）", options=list(options.keys()), index=0)
                emp_id = options[label]
                grp = roster.set_index("社員ID").loc[emp_id, "グループ"]
                st.text_input("グループ（自動）", value=grp, disabled=True)
            with c3:
                point = st.number_input("ポイント *", min_value=0.0, step=0.5, value=0.0, format="%.1f")
                memo = st.text_input("メモ（任意）", value="")

            ok = st.form_submit_button("追加する", type="primary", use_container_width=True)
            if ok:
                errs = []
                if d is None: errs.append("日付")
                if not shift: errs.append("シフト")
                if not emp_id: errs.append("社員ID")
                if point is None: errs.append("ポイント")
                if errs:
                    st.error("必須項目が未入力: " + ", ".join(errs))
                else:
                    if add_record(d, shift, emp_id, point, memo):
                        st.success(f"追加しました！（{d} / {shift} / {label}）")

    st.divider()
    st.subheader("最近の入力（当月）")
    dfm = month_df(date.today().replace(day=1))
    if dfm.empty:
        st.caption("この月のデータはありません。")
    else:
        dfm["日付"] = pd.to_datetime(dfm["d"]).dt.date
        dfm["ポイント"] = pd.to_numeric(dfm["points"], errors="coerce").fillna(0.0)
        st.dataframe(dfm.sort_values("日付", ascending=False).head(10), use_container_width=True)

elif page == "ダッシュボード":
    st.header("ダッシュボード")

    presets = ["今日", "今週", "今月", "先月", "カスタム"]
    preset = st.radio("期間", presets, horizontal=True, index=2)

    def _week_range(d: _dt.date):
        start = d - _dt.timedelta(days=d.weekday())
        end = start + _dt.timedelta(days=7)
        return start, end

    def _month_range(d: _dt.date):
        start = _dt.date(d.year, d.month, 1)
        if d.month == 12:
            end = _dt.date(d.year+1, 1, 1)
        else:
            end = _dt.date(d.year, d.month+1, 1)
        return start, end

    def _prev_month_range(d: _dt.date):
        if d.month == 1:
            start = _dt.date(d.year-1, 12, 1)
        else:
            start = _dt.date(d.year, d.month-1, 1)
        end = _dt.date(d.year, d.month, 1)
        return start, end

    today = _dt.date.today()
    if preset == "今日":
        start, end = today, today + _dt.timedelta(days=1)
    elif preset == "今週":
        start, end = _week_range(today)
    elif preset == "今月":
        start, end = _month_range(today)
    elif preset == "先月":
        start, end = _prev_month_range(today)
    else:
        c1, c2 = st.columns(2)
        with c1:
            start = st.date_input("開始日", value=today.replace(day=1))
        with c2:
            end = st.date_input("終了日（翌日でも可）", value=_month_range(today)[1])
        if start >= end:
            st.warning("終了日は開始日より後にしてください。")

    df = query_range(start, end)
    st.caption(f"表示範囲：{start} 〜 {end}（{(end - start).days}日間）")

    if df.empty:
        st.info("データがありません。まずは入力ページから追加してください。")
    else:
        df["日付"] = pd.to_datetime(df["d"]).dt.date
        df["ポイント"] = pd.to_numeric(df["points"], errors="coerce").fillna(0.0)
        roster = get_roster_df().rename(columns={"社員ID":"emp_id"}).set_index("emp_id")
        df["名前"] = df["emp_id"].map(roster["名前"])
        df["グループ"] = df["grp"]
        df["シフト"] = df["shift"]
        df["社員ID"] = df["emp_id"]
        df = df[["日付","シフト","社員ID","名前","グループ","ポイント","memo"]].rename(columns={"memo":"メモ"})

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("グループ別 合計")
            g = df.groupby("グループ", dropna=False, as_index=False)["ポイント"].sum().rename(columns={"ポイント":"合計ポイント"})
            g = g.sort_values("合計ポイント", ascending=False)
            st.dataframe(g, use_container_width=True, hide_index=True)
            if not g.empty:
                st.bar_chart(g.set_index("グループ"))
        with c2:
            st.subheader("個人合算 ランキング")
            p = df.groupby(["社員ID","名前"], as_index=False)["ポイント"].sum().rename(columns={"ポイント":"合計ポイント"})
            p = p.sort_values("合計ポイント", ascending=False)
            st.dataframe(p.head(10), use_container_width=True, hide_index=True)
            st.markdown("**TOP3**")
            cA, cB = st.columns(2)
            if len(p) > 0:
                st.metric("1位", f"{p.iloc[0]['名前']}（{p.iloc[0]['社員ID']}）", f"{p.iloc[0]['合計ポイント']:.1f}")
            with cA:
                if len(p) > 1:
                    st.metric("2位", f"{p.iloc[1]['名前']}（{p.iloc[1]['社員ID']}）", f"{p.iloc[1]['合計ポイント']:.1f}")
            with cB:
                if len(p) > 2:
                    st.metric("3位", f"{p.iloc[2]['名前']}（{p.iloc[2]['社員ID']}）", f"{p.iloc[2]['合計ポイント']:.1f}")

        st.divider()
        with st.expander("明細（期間内）"):
            st.dataframe(df.sort_values("日付", ascending=False), use_container_width=True, hide_index=True)

elif page == "名簿":
    st.header("名簿（社員ID・名前・グループ）")
    df = get_roster_df()
    st.caption("社員IDはユニーク。入力はID、表示は名前。")
    edited = st.data_editor(
        df if not df.empty else pd.DataFrame({"社員ID":[],"名前":[],"グループ":[]}),
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "社員ID": st.column_config.TextColumn("社員ID", required=True),
            "名前": st.column_config.TextColumn("名前", required=True),
            "グループ": st.column_config.TextColumn("グループ", required=True),
        }
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("保存する", type="primary", use_container_width=True):
            e = edited.copy().dropna(subset=["社員ID","名前","グループ"])
            for col in ["社員ID","名前","グループ"]:
                e[col] = e[col].astype(str).str.strip()
            if e["社員ID"].duplicated().any():
                st.error("社員IDが重複しています。ユニークにしてください。")
            else:
                with get_conn() as conn:
                    conn.execute("DELETE FROM roster")
                    conn.executemany("INSERT INTO roster(emp_id, name, grp) VALUES(?,?,?)",
                                     list(e[["社員ID","名前","グループ"]].itertuples(index=False, name=None)))
                st.success("名簿を保存しました。")
    with c2:
        st.download_button("名簿CSVをエクスポート", data=edited.to_csv(index=False).encode("utf-8-sig"),
                           file_name="roster.csv", mime="text/csv", use_container_width=True)

else:
    st.header("管理（PIN保護）")
    pin_ok = False
    with st.expander("管理PINを入力"):
        pin = st.text_input("管理PIN", type="password", placeholder="例: 1234")
        if st.button("認証", type="primary"):
            if pin == ADMIN_PIN:
                st.success("認証成功")
                pin_ok = True
            else:
                st.error("PINが違います")
    if not pin_ok:
        st.info("名簿・シフト編集、月締めロック、CSV入出力はPIN認証が必要です。")
        st.stop()

    st.subheader("月締めロック")
    c1, c2 = st.columns(2)
    with c1:
        d_lock = st.date_input("ロック/解除する月（1日）", value=date.today().replace(day=1))
        if st.button("この月をロックする", use_container_width=True):
            with get_conn() as conn: conn.execute("INSERT OR IGNORE INTO locks(ym) VALUES(?)", (f"{d_lock.year:04d}-{d_lock.month:02d}",))
            st.success("ロックしました")
        if st.button("この月のロックを解除する", use_container_width=True):
            with get_conn() as conn: conn.execute("DELETE FROM locks WHERE ym=?", (f"{d_lock.year:04d}-{d_lock.month:02d}",))
            st.success("ロックを解除しました")

    st.divider()
    st.subheader("シフト候補の編集")
    with get_conn() as conn:
        current = [r[0] for r in conn.execute("SELECT name FROM shifts ORDER BY name").fetchall()]
    editable = st.data_editor(pd.DataFrame({"シフト": current}), num_rows="dynamic", use_container_width=True)
    if st.button("シフトを保存", use_container_width=True):
        vals = [s for s in editable["シフト"].astype(str).str.strip().tolist() if s]
        vals = sorted(list(dict.fromkeys(vals)))
        with get_conn() as conn:
            conn.execute("DELETE FROM shifts")
            conn.executemany("INSERT INTO shifts(name) VALUES(?)", [(v,) for v in vals])
        st.success("保存しました。")

    st.divider()
    st.subheader("CSV入出力（バックアップ/移行）")
    cA, cB = st.columns(2)
    with cA:
        with get_conn() as conn:
            df_exp = pd.read_sql_query("SELECT d, shift, emp_id, grp, points, memo FROM records ORDER BY d DESC", conn)
        st.download_button("records.csv をエクスポート", data=df_exp.to_csv(index=False).encode("utf-8-sig"),
                           file_name="records.csv", mime="text/csv", use_container_width=True)
    with cB:
        up = st.file_uploader("records.csv をインポート（上書き）", type=["csv"])
        if up is not None:
            try:
                df_new = pd.read_csv(up)
        # 列名マッピング（日本語/英語どちらでもOK）
                col_map = {"日付":"d","シフト":"shift","社員ID":"emp_id","グループ":"grp","ポイント":"points","メモ":"memo"}
                df_new = df_new.rename(columns=col_map)
                need = ["d","shift","emp_id","grp","points","memo"]
                miss = [c for c in need if c not in df_new.columns]
                if miss:
                    st.error(f"必要な列がありません: {miss}")
                else:
                    df_new["points"] = pd.to_numeric(df_new["points"], errors="coerce").fillna(0.0)
                    with get_conn() as conn:
                        conn.execute("DELETE FROM records")
                        conn.executemany(
                        "INSERT INTO records(d, shift, emp_id, grp, points, memo) VALUES(?,?,?,?,?,?)",
                        list(df_new[need].itertuples(index=False, name=None))
                        )
                    st.success("records をインポートしました。")
            except Exception as e:
                st.error(f"インポートに失敗しました: {e}")
