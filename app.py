import os
import sqlite3
from pathlib import Path
from datetime import date, timedelta
import datetime as _dt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# =========================================================
# 初期ロードと基本設定
# =========================================================
load_dotenv()
ADMIN_PIN = os.getenv("ADMIN_PIN", "329865")  # PIN
STORE_PASS = os.getenv("STORE_PASS", "000113")

IS_RENDER = bool(os.getenv("RENDER"))  # Renderでは RENDER 環境変数が入ります
DATA_DIR  = Path("/data" if IS_RENDER else "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH     = DATA_DIR / "app.db"              # ← SQLite の保存先（永続化）
BACKUP_PATH = DATA_DIR / "latest_backup.csv"   # ← バックアップCSVの保存先（永続化）



st.set_page_config(
    page_title="B-POINT選手権 ☕",
    layout="wide"
)



# =========================================================
# DBまわり
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS roster (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                emp_id TEXT UNIQUE,
                name TEXT NOT NULL UNIQUE,
                grp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                d TEXT NOT NULL,          -- YYYY-MM-DD
                emp_id TEXT NOT NULL,
                grp TEXT NOT NULL,
                points REAL NOT NULL CHECK(points >= 0),
                -- 以下2つはもうUIから使わないが互換性のために残す
                shift TEXT,
                memo TEXT,
                UNIQUE(d, emp_id)
            );

            CREATE TABLE IF NOT EXISTS locks (
                ym TEXT PRIMARY KEY       -- 'YYYY-MM'
            );
            """
        )


init_db()


# =========================================================
# ロック確認
# =========================================================
def is_locked(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        row = conn.execute("SELECT 1 FROM locks WHERE ym=?", (ym,)).fetchone()
    return row is not None

def lock_month(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO locks(ym) VALUES(?)", (ym,))

def unlock_month(dt: date):
    ym = f"{dt.year:04d}-{dt.month:02d}"
    with get_conn() as conn:
        conn.execute("DELETE FROM locks WHERE ym=?", (ym,))


# =========================================================
# データ取得系
# =========================================================
def get_roster_df():
    with get_conn() as conn:
        df = pd.read_sql_query(
            "SELECT emp_id AS 社員ID, name AS 名前, grp AS グループ FROM roster ORDER BY emp_id",
            conn
        )
    return df

def get_records_df():
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT id, d, emp_id, grp, points, shift, memo
            FROM records
            ORDER BY d ASC, emp_id ASC
            """,
            conn
        )
    return df

def query_range(start: _dt.date, end: _dt.date):
    """start <= d < end のデータを返す"""
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT id, d, emp_id, grp, points
            FROM records
            WHERE d >= ? AND d < ?
            """,
            conn,
            params=(start.isoformat(), end.isoformat())
        )
    return df

def month_df(base_month: date):
    start = date(base_month.year, base_month.month, 1)
    if base_month.month == 12:
        end = date(base_month.year + 1, 1, 1)
    else:
        end = date(base_month.year, base_month.month + 1, 1)
    return query_range(start, end)


# =========================================================
# 自動バックアップ（最新のみ）
# =========================================================
def auto_backup_latest():
    """
    現在の全recordsをCSVとしてlatest_backup.csvに保存する。
    最新1本だけを常に上書き保持するイメージ。
    """
    try:
        df_all = get_records_df()
        df_all.to_csv(BACKUP_PATH, index=False, encoding="utf-8-sig")
    except Exception:
        # バックアップ失敗してもアプリは落とさない
        pass

# アプリに誰かがアクセスするたびに呼ぶ
auto_backup_latest()


# =========================================================
# 入力（1日1人1回）を追加
# =========================================================
def add_record(d: date, emp_id: str, points: float):
    # 月ロックチェック
    if is_locked(d):
        st.error("この月は締め済みのため入力できません。")
        return False

    # rosterからグループ確認
    roster = get_roster_df().set_index("社員ID")
    if emp_id not in roster.index:
        st.error("その社員IDは存在しません")
        return False
    grp = roster.loc[emp_id, "グループ"]

    with get_conn() as conn:
        # 同日・同社員のデータがあるか確認
        exists = conn.execute(
            "SELECT id FROM records WHERE d=? AND emp_id=?",
            (d.isoformat(), emp_id)
        ).fetchone()

        if exists:
            # 既存データを上書き（ポイントとグループを更新）
            conn.execute(
                """
                UPDATE records
                SET grp=?, points=?
                WHERE d=? AND emp_id=?
                """,
                (grp, float(points), d.isoformat(), emp_id)
            )
            st.info("本日のポイントを上書きしました。")
        else:
            # 新規登録
            conn.execute(
                """
                INSERT INTO records(d, emp_id, grp, points, shift, memo)
                VALUES(?,?,?,?,?,?)
                """,
                (d.isoformat(), emp_id, grp, float(points), "", "")
            )

    return True



# =========================================================
# 社員の累計ポイント（全期間）
# =========================================================
def get_total_points_by_emp():
    rec = get_records_df()
    if rec.empty:
        return pd.DataFrame(columns=["社員ID", "累計ポイント"])
    agg = (
        rec.groupby("emp_id", as_index=False)["points"]
        .sum()
        .rename(columns={"emp_id": "社員ID", "points": "累計ポイント"})
    )
    return agg


# =========================================================
# 共通：期間プリセット
# =========================================================
def get_period(preset: str):
    today = _dt.date.today()

    def week_range(d: _dt.date):
        # 週は月曜はじまり (weekday()=0が月曜)
        start = d - _dt.timedelta(days=d.weekday())
        end = start + _dt.timedelta(days=7)
        return start, end

    def month_range(d: _dt.date):
        start = _dt.date(d.year, d.month, 1)
        if d.month == 12:
            end = _dt.date(d.year + 1, 1, 1)
        else:
            end = _dt.date(d.year, d.month + 1, 1)
        return start, end

    def prev_month_range(d: _dt.date):
        if d.month == 1:
            start = _dt.date(d.year - 1, 12, 1)
        else:
            start = _dt.date(d.year, d.month - 1, 1)
        end = _dt.date(d.year, d.month, 1)
        return start, end

    if preset == "今日":
        return today, today + _dt.timedelta(days=1)
    elif preset == "今週":
        return week_range(today)
    elif preset == "今月":
        return month_range(today)
    elif preset == "先月":
        return prev_month_range(today)
    else:
        # preset == "カスタム" は後でUI側で手動指定
        return None, None


# =========================================================
# UI: サイドバー
# =========================================================
st.sidebar.title("B-POINT選手権 ☕")
page = st.sidebar.radio(
    "メニュー",
    ["入力", "順位", "名簿", "設定"],
    index=0
)
# 初期化（なければ）
for k in ("rank_ok", "roster_ok", "admin_ok"):
    st.session_state.setdefault(k, False)
# --- ページ切り替えで認証をリセット ---
if "last_page" not in st.session_state:
    st.session_state["last_page"] = page

if st.session_state["last_page"] != page:
    # ページが変わったら閲覧系の認証をリセット
    st.session_state["rank_ok"] = False
    st.session_state["roster_ok"] = False
    # （設定ページを毎回要求したいなら↓もFalseに）
    st.session_state["admin_ok"] = False

    st.session_state["last_page"] = page


# =========================================================
# ページ1: 入力（スタッフ用）
# =========================================================

if page == "入力":
    # メインタイトル（中央寄せ）
    st.markdown(
        "<h1 style='text-align: center; font-size: 42px;'>B-POINT選手権 ☕</h1>",
        unsafe_allow_html=True
    )

    st.header("入力")

    roster = get_roster_df()
    if roster.empty:
        st.info("まだ名簿がありません（設定→名簿編集で登録してください）")
    else:
        # 表示ラベル "ID - 名前" -> 実値emp_id
        choices_map = {
            f"{row['社員ID']} - {row['名前']}": row["社員ID"]
            for _, row in roster.iterrows()
        }

        # 日付の扱い
        st.caption("日付は基本『今日』になります。必要なら日付を指定してください。")
        col_date_flags1, _ = st.columns([1,1])  # 列を2→1に変更
        with col_date_flags1:
            use_today = st.toggle("今日の日付で記録する", value=True)

        with st.form("entry_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([1,1,1])
            with c1:
        # 日付ロジック
                if use_today:
                    d_val = date.today()
                else:
                    d_val = st.date_input("日付 *", value=date.today())

        # ✅ 社員ID手入力 + 名簿チェック
                emp_id = st.text_input("社員ID").strip()

                if emp_id:
                    if emp_id not in roster["社員ID"].astype(str).tolist():
                        st.error("この社員IDは存在しません。")
                        grp_val = ""
                    else:
                        grp_val = roster.set_index("社員ID").loc[emp_id, "グループ"]
                else:
                    grp_val = ""

                st.text_input("チーム（自動）", value=grp_val, disabled=True)

            with c2:
                point_val = st.number_input(
                    "ポイント *",
                    min_value=0.0,
                    step=0.5,
                    value=0.0,
                    format="%.1f"
                )

            with c3:
                st.write("")
                submit_btn = st.form_submit_button("追加する", type="primary")

            if submit_btn:
                missing = []
                if not d_val:
                    missing.append("日付")
                if not emp_id:
                    missing.append("社員ID")
                elif emp_id not in roster["社員ID"].astype(str).tolist():
                    missing.append("社員ID（未登録）")
                if point_val is None:
                    missing.append("ポイント")

                if missing:
                    st.error("必須項目が未入力または不正: " + ", ".join(missing))
                else:
                    ok = add_record(d_val, emp_id, point_val)
                    if ok:
                        st.success(f"追加しました！（{d_val} / {emp_id} / {point_val:.1f}pt）")

    # 入力画面では最近ログは表示しない（店員が見れないようにする）


# =========================================================
# ページ2: 順位（旧ダッシュボード / 閲覧のみ）
# =========================================================
elif page == "順位":
    st.header("順位")
    
        # --- 店舗共有パス認証 ---

    # --- パスワード入力UI ---
    # 認証フラグが立ってなければフォームを表示
    if not st.session_state.get("rank_ok", False):
        with st.form("rank_auth_form", clear_on_submit=False):
            st.write("この画面を見るには店舗用パスワードが必要です。")
            pw_try = st.text_input("店舗共有パスワード", type="password")
            ok = st.form_submit_button("表示する", type="primary")

        if not ok:
            st.stop()

        if pw_try != STORE_PASS:
            st.error("パスワードが違います。")
            st.stop()

        # 認証成功 → フラグを立ててページ再描画（以後は保持）
        st.session_state["rank_ok"] = True
        st.rerun()


    presets = ["今日", "今週", "今月", "先月", "カスタム"]
    preset = st.radio("期間", presets, horizontal=True, index=2)  # デフォルト今月

    today = _dt.date.today()
    if preset == "カスタム":
        c_custom1, c_custom2 = st.columns(2)
        with c_custom1:
            start_date = st.date_input("開始日", value=today.replace(day=1))
        with c_custom2:
            # デフォルトは今月末の次の日
            end_default = (
                _dt.date(today.year + (today.month == 12),
                        (today.month % 12) + 1,
                        1)
            )
            end_date = st.date_input("終了日（翌日でも可）", value=end_default)

        if start_date >= end_date:
            st.warning("終了日は開始日より後にしてください。")
        period_start, period_end = start_date, end_date
    else:
        period_start, period_end = get_period(preset)

    st.caption(
        f"表示範囲：{period_start} 〜 {period_end} "
        f"（{(period_end - period_start).days}日間）"
        if period_start and period_end else ""
    )

    df_range = query_range(period_start, period_end)
    if df_range.empty:
        st.info("この期間のデータはありません。")
    else:
        # rosterと結合して「名前」を出す
        roster = get_roster_df().rename(columns={"社員ID":"emp_id"}).set_index("emp_id")
        df_range = df_range.copy()
        df_range["名前"] = df_range["emp_id"].map(roster["名前"])
        df_range["チーム"] = df_range["grp"]
        df_range["ポイント"] = pd.to_numeric(df_range["points"], errors="coerce").fillna(0.0)

        # -------------------
        # A. チーム順位（1位〜最後まで）
        # -------------------
        st.subheader("チーム順位（合計ポイント）")

        team_totals = (
            df_range
            .groupby("チーム", as_index=False)["ポイント"]
            .sum()
            .rename(columns={"ポイント": "合計ポイント"})
            .sort_values("合計ポイント", ascending=False)
            .reset_index(drop=True)
        )

        # 順位番号をつける
        team_totals.insert(0, "順位", range(1, len(team_totals) + 1))

        st.dataframe(
            team_totals,
            hide_index=True
        )


        st.divider()

        # -------------------
        # B. チームごとの個人TOP3
        # -------------------
        st.subheader("各チームの個人TOP3")

        # 個人ごとの合計
        per_person = (
            df_range
            .groupby(["チーム", "emp_id", "名前"], as_index=False)["ポイント"]
            .sum()
            .rename(columns={"ポイント": "個人合計"})
        )

        # チームごとにTOP3を出す
        for team_name in per_person["チーム"].unique():
            sub = (
                per_person[per_person["チーム"] == team_name]
                .sort_values("個人合計", ascending=False)
                .head(3)
                .reset_index(drop=True)
            )
            st.markdown(f"**{team_name}**")
            # 1位,2位,3位の形式で見せる
            for i, row in sub.iterrows():
                rank_num = i + 1
                st.write(
                    f"{rank_num}位: {row['名前']} - {row['個人合計']:.1f} pt"
                )
            st.write("---")


# =========================================================
# ページ3: 名簿（閲覧専用）
# =========================================================
elif page == "名簿":
    st.header("名簿（閲覧専用）")

    # --- 店舗共有パス認証 ---
    if not st.session_state.get("roster_ok", False):
        with st.form("roster_auth_form", clear_on_submit=False):
            st.write("この画面を見るには店舗用パスワードが必要です。")
            pw_try = st.text_input("店舗共有パスワード", type="password")
            ok = st.form_submit_button("表示する", type="primary")

        if not ok:
            st.stop()

        if pw_try != STORE_PASS:
            st.error("パスワードが違います。")
            st.stop()

        st.session_state["roster_ok"] = True
        st.rerun()

    # --- 期間フィルタ UI ---
    st.divider()
    st.subheader("📅 期間指定（任意）")
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        use_period = st.toggle("期間で絞り込み", value=False)
    with c2:
        start_date = st.date_input("開始日", value=date.today().replace(day=1))
    with c3:
        end_date = st.date_input("終了日", value=date.today())

    if use_period and start_date > end_date:
        st.warning("開始日が終了日より後になっています。日付を見直してください。")
        st.stop()

    roster = get_roster_df()  # 社員ID, 名前, グループ

    # --- 期間で合計を作る or 全期間の累計を使う ---
    if use_period:
        rec_df = get_records_df()  # d, 社員ID, グループ, ポイント, ...
        if rec_df.empty:
            totals = pd.DataFrame({"社員ID": [], "累計ポイント": []})
        else:
            rec_df["d"] = pd.to_datetime(rec_df["d"])
            mask = (rec_df["d"] >= pd.Timestamp(start_date)) & (rec_df["d"] <= pd.Timestamp(end_date))
            rec_period = rec_df.loc[mask]

            if rec_period.empty:
                totals = pd.DataFrame({"社員ID": [], "累計ポイント": []})
            else:
                totals = (rec_period
                          .groupby("社員ID", as_index=False)["ポイント"]
                          .sum()
                          .rename(columns={"ポイント": "累計ポイント"}))
    else:
        # 既存の全期間累計を使う（あなたのヘルパー関数）
        totals = get_total_points_by_emp()  # 社員ID, 累計ポイント

    if roster.empty:
        st.info("まだ名簿がありません（設定→名簿編集で登録してください）")
    else:
        # 累計ポイントを結合（見せるのは 名前/グループ/累計ポイント のみ）
        merged = pd.merge(roster, totals, on="社員ID", how="left")
        merged["累計ポイント"] = pd.to_numeric(merged["累計ポイント"], errors="coerce").fillna(0.0)

        # 見やすい並び順: グループ → ポイント降順
        merged = merged.sort_values(by=["グループ", "累計ポイント"], ascending=[True, False])

        st.dataframe(
            merged[["名前", "グループ", "累計ポイント"]],
            hide_index=True
        )

    st.caption("編集や新規登録は「設定」からのみできます。")


# =========================================================
# ページ4: 設定（PINロック ）
# =========================================================
elif page == "設定":
    st.header("設定")

    # ---- リフレッシュフラグ処理（最優先で実行）----
    # 直前の操作（新規追加・全消しなど）が成功してたら画面をクリーンに描き直す
    if st.session_state.get("refresh_settings", False):
        st.session_state["refresh_settings"] = False
        st.rerun()

    # ---- PIN認証をセッションで保持 ----
    if not st.session_state.get("admin_ok", False):
        with st.form("admin_auth_form", clear_on_submit=False):
            pin_try = st.text_input("管理PIN", type="password", placeholder="****")
            ok = st.form_submit_button("表示する", type="primary")

        if not ok:
            st.stop()
        if pin_try != ADMIN_PIN:
            st.error("PINが違います。"); st.stop()

        st.session_state["admin_ok"] = True
        st.rerun()   # ← これで再描画して以後は認証済みのまま

    st.success("管理者認証OK ✅")

    # ← ここからは「PIN通った人だけ」が実行されるゾーン

    # 直前に新規メンバー追加や全データリセットが成功していたら、ここでリロード
    if st.session_state.get("reload_after_add", False):
        st.session_state["reload_after_add"] = False
        st.rerun()


        # -------- 名簿編集 / 登録 --------
    st.subheader("名簿の管理")

    st.markdown("#### 新規メンバー追加")

    with st.form("add_member_form", clear_on_submit=True):
        new_emp_id = st.text_input("社員ID *", key="new_emp_id")
        new_name = st.text_input("名前 *", key="new_emp_name")
        new_grp = st.text_input("グループ *", key="new_emp_grp")

        submitted_new = st.form_submit_button("この人を追加", type="primary")
        if submitted_new:
            emp_id_clean = new_emp_id.strip()
            name_clean = new_name.strip()
            grp_clean = new_grp.strip()

            if not emp_id_clean or not name_clean or not grp_clean:
                st.error("社員ID・名前・グループはすべて必須です。")
            else:
                try:
                    with get_conn() as conn:
                        conn.execute(
                            "INSERT INTO roster(emp_id, name, grp) VALUES(?,?,?)",
                            (emp_id_clean, name_clean, grp_clean)
                        )

                    st.success(f"{emp_id_clean} を追加しました。")

                    # ← ここが大事
                    # 追加したら設定ページを最新状態で描き直したいのでフラグを立てる
                    st.session_state["refresh_settings"] = True

                except Exception as e:
                    st.error(f"追加できませんでした: {e}")

    st.divider()
    
    # === 名簿 編集・削除 ===
    st.markdown("#### 名簿の編集 / 削除")

# 現在の名簿を取得（社員ID・名前・グループ）
    roster_df = get_roster_df().rename(columns={"社員ID":"emp_id","名前":"name","グループ":"grp"})

    if roster_df.empty:
        st.caption("まだ名簿がありません。先に新規追加してください。")
    else:
        # 編集用ワークコピーをセッションに保持（編集中に消えないように）
        if "roster_work" not in st.session_state:
            st.session_state["roster_work"] = roster_df.copy()

        st.session_state["roster_work"] = st.data_editor(
            st.session_state["roster_work"],
            num_rows="fixed",
            key="roster_editor",
            # 社員IDは基本変えない想定なら True で保護（変えるなら False）
            column_config={
                "emp_id": st.column_config.Column("社員ID", disabled=True),
                "name":   st.column_config.Column("名前"),
                "grp":    st.column_config.Column("グループ"),
            }
        )

        col_upd, col_del = st.columns(2)
        with col_upd:
            if st.button("名簿の編集内容を保存", type="primary", key="save_roster_edit"):
                try:
                    with get_conn() as conn:
                        for _, row in st.session_state["roster_work"].iterrows():
                            conn.execute(
                                "UPDATE roster SET name=?, grp=? WHERE emp_id=?",
                                (row["name"].strip(), row["grp"].strip(), row["emp_id"].strip())
                            )
                    st.success("名簿を更新しました。")
                    st.session_state["refresh_settings"] = True
                except Exception as e:
                    st.error(f"名簿更新でエラー: {e}")

        with col_del:
            del_targets = st.multiselect(
                "削除する社員IDを選択",
                options=st.session_state["roster_work"]["emp_id"].tolist()
            )
            if st.button("選択した人を削除", key="del_roster_btn"):
                if del_targets:
                    try:
                        with get_conn() as conn:
                            for emp in del_targets:
                                conn.execute("DELETE FROM roster WHERE emp_id=?", (emp,))
                        st.success("削除しました。")
                        st.session_state["refresh_settings"] = True
                    except Exception as e:
                        st.error(f"削除でエラー: {e}")
                else:
                    st.info("削除対象が選ばれていません。")
    st.divider()

    # -------- 入力履歴（当月）と修正 / 削除 --------
    st.subheader("入力履歴（当月）と修正 / 削除")

    this_month_start = date.today().replace(day=1)
    rec_m = month_df(this_month_start)
    if rec_m.empty:
        st.caption("今月の入力はまだありません。")
    else:
        roster_map = get_roster_df().set_index("社員ID")
        rec_view = rec_m.copy()
        rec_view["名前"] = rec_view["emp_id"].map(roster_map["名前"])
        rec_view["チーム"] = rec_view["grp"]
        rec_view["ポイント"] = pd.to_numeric(rec_view["points"], errors="coerce").fillna(0.0)
        rec_view["日付"] = pd.to_datetime(rec_view["d"]).dt.date

        # 編集用のDataFrameに整形
        edit_df = rec_view[["id", "日付", "emp_id", "名前", "チーム", "ポイント"]].rename(
            columns={
                "emp_id": "社員ID",
                "チーム": "グループ",
                "ポイント": "ポイント"
            }
        )

        if "records_work" not in st.session_state:
            st.session_state["records_work"] = edit_df.copy()

        st.session_state["records_work"] = st.data_editor(
            st.session_state["records_work"],
            num_rows="fixed",
            key="records_edit"
        )

        # 削除対象のチェック
        delete_ids = st.multiselect(
            "削除したい行（id）を選択",
            options=st.session_state["records_work"]["id"].tolist()
        )

        c_upd, c_del = st.columns(2)
        with c_upd:
            if st.button("修正を保存", type="primary"):
                try:
                    with get_conn() as conn:
                        for _, row in st.session_state["records_work"].iterrows():
                            rid = row["id"]
                            new_date = row["日付"]
                            new_emp = row["社員ID"]
                            new_grp = row["グループ"]
                            new_pts = float(row["ポイント"])

                            # ロックされてる月はスキップ
                            if is_locked(new_date):
                                continue

                            conn.execute(
                                """
                                UPDATE records
                                SET d=?, emp_id=?, grp=?, points=?
                                WHERE id=?
                                """,
                                (new_date.isoformat(), new_emp, new_grp, new_pts, rid)
                            )
                    st.success("修正を保存しました。")
                except Exception as e:
                    st.error(f"修正の保存でエラー: {e}")

        with c_del:
            if st.button("選択した行を削除"):
                if delete_ids:
                    try:
                        with get_conn() as conn:
                            rows_for_delete = st.session_state["records_work"][
                                st.session_state["records_work"]["id"].isin(delete_ids)
                            ]
                            for _, row in rows_for_delete.iterrows():
                                del_date = row["日付"]
                                if not is_locked(del_date):
                                    conn.execute("DELETE FROM records WHERE id=?", (row["id"],))
                        st.success("削除しました。")
                    except Exception as e:
                        st.error(f"削除でエラー: {e}")
                else:
                    st.info("削除対象が選ばれていません。")

    st.divider()

    # -------- 月締めロック --------
    st.subheader("月締めロック")
    col_lock1, col_lock2 = st.columns(2)
    with col_lock1:
        lock_target = st.date_input("ロ締め・解除する月の1日を選択", value=date.today().replace(day=1))
        if st.button("この月をロックする"):
            lock_month(lock_target)
            st.success("ロックしました。この月の新規入力や修正・削除はできません。")
        if st.button("この月のロックを解除する"):
            unlock_month(lock_target)
            st.success("ロックを解除しました。修正・削除が可能になります。")

    st.divider()

    # -------- CSVバックアップ / 復元 --------
    st.subheader("バックアップ / 復元")

    with get_conn() as conn:
        df_export = pd.read_sql_query(
            "SELECT d, emp_id, grp, points, shift, memo FROM records ORDER BY d ASC",
            conn
        )

    st.download_button(
        "現在の全データをCSVダウンロード",
        data=df_export.to_csv(index=False).encode("utf-8-sig"),
        file_name="records_export.csv",
        mime="text/csv"
    )

    # latest_backup.csv のダウンロード
    if BACKUP_PATH.exists():
        st.download_button(
            "latest_backup.csv をダウンロード（自動バックアップ）",
            data=BACKUP_PATH.read_bytes(),
            file_name="latest_backup.csv"
        )
    else:
        st.caption("latest_backup.csv はまだありません（誰かが画面を開くと自動で作られます）。")

    st.subheader("CSVインポート（上書き復元）")
    st.caption("latest_backup.csv または過去エクスポートしたCSVを使って復元できます。")

    uploaded = st.file_uploader("CSVを選択して復元", type=["csv"])
    if uploaded is not None:
        if st.button("このCSVで上書き復元する", type="primary"):
            try:
                df_new = pd.read_csv(uploaded)

                col_map = {
                    "日付": "d",
                    "社員ID": "emp_id",
                    "チーム": "grp",
                    "グループ": "grp",
                    "ポイント": "points",
                    "メモ": "memo",
                    "シフト": "shift"
                }
                df_new = df_new.rename(columns=col_map)

                need_cols = ["d", "emp_id", "grp", "points"]
                for c in need_cols:
                    if c not in df_new.columns:
                        st.error(f"必要な列が足りません: {c}")
                        st.stop()

                if "shift" not in df_new.columns:
                    df_new["shift"] = ""
                if "memo" not in df_new.columns:
                    df_new["memo"] = ""

                df_new["points"] = pd.to_numeric(df_new["points"], errors="coerce").fillna(0.0)

                with get_conn() as conn:
                    conn.execute("DELETE FROM records")
                    conn.executemany(
                        """
                        INSERT INTO records(d, emp_id, grp, points, shift, memo)
                        VALUES(?,?,?,?,?,?)
                        """,
                        list(df_new[["d","emp_id","grp","points","shift","memo"]].itertuples(index=False, name=None))
                    )
                st.success("復元が完了しました。")
                auto_backup_latest()
            except Exception as e:
                st.error(f"復元でエラーが発生しました: {e}")
                
        st.divider()
    st.markdown("### ⚠️ データリセット")
    st.caption("※ 名簿（社員ID・名前・チーム）はそのまま残ります。ポイント履歴だけ全削除します。")

    if "confirm_wipe" not in st.session_state:
        st.session_state["confirm_wipe"] = False

    if st.button("全データをリセットする", type="secondary"):
        st.session_state["confirm_wipe"] = True

    if st.session_state["confirm_wipe"]:
        st.warning(
            "本当にすべての入力データ（ポイント履歴）を削除しますか？この操作は取り消せません。",
            icon="⚠️"
        )

        col1, col2 = st.columns([1,1])

        with col1:
            if st.button("はい、削除します", type="primary", key="do_wipe"):
                try:
                    with get_conn() as conn:
                        conn.execute("DELETE FROM records")
                    auto_backup_latest()

                    # 状態リセット
                    st.session_state["confirm_wipe"] = False

                    # ✅ 新仕様：ページを更新して最新状態へ
                    st.session_state["refresh_settings"] = True

                    st.success("✅ 入力データをすべて削除しました。")

                except Exception as e:
                    st.error(f"削除中にエラーが発生しました: {e}")

        with col2:
            if st.button("キャンセル", type="secondary", key="cancel_wipe"):
                st.session_state["confirm_wipe"] = False
                st.success("キャンセルしました。")


