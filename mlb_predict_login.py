import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone, date
import sqlite3
import os
import requests



st.write("這是 MLB_PREDICT Login 版 測試畫面 v4.4（點數紀錄+每日獎勵+補點）")

DB_PATH = "mlb_predictions.db"

# ========= 在這裡填入你的 The Odds API 金鑰 =========
THE_ODDS_API_KEY = "208a1ed1cbf73d8a1169675d84372d41"
THE_ODDS_BASE_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
# ===================================================

# 每日首筆預測獎勵點數
DAILY_BONUS_POINTS = 10

# ===================== MLB 30 隊中文名 =====================

TEAM_NAME_ZH = {
    "Arizona Diamondbacks": "亞利桑那 響尾蛇",
    "Atlanta Braves": "亞特蘭大 勇士",
    "Baltimore Orioles": "巴爾的摩 金鶯",
    "Boston Red Sox": "波士頓 紅襪",
    "Chicago Cubs": "芝加哥 小熊",
    "Chicago White Sox": "芝加哥 白襪",
    "Cincinnati Reds": "辛辛那提 紅人",
    "Cleveland Guardians": "克里夫蘭 守護者",
    "Colorado Rockies": "科羅拉多 落磯",
    "Detroit Tigers": "底特律 老虎",
    "Houston Astros": "休士頓 太空人",
    "Kansas City Royals": "堪薩斯市 皇家",
    "Los Angeles Angels": "洛杉磯 天使",
    "Los Angeles Dodgers": "洛杉磯 道奇",
    "Miami Marlins": "邁阿密 馬林魚",
    "Milwaukee Brewers": "密爾瓦基 釀酒人",
    "Minnesota Twins": "明尼蘇達 雙城",
    "New York Mets": "紐約 大都會",
    "New York Yankees": "紐約 洋基",
    "Oakland Athletics": "奧克蘭 運動家",
    "Philadelphia Phillies": "費城 費城人",
    "Pittsburgh Pirates": "匹茲堡 海盜",
    "San Diego Padres": "聖地牙哥 教士",
    "San Francisco Giants": "舊金山 巨人",
    "Seattle Mariners": "西雅圖 水手",
    "St. Louis Cardinals": "聖路易 紅雀",
    "Tampa Bay Rays": "坦帕灣 光芒",
    "Texas Rangers": "德州 遊騎兵",
    "Toronto Blue Jays": "多倫多 藍鳥",
    "Washington Nationals": "華盛頓 國民",
}

# ===================== MLB 明日賽程（statsapi） =====================

def fetch_schedule_by_date_tw(target_date: date):
    """
    以「台灣時間的指定日期」為基準，向 statsapi 要該日所有 MLB 比賽。
    target_date: datetime.date 物件（台灣日曆）。
    """
    tz_tw = timezone(timedelta(hours=8))
    target_date_str = target_date.strftime("%Y-%m-%d")

    # 直接用 target_date 當 startDate / endDate
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&startDate={target_date_str}&endDate={target_date_str}"
        "&language=en&hydrate=team&timeZone=America/New_York"
    )
    resp = requests.get(url, timeout=10)
    data = resp.json()

    games_data = []
    for day in data.get("dates", []):
        for game in day.get("games", []):
            game_pk = game["gamePk"]
            away_team = game["teams"]["away"]["team"]["name"]
            home_team = game["teams"]["home"]["team"]["name"]

            # 官方給的 gameDate 是 UTC，轉成台灣時間只做顯示
            game_dt_utc = datetime.fromisoformat(game["gameDate"].replace("Z", "+00:00"))
            game_dt_tw = game_dt_utc.astimezone(tz_tw)

            venue = game.get("venue", {}).get("name", "")

            games_data.append(
                {
                    "game_id": str(game_pk),
                    "away_name": away_team,
                    "home_name": home_team,
                    "game_date": target_date_str,
                    # 注意：這裡先保留「原始 UTC 字串」，方便之後比時間＆寫 DB
                    "game_datetime_utc": game["gameDate"],
                    "game_datetime_tw": game_dt_tw.strftime("%Y-%m-%d %H:%M"),
                    "venue": venue,
                    "ml_away": 0,
                    "ml_home": 0,
                    "runline": "N/A",
                }
            )
    return games_data


def get_games(target_date: date):
    """
    目前先只抓 statsapi，不在這裡寫 DB，避免 database is locked。
    之後我們再做一個「管理員同步賽程到 DB」的工具，分開處理。
    """
    try:
        games = fetch_schedule_by_date_tw(target_date)
        # 為了跟你前面使用欄位相容，補一個 game_datetime 欄位給前端顯示
        for g in games:
            g["game_datetime"] = g["game_datetime_utc"]  # 或用 g["game_datetime_tw"]
        return games
    except Exception as e:
        st.warning(f"抓取 MLB 賽程失敗：{e}")
        return []

    # 同步寫入 DB 的 games 表           ← 從這裡以下全部刪掉
    with get_db() as conn:
        cur = conn.cursor()
        for g in games:
            cur.execute(
                """
                INSERT OR REPLACE INTO games
                    (game_id, away_team, home_team, game_date, game_datetime, venue)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    g["game_id"],
                    g["away_name"],
                    g["home_name"],
                    g["game_date"],
                    g["game_datetime"],
                    g["venue"],
                ),
            )
        conn.commit()

    return games

# ========================================================

def fetch_game_final_score_from_statsapi(game_id: str):
    """
    用 statsapi 抓單場比賽最終比分。

    回傳:
        (away_score, home_score, status_str)
        若抓取失敗或比賽尚未結束，回傳 (None, None, status_str)
    """
    try:
        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        st.warning(f"statsapi 抓取比賽 {game_id} 失敗：{e}")
        return None, None, "ERROR"

    try:
        status_str = data.get("gameData", {}).get("status", {}).get("detailedState", "")
        linescore = data.get("liveData", {}).get("linescore", {})
        teams = linescore.get("teams", {})
        away = teams.get("away", {})
        home = teams.get("home", {})
        away_score = away.get("runs")
        home_score = home.get("runs")
    except Exception:
        return None, None, status_str or "UNKNOWN"

    return away_score, home_score, status_str

def set_game_result(game_id: str, winner_pick: str, spread_winner: str = "push"):
    """
    結算某一場比賽的所有預測：
    - 更新 predictions.is_correct / spread_result
    - 幫命中的玩家發點數
    - 寫入 points_logs
    winner_pick: "home" 或 "away"
    spread_winner: 目前先預設 "push"，之後要做讓分再擴充
    """
    # 1. 撈出這場比賽所有預測
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT id, player, pick, spread_pick, confidence, is_main
            FROM predictions
            WHERE game_id = ?
            """,
            (game_id,),
        )
        rows = c.fetchall()

    if not rows:
        return

    # 2. 逐筆判斷勝負，整理要更新的結果與點數變化
    updates = []         # (is_correct, spread_result, id)
    points_changes = []  # (username, delta, reason)

    for pred_id, player, pick, spread_pick, confidence, is_main in rows:
        # 勝負命中與否
        is_correct = 1 if pick == winner_pick else 0

        # 讓分目前先全部視為 push（之後你要算盤口再補）
        spread_result = "push"

        updates.append((is_correct, spread_result, pred_id))

        # 命中才發點數：
        # 你原本先扣 20 點，所以這裡可以一次補回 40（退 20 + 獎勵 20）
        if is_correct == 1:
            base_reward = 40
            reward = base_reward  # 之後可以依 is_main 再加成

            reason = f"比賽 {game_id} 命中勝負盤，獎勵 {reward} 點"
            points_changes.append((player, reward, reason))

    # 3. 寫回 predictions 狀態
    with get_db() as conn:
        c = conn.cursor()
        c.executemany(
            """
            UPDATE predictions
            SET is_correct = ?, spread_result = ?
            WHERE id = ?
            """,
            updates,
        )
        conn.commit()

    # 4. 依 points_changes 幫玩家加點 + 寫 points_logs
    for username, delta, reason in points_changes:
        update_user_points(username, delta)
        log_points_change(username, delta, reason)

# ===================== The Odds API =====================

@st.cache_data(ttl=300)
def fetch_mlb_odds():
    if not THE_ODDS_API_KEY or THE_ODDS_API_KEY == "YOUR_THE_ODDS_API_KEY_HERE":
        return {}

    params = {
        "apiKey": THE_ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(THE_ODDS_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        st.warning(f"The Odds API 抓取失敗：{e}")
        return {}

    odds_map = {}
    for game in data:
        away_team = game.get("away_team")
        home_team = game.get("home_team")
        if not away_team or not home_team:
            continue

        bookmakers = game.get("bookmakers", [])
        if not bookmakers:
            continue
        bk = bookmakers[0]
        markets = bk.get("markets", [])

        moneyline = {"away": None, "home": None}
        spread = {"point": None, "away": None, "home": None}
        totals = {"point": None, "over": None, "under": None}

        for m in markets:
            key = m.get("key")
            outcomes = m.get("outcomes", [])
            if key == "h2h":
                for o in outcomes:
                    if o.get("name") == away_team:
                        moneyline["away"] = o.get("price")
                    elif o.get("name") == home_team:
                        moneyline["home"] = o.get("price")
            elif key == "spreads":
                for o in outcomes:
                    if o.get("name") == away_team:
                        spread["away"] = o.get("price")
                        spread["point"] = o.get("point")
                    elif o.get("name") == home_team:
                        spread["home"] = o.get("price")
                        spread["point"] = o.get("point")
            elif key == "totals":
                for o in outcomes:
                    if o.get("name") == "Over":
                        totals["over"] = o.get("price")
                        totals["point"] = o.get("point")
                    elif o.get("name") == "Under":
                        totals["under"] = o.get("price")
                        totals["point"] = o.get("point")

        odds_map[(away_team, home_team)] = {
            "moneyline": moneyline,
            "spread": spread,
            "totals": totals,
        }
    return odds_map
    
def resync_games_table():
    """清空 games 表，並用『今天台灣日期』重新抓一次賽程寫回去。"""
    # 1. 先清空 games 表
    with get_db() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM games")
        conn.commit()

    # 2. 算出今天的台灣日期
    tz_tw = timezone(timedelta(hours=8))
    today_tw = datetime.now(tz_tw).date()

    # 3. 用今天日期呼叫 get_games（注意：get_games 需要 target_date 參數）
    games = get_games(today_tw)

    # 4. 把剛抓到的賽程寫回 games 表
    with get_db() as conn:
        c = conn.cursor()
        for g in games:
            c.execute(
                """
                INSERT OR REPLACE INTO games
                    (game_id, away_team, home_team, game_date, game_datetime, venue)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    g["game_id"],
                    g["away_name"],
                    g["home_name"],
                    g["game_date"],
                    g.get("game_datetime") or g.get("game_datetime_utc") or "",
                    g["venue"],
                ),
            )
    conn.commit()
    conn.close()


# ===================== DB & helpers =====================

def init_db():
    need_init = not os.path.exists(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if need_init:
        c.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT,
                is_admin INTEGER DEFAULT 0,
                is_blocked INTEGER DEFAULT 0,
                points INTEGER DEFAULT 100,
                last_bonus_date TEXT,
                is_active INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                away_team TEXT,
                home_team TEXT,
                game_date TEXT,
                game_datetime TEXT,
                venue TEXT,
                ml_away REAL,
                ml_home REAL,
                runline TEXT,
                status TEXT DEFAULT 'Scheduled'
            )
        """)
        c.execute("""
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT,
                player TEXT,
                pick TEXT,
                spread_pick TEXT,
                confidence INTEGER,
                created_at TEXT,
                is_correct INTEGER DEFAULT NULL,
                spread_result INTEGER DEFAULT NULL,
                is_main INTEGER DEFAULT 0,
                FOREIGN KEY(game_id) REFERENCES games(game_id)
            )
        """)
        c.execute("""
            CREATE TABLE points_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                delta INTEGER,
                reason TEXT,
                created_at TEXT
            )
        """)
        games = get_games(date.today() + timedelta(days=1))
        for g in games:
            c.execute(
                """
                INSERT OR IGNORE INTO games
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Scheduled')
                """,
                (
                    g["game_id"],
                    g["away_name"],
                    g["home_name"],
                    g["game_date"],
                    g["game_datetime"],
                    g["venue"],
                    g["ml_away"],
                    g["ml_home"],
                    g["runline"],
                ),
            )
        conn.commit()
    else:
        # 這裡是舊 DB 版本升級：逐一嘗試加欄位
        for col_def in [
            ("is_admin", "INTEGER", 0),
            ("is_blocked", "INTEGER", 0),
            ("points", "INTEGER", 100),
            ("last_bonus_date", "TEXT", "NULL"),
            ("is_active", "INTEGER", 1),  # 新增：是否已啟用（1=已啟用,0=待審核）
        ]:
            col, col_type, default = col_def
            try:
                c.execute(f"ALTER TABLE users ADD COLUMN {col} {col_type} DEFAULT {default}")
                conn.commit()
            except sqlite3.OperationalError:
                pass

        try:
            c.execute("ALTER TABLE predictions ADD COLUMN is_main INTEGER DEFAULT 0")
            conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            c.execute("""
                CREATE TABLE points_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    delta INTEGER,
                    reason TEXT,
                    created_at TEXT
                )
            """)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    conn.close()


def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def get_or_create_user(username, password):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "SELECT username, password, is_admin, is_blocked, points, is_active FROM users WHERE username=?",
        (username,),
    )
    row = c.fetchone()

    # 已有帳號：只用來檢查登入，不在這裡處理審核邏輯
    if row:
        if row[1] == password:
            conn.close()
            is_admin = bool(row[2])
            is_blocked = bool(row[3])
            return True, is_admin, is_blocked
        else:
            conn.close()
            return False, False, False

    # 沒有帳號：建立新帳號，預設 is_active=0, points=0
    is_admin = 1 if (username == "admin" and password == "admin123") else 0
    is_blocked = 0

    try:
        c.execute(
            """
            INSERT INTO users (username, password, is_admin, is_blocked, points, last_bonus_date, is_active)
            VALUES (?, ?, ?, ?, 0, NULL, ?)
            """,
            (username, password, is_admin, is_blocked, 1 if is_admin else 0),
        )
        conn.commit()
        conn.close()
        return True, bool(is_admin), bool(is_blocked)
    except sqlite3.IntegrityError:
        conn.close()
        return False, False, False

        
def get_user_row(username):
    """只查詢使用者資料，不自動建立帳號"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "SELECT username, password, is_admin, is_blocked, points, is_active FROM users WHERE username=?",
        (username,),
    )
    row = c.fetchone()
    conn.close()
    return row

def get_user_points(username):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT points FROM users WHERE username=?", (username,))
    row = c.fetchone()
    conn.close()
    if row is None:
        return 0
    return int(row[0])

def update_user_points(username, delta):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "UPDATE users SET points = points + ? WHERE username=?",
        (delta, username),
    )
    conn.commit()
    conn.close()

def log_points_change(username, delta, reason):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO points_logs (username, delta, reason, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (username, delta, reason, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()

def get_latest_points_log(username):
    conn = get_db()
    df = pd.read_sql_query(
        """
        SELECT delta, reason, created_at
        FROM points_logs
        WHERE username=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        conn,
        params=(username,),
    )
    conn.close()
    if df.empty:
        return None
    return df.iloc[0]
    
def get_recent_points_logs_all(limit=100):
    """取得全站最近 limit 筆點數異動紀錄"""
    conn = get_db()
    df = pd.read_sql_query(
        """
        SELECT username, delta, reason, created_at
        FROM points_logs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        conn,
        params=(limit,),
    )
    conn.close()
    if df.empty:
        return []
    return df.to_dict(orient="records")
    

def apply_daily_bonus_if_needed(username):
    """若今天尚未發每日加成，發一次性獎勵，回傳 bool 表示是否有發獎金。"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT last_bonus_date FROM users WHERE username=?", (username,))
    row = c.fetchone()
    today_str = date.today().strftime("%Y-%m-%d")

    already_bonus = (row is not None and row[0] == today_str)
    if not already_bonus:
        c.execute(
            "UPDATE users SET points = points + ?, last_bonus_date=? WHERE username=?",
            (DAILY_BONUS_POINTS, today_str, username),
        )
        conn.commit()
    conn.close()
    return not already_bonus

def save_prediction(game_id, player, pick, spread_pick, confidence):
    """寫入 / 更新一筆預測紀錄（避免長時間佔用 DB 鎖）"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO predictions (game_id, player, pick, spread_pick, confidence, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            """,
            (game_id, player, pick, spread_pick, confidence),
        )
    conn.commit()
    conn.close()

def set_main_pick(player, record_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("UPDATE predictions SET is_main=0 WHERE player=?", (player,))
    c.execute("UPDATE predictions SET is_main=1 WHERE player=? AND id=?", (player, record_id))
    conn.commit()
    conn.close()

def get_player_latest_prediction(game_id, player):
    conn = get_db()
    df = pd.read_sql_query(
        """
        SELECT * FROM predictions
        WHERE game_id=? AND player=?
        ORDER BY created_at DESC LIMIT 1
        """,
        conn,
        params=(game_id, player),
    )
    conn.close()
    return df.iloc[0] if not df.empty else None

def set_game_result(game_id, winner_pick, spread_winner):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        """
        UPDATE predictions
        SET is_correct = CASE WHEN pick=? THEN 1 ELSE 0 END
        WHERE game_id=?
        """,
        (winner_pick, game_id),
    )
    if spread_winner == "push":
        c.execute(
            """
            UPDATE predictions
            SET spread_result = NULL
            WHERE game_id=?
            """,
            (game_id,),
        )
    else:
        c.execute(
            """
            UPDATE predictions
            SET spread_result = CASE WHEN spread_pick=? THEN 1 ELSE 0 END
            WHERE game_id=?
            """,
            (spread_winner, game_id),
        )
    conn.commit()

    df = pd.read_sql_query(
        """
        SELECT player, is_correct, is_main
        FROM predictions
        WHERE game_id=?
        """,
        conn,
        params=(game_id,),
    )
    if not df.empty:
        for _, row in df.iterrows():
            player = row["player"]
            is_correct = row["is_correct"]
            is_main = row["is_main"]
            if is_correct == 1:
                bonus = 40 + (20 if is_main == 1 else 0)
                c.execute(
                    "UPDATE users SET points = points + ? WHERE username=?",
                    (bonus, player),
                )
                conn.commit()
                reason = "主力推命中獎勵" if is_main == 1 else "預測命中獎勵"
                log_points_change(player, bonus, reason)

    conn.close()

def get_leaderboard(where_clause="", params=(), use_spread=False):
    col = "spread_result" if use_spread else "is_correct"
    conn = get_db()
    query = f"""
        SELECT 
            player,
            COUNT(*) as total_games,
            SUM(CASE WHEN {col}=1 THEN 1 ELSE 0 END) as win_games,
            ROUND(AVG({col})*100.0, 1) as win_rate,
            AVG(confidence) as avg_conf
        FROM predictions
        WHERE {col} IS NOT NULL
        {where_clause}
        GROUP BY player
        HAVING total_games > 0
        ORDER BY win_rate DESC, win_games DESC
    """
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def export_predictions_to_csv():
    conn = get_db()
    df = pd.read_sql_query("SELECT * FROM predictions", conn)
    conn.close()
    path = "predictions_export.csv"
    df.to_csv(path, index=False)
    return path

def get_all_users():
    conn = get_db()
    df = pd.read_sql_query("SELECT id, username, is_admin, is_blocked, points FROM users", conn)
    conn.close()
    return df

def update_user_block(username, blocked: bool):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "UPDATE users SET is_blocked=? WHERE username=?",
        (1 if blocked else 0, username),
    )
    conn.commit()
    conn.close()

def get_all_predictions_join():
    conn = get_db()
    df = pd.read_sql_query(
        """
        SELECT 
            p.id,
            p.player,
            p.game_id,
            g.away_team,
            g.home_team,
            g.game_datetime,
            p.pick,
            p.spread_pick,
            p.confidence,
            p.is_correct,
            p.spread_result,
            p.is_main,
            p.created_at
        FROM predictions p
        LEFT JOIN games g ON p.game_id = g.game_id
        ORDER BY p.created_at DESC
        """,
        conn,
    )
    conn.close()
    return df

def get_all_predicted_game_ids():
    """
    從 predictions 抓出所有曾經被預測過的 game_id（去重），
    依照 game_id 排序，回傳 list[str]。
    """
    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT DISTINCT game_id
            FROM predictions
            WHERE game_id IS NOT NULL
            ORDER BY game_id
            """
        )
        rows = c.fetchall()
    return [row[0] for row in rows]

def get_game_ids_by_date_from_created_at(target_date_str: str):
    """
    從 predictions.created_at 判斷「指定日期（台灣時間）」有哪些 game_id 有預測。
    target_date_str 例：'2026-02-22'
    """
    tz_tw = timezone(timedelta(hours=8))

    # 把字串轉成日期（台灣日曆）
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    # 當天台灣時間 00:00 ~ 23:59:59
    start_dt_tw = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=tz_tw)
    end_dt_tw = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=tz_tw)

    # 你的 created_at 是 TEXT，例如 '2026-02-22 13:45:00'，目前先當成「台灣時間字串」來比
    start_str = start_dt_tw.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt_tw.strftime("%Y-%m-%d %H:%M:%S")

    with get_db() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT DISTINCT game_id
            FROM predictions
            WHERE created_at BETWEEN ? AND ?
              AND game_id IS NOT NULL
            ORDER BY game_id
            """,
            (start_str, end_str),
        )
        rows = c.fetchall()

    return [row[0] for row in rows]

def is_user_blocked(username):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT is_blocked FROM users WHERE username=?", (username,))
    row = c.fetchone()
    conn.close()
    if row:
        return bool(row[0])
    return False

# ======= 週 / 月起始日期 helper =======

def get_week_start_today():
    today = date.today()
    start = today - timedelta(days=today.weekday())  # Monday = 0
    return start.strftime("%Y-%m-%d")

def get_month_start_today():
    today = date.today()
    start = today.replace(day=1)
    return start.strftime("%Y-%m-%d")

# ====predictions + 結算結果計算某玩家的徽章列表 ========

def compute_player_badges(player: str):
    """根據 predictions + 結算結果計算某玩家的徽章列表"""
    conn = get_db()
    df = pd.read_sql_query(
        """
        SELECT pick, is_correct, is_main, created_at
        FROM predictions
        WHERE player=?
        ORDER BY created_at ASC
        """,
        conn,
        params=(player,),
    )
    conn.close()

    badges = []

    if df.empty:
        return badges

    # 總場次、命中場次、主力命中
    total_games = df["is_correct"].notnull().sum()
    win_games = df[df["is_correct"] == 1].shape[0]
    main_hits = df[(df["is_main"] == 1) & (df["is_correct"] == 1)].shape[0]
    win_rate = (win_games / total_games) * 100 if total_games > 0 else 0.0

    # 1) 新手起步：10 場已結算
    if total_games >= 10:
        badges.append("新手起步")

    # 2) 穩定射手：50 場以上且勝率 >= 55%
    if total_games >= 50 and win_rate >= 55:
        badges.append("穩定射手")

    # 3) 連勝達人：曾達成 >=3 連勝
    streak = 0
    best_streak = 0
    for _, row in df.iterrows():
        if row["is_correct"] == 1:
            streak += 1
            best_streak = max(best_streak, streak)
        elif row["is_correct"] == 0:
            streak = 0
        # is_correct 為 None（未結算）直接略過
    if best_streak >= 3:
        badges.append("連勝達人")

    # 4) 主力大師：主力推命中場次 >= 10
    if main_hits >= 10:
        badges.append("主力大師")

    return badges


def compute_season_score(player: str, days: int = 365):
    """計算指定期間內的賽季積分（預設最近一年）"""
    conn = get_db()
    since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = pd.read_sql_query(
        """
        SELECT is_correct, is_main, created_at
        FROM predictions
        WHERE player=? AND created_at >= ?
        """,
        conn,
        params=(player, since),
    )
    conn.close()

    if df.empty:
        return 0

    score = 0
    for _, row in df.iterrows():
        if row["is_correct"] == 1:
            # 一般命中 +2
            score += 2
            # 主力推再額外 +3（總共等於 5）
            if row["is_main"] == 1:
                score += 3
    return score

# ===================== Streamlit UI =====================

st.set_page_config(page_title="⚾ MLB 預測王 v4.4", layout="wide", page_icon="⚾")
st.title("⚾ MLB 明日賽程預測系統（登入 + 管理員 + 盤口 + 點數 + 排行榜 + 每日獎勵）")

init_db()

# ---- Sidebar 登入 / 登出 ----
st.sidebar.header("玩家登入 / 註冊")
current_user = st.session_state.get("current_user", None)
is_admin = st.session_state.get("is_admin", False)

if current_user:
    role = "管理員" if is_admin else "一般玩家"
    blocked_flag = is_user_blocked(current_user)
    points = get_user_points(current_user)
    if blocked_flag:
        role += "（已被封鎖）"
    st.sidebar.success(f"目前使用者：{current_user}（{role}）")
    st.sidebar.info(f"目前點數：{points} 點")

    # 最近一次點數異動
    log = get_latest_points_log(current_user)
    if log is not None:
        delta = int(log["delta"])
        reason = log["reason"] or ""
        ts = log["created_at"][:16]
        symbol = "+" if delta > 0 else ""
        st.sidebar.caption(f"最近點數變動：{symbol}{delta} 點（{reason}，{ts}）")

    if st.sidebar.button("登出"):
        st.session_state.pop("current_user", None)
        st.session_state.pop("is_admin", None)
        st.session_state["active_page"] = "明日賽程"
        st.rerun()

else:
    input_user = st.sidebar.text_input("暱稱（帳號）")
    input_pwd = st.sidebar.text_input("密碼", type="password")
    if st.sidebar.button("登入 / 註冊"):
        if input_user and input_pwd:
            # 先查有沒有這個帳號
            row = get_user_row(input_user)
            if row:
                # 已存在帳號：檢查密碼 + 狀態
                username_db, pwd_db, is_admin_db, is_blocked_db, points_db, is_active_db = row
                if pwd_db != input_pwd:
                    st.sidebar.error("登入失敗：帳號已存在但密碼不符。")
                else:
                    is_admin_flag = bool(is_admin_db)
                    is_blocked_flag = bool(is_blocked_db)
                    is_active_flag = bool(is_active_db)
                    if is_blocked_flag:
                        st.sidebar.error("此帳號已被管理員封鎖。")
                    elif (not is_active_flag) and (not is_admin_flag):
                        st.sidebar.warning("帳號已建立，但尚未通過管理員審核，請稍後再試。")
                    else:
                        st.session_state.current_user = username_db
                        st.session_state.is_admin = is_admin_flag
                        st.session_state["active_page"] = "明日賽程"
                        st.sidebar.success(
                            f"已登入：{username_db}" + ("（管理員）" if is_admin_flag else "")
                        )
                        st.rerun()
            else:
                # 沒有帳號：建立一個新的（預設 is_active=0, points=0）
                ok, admin_flag, blocked_flag = get_or_create_user(input_user, input_pwd)
                if ok:
                    if admin_flag:
                        # admin 直接啟用
                        st.session_state.current_user = input_user
                        st.session_state.is_admin = True
                        st.session_state["active_page"] = "明日賽程"
                        st.sidebar.success("已以管理員身分登入。")
                        st.rerun()
                    else:
                        st.sidebar.success("註冊成功！帳號已送交管理員審核，通過後才能登入使用。")
                else:
                    st.sidebar.error("登入失敗：帳號已存在但密碼不符。")
        else:
            st.sidebar.error("請輸入暱稱和密碼。")



current_user = st.session_state.get("current_user", None)
is_admin = st.session_state.get("is_admin", False)
current_blocked = is_user_blocked(current_user) if current_user else False

# ---- Sidebar 功能選單 ----
st.sidebar.markdown("---")
options = ["明日賽程", "預測中心", "我的預測", "我的勝率"]
if is_admin:
    options.append("管理員後台")

menu_choice = st.sidebar.radio("功能選單", options, key="menu_page")

if "active_page" not in st.session_state:
    st.session_state["active_page"] = "明日賽程"
if menu_choice != st.session_state["active_page"]:
    st.session_state["active_page"] = menu_choice

active_page = st.session_state["active_page"]

odds_map = fetch_mlb_odds()

# ===================== 明日賽程 =====================

if active_page == "明日賽程":
    st.header("📅 賽程查詢（statsapi + 市場盤口）")

    # 預設日期：台灣的「明天」
    tz_tw = timezone(timedelta(hours=8))
    now_tw = datetime.now(tz=tz_tw)
    default_date = (now_tw + timedelta(days=1)).date()

    target_date = st.date_input("選擇要查看的日期（台灣日曆）", value=default_date)
    st.caption(f"目前顯示日期：{target_date.strftime('%Y-%m-%d')}")

    games = get_games(target_date)
    if not games:
        st.info("目前查不到『台灣明日』的 MLB 賽程（可能尚未排定）。")
    else:
        df = pd.DataFrame(games)
        df["客隊"] = df["away_name"].map(lambda x: TEAM_NAME_ZH.get(x, x)) + " (客隊)"
        df["主隊"] = df["home_name"].map(lambda x: TEAM_NAME_ZH.get(x, x)) + " (主隊)"

        ml_away_list, ml_home_list = [], []
        spread_point_list, spread_away_list, spread_home_list = [], [], []
        total_point_list, total_over_list, total_under_list = [], [], []

        for _, row in df.iterrows():
            key = (row["away_name"], row["home_name"])
            odds = odds_map.get(key, {})
            ml = odds.get("moneyline", {})
            sp = odds.get("spread", {})
            tot = odds.get("totals", {})

            ml_away_list.append(ml.get("away"))
            ml_home_list.append(ml.get("home"))
            spread_point_list.append(sp.get("point"))
            spread_away_list.append(sp.get("away"))
            spread_home_list.append(sp.get("home"))
            total_point_list.append(tot.get("point"))
            total_over_list.append(tot.get("over"))
            total_under_list.append(tot.get("under"))

        df["客勝賠率"] = ml_away_list
        df["主勝賠率"] = ml_home_list
        df["讓分盤"] = spread_point_list
        df["客隊讓分賠率"] = spread_away_list
        df["主隊讓分賠率"] = spread_home_list
        df["大小分盤"] = total_point_list
        df["大分賠率"] = total_over_list
        df["小分賠率"] = total_under_list

        df_show = df[
            [
                "客隊",
                "主隊",
                "game_datetime",
                "venue",
                "客勝賠率",
                "主勝賠率",
                "讓分盤",
                "客隊讓分賠率",
                "主隊讓分賠率",
                "大小分盤",
                "大分賠率",
                "小分賠率",
            ]
        ].rename(columns={"game_datetime": "開賽時間", "venue": "球場"})
        st.dataframe(df_show, use_container_width=True)

        st.subheader("快速進入預測中心")
        cols = st.columns(len(games))

        # 目前時間（台灣）
        tz_tw = timezone(timedelta(hours=8))
        now_tw = datetime.now(tz=tz_tw)

        for i, g in enumerate(games):
            with cols[i]:
                away_zh = TEAM_NAME_ZH.get(g["away_name"], g["away_name"])
                home_zh = TEAM_NAME_ZH.get(g["home_name"], g["home_name"])

                st.markdown(
                    f"**{away_zh} (客隊)**<br><small>@ {home_zh} (主隊)</small>",
                    unsafe_allow_html=True,
                )

                # 將 UTC 開賽時間轉成台灣時間
                try:
                    game_dt_utc = datetime.fromisoformat(g["game_datetime"].replace("Z", "+00:00"))
                    game_dt_tw = game_dt_utc.astimezone(tz_tw)
                except Exception:
                    game_dt_tw = None

                # 判斷是否允許預測：只看時間
                can_predict = False
                reason_msg = ""

                if game_dt_tw is None:
                    can_predict = False
                    reason_msg = "本場開賽時間異常，暫不開放預測。"
                else:
                    if now_tw < game_dt_tw:
                        can_predict = True
                    else:
                        can_predict = False
                        reason_msg = "此場已開打或已結束，不能再預測。"

                # 顯示開賽時間（台灣）
                if game_dt_tw is not None:
                    st.caption(f"開賽時間（台灣）：{game_dt_tw.strftime('%Y-%m-%d %H:%M')}")
                else:
                    st.caption(f"開賽時間：{g['game_datetime']}")

                if can_predict:
                    if st.button("預測這場", key=f"goto_{g['game_id']}"):
                        st.session_state.selected_game = g
                        st.session_state["active_page"] = "預測中心"
                        st.rerun()
                else:
                    st.button("預測已關閉", key=f"goto_{g['game_id']}", disabled=True)
                    if reason_msg:
                        st.caption(reason_msg)

# ===================== 預測中心（純預測 + 每日加成） =====================

elif active_page == "預測中心":
    st.header("🎯 預測中心")
    if "selected_game" not in st.session_state:
        st.info("請先到『明日賽程』選一場比賽。")
    elif not current_user:
        st.warning("請先在左側登入玩家，再進行預測。")
    elif current_blocked:
        st.error("此帳號已被管理員封鎖，目前無法提交新的預測。")
    else:
        g = st.session_state.selected_game
        away_en = g["away_name"]
        home_en = g["home_name"]
        away_zh = TEAM_NAME_ZH.get(away_en, away_en) + " (客隊)"
        home_zh = TEAM_NAME_ZH.get(home_en, home_en) + " (主隊)"

        # ===== 預測中心：時間 / 狀態檢查 =====
        tz_tw = timezone(timedelta(hours=8))
        now_tw = datetime.now(tz=tz_tw)

        try:
            game_dt_utc = datetime.fromisoformat(g["game_datetime"].replace("Z", "+00:00"))
            game_dt_tw = game_dt_utc.astimezone(tz_tw)
        except Exception:
            game_dt_tw = None

            can_predict = False
        lock_reason = ""

        if game_dt_tw is None:
            can_predict = False
            lock_reason = "本場開賽時間異常，暫不開放預測。"
        else:
            if now_tw < game_dt_tw:
                can_predict = True
            else:
                can_predict = False
                lock_reason = "此場已開打或已結束，不能再預測。"

        col_main, col_odds = st.columns([2, 1])

        with col_main:
            st.markdown(f"### {away_zh} @ {home_zh}")

            if game_dt_tw is not None:
                st.caption(f"{game_dt_tw.strftime('%Y-%m-%d %H:%M')}（台灣時間） • {g['venue']}")
            else:
                st.caption(f"{g['game_datetime']} • {g['venue']}")

            last = get_player_latest_prediction(g["game_id"], current_user)
            user_points = get_user_points(current_user)
            st.write(f"目前點數：{user_points} 點（每次預測消耗 20 點，今日首筆預測額外 +{DAILY_BONUS_POINTS} 點）")

            col1, col2, col3 = st.columns([2, 2, 2])
            with col1:
                pick_radio = st.radio(
                    "勝負預測",
                    ["客勝", "主勝"],
                    horizontal=True,
                    index=0 if last is None or last["pick"] == "away" else 1,
                )
            with col2:
                spread_radio = st.radio(
                    "讓分盤",
                    ["不玩讓分", "主隊過盤", "客隊過盤"],
                    horizontal=True,
                )
            with col3:
                conf_val = st.slider(
                    "信心 ⭐",
                    1,
                    3,
                    2 if last is None else int(last["confidence"]),
                )

            if st.button("💾 儲存預測"):
                # 先檢查是否允許預測
                if not can_predict:
                    st.error(lock_reason or "本場目前已關閉預測。")
                elif user_points < 20:
                    st.error("點數不足，無法預測（每次需 20 點）。")
                else:
                    pick_val = "away" if pick_radio == "客勝" else "home"
                    if spread_radio == "主隊過盤":
                        spread_val = "home_cover"
                    elif spread_radio == "客隊過盤":
                        spread_val = "away_cover"
                    else:
                        spread_val = "none"

                    save_prediction(
                        g["game_id"],
                        current_user,
                        pick_val,
                        spread_val,
                        conf_val,
                    )
                    update_user_points(current_user, -20)
                    log_points_change(current_user, -20, "預測消耗點數 20")

                    got_bonus = apply_daily_bonus_if_needed(current_user)
                    if got_bonus:
                        log_points_change(current_user, DAILY_BONUS_POINTS, "每日首筆預測獎勵")
                        st.success(
                            f"已儲存！已扣除 20 點，今日首筆預測額外獲得 +{DAILY_BONUS_POINTS} 點。"
                        )
                    else:
                        st.success("已儲存！已扣除 20 點。")
                    st.rerun()

            if last is not None:
                last_pick = "客勝" if last["pick"] == "away" else "主勝"
                if last["spread_pick"] == "home_cover":
                    last_spread = "主隊過盤"
                elif last["spread_pick"] == "away_cover":
                    last_spread = "客隊過盤"
                else:
                    last_spread = "不玩讓分"
                st.caption(
                    f"上次：勝負 {last_pick}，讓分 {last_spread}，"
                    f"{last['confidence']}⭐，時間 {last['created_at'][:19]}"
                )

        with col_odds:
            st.subheader("📊 市場盤口（The Odds API）")
            odds = odds_map.get((away_en, home_en))
            if not odds:
                st.info("目前此場尚未開盤或 API 無資料。")
            else:
                ml = odds.get("moneyline", {})
                sp = odds.get("spread", {})
                tot = odds.get("totals", {})

                st.markdown("**Moneyline（勝負賠率）**")
                st.write(f"{away_zh}: {ml.get('away')}")
                st.write(f"{home_zh}: {ml.get('home')}")

                st.markdown("---")
                st.markdown("**Run Line（讓分盤）**")
                point = sp.get("point")
                if point is not None:
                    st.write(f"讓分數：{point}")
                st.write(f"{away_zh} 賠率: {sp.get('away')}")
                st.write(f"{home_zh} 賠率: {sp.get('home')}")

                st.markdown("---")
                st.markdown("**Totals（大小分）**")
                t_point = tot.get("point")
                if t_point is not None:
                    st.write(f"大小分盤：{t_point}")
                st.write(f"大分 Over 賠率: {tot.get('over')}")
                st.write(f"小分 Under 賠率: {tot.get('under')}")

# ===================== 我的預測（主力推卡片） =====================

elif active_page == "我的預測":
    st.header("📓 我的預測紀錄")
    if not current_user:
        st.warning("請先登入後再查看自己的預測紀錄。")
    else:
        conn = get_db()
        df = pd.read_sql_query(
            """
            SELECT 
                p.id,
                p.game_id,
                p.pick,
                p.spread_pick,
                p.confidence,
                p.is_correct,
                p.spread_result,
                p.is_main,
                p.created_at
            FROM predictions p
            WHERE p.player=?
            ORDER BY p.created_at DESC
            """,
            conn,
            params=(current_user,),
        )
        conn.close()

        if df.empty:
            st.info("你目前沒有任何預測紀錄。")
        else:
            # ===== 用 statsapi 依 game_id 補上隊名、開賽時間、比分 =====
            df["away_team"] = None
            df["home_team"] = None
            df["game_datetime"] = None
            df["away_score_val"] = None
            df["home_score_val"] = None

            import statsapi

            unique_game_ids = df["game_id"].dropna().unique().tolist()

            for gid in unique_game_ids:
                try:
                    # 用 schedule 拿隊名與開賽時間
                    sched = statsapi.schedule(game_id=int(gid))
                    if sched:
                        ginfo = sched[0]
                        away_name = ginfo.get("away_name") or ginfo.get("away_team_name")
                        home_name = ginfo.get("home_name") or ginfo.get("home_team_name")
                        game_dt = ginfo.get("game_date") or ginfo.get("game_datetime")
                    else:
                        away_name = None
                        home_name = None
                        game_dt = None

                    # 用 linescore 拿比分
                    try:
                        ls = statsapi.linescore(int(gid))
                        away_score = ls.get("teams", {}).get("away", {}).get("runs")
                        home_score = ls.get("teams", {}).get("home", {}).get("runs")
                    except Exception:
                        away_score = None
                        home_score = None
                except Exception:
                    away_name = None
                    home_name = None
                    game_dt = None
                    away_score = None
                    home_score = None

                df.loc[df["game_id"] == gid, "away_team"] = away_name
                df.loc[df["game_id"] == gid, "home_team"] = home_name
                df.loc[df["game_id"] == gid, "game_datetime"] = game_dt
                df.loc[df["game_id"] == gid, "away_score_val"] = away_score
                df.loc[df["game_id"] == gid, "home_score_val"] = home_score


        if df.empty:
            st.info("你目前沒有任何預測紀錄。")
        else:
            col1, col2 = st.columns(2)
            min_date = df["created_at"].min()[:10]
            max_date = df["created_at"].max()[:10]
            with col1:
                start_d = st.date_input("起始日期", datetime.fromisoformat(min_date))
            with col2:
                end_d = st.date_input("結束日期", datetime.fromisoformat(max_date))

            filtered = df[
                (df["created_at"] >= start_d.strftime("%Y-%m-%d"))
                & (df["created_at"] <= end_d.strftime("%Y-%m-%d") + "T23:59:59")
            ].copy()

            filtered["客隊"] = filtered["away_team"].map(lambda x: TEAM_NAME_ZH.get(x, x))
            filtered["主隊"] = filtered["home_team"].map(lambda x: TEAM_NAME_ZH.get(x, x))

            filtered = filtered.rename(
                columns={
                    "id": "紀錄ID",
                    "game_id": "比賽編號",
                    "game_datetime": "開賽時間",
                    "away_score_val": "客隊得分",
                    "home_score_val": "主隊得分",
                    "pick": "勝負預測",
                    "spread_pick": "讓分預測",
                    "confidence": "信心星數",
                    "is_correct": "勝負命中",
                    "spread_result": "讓分命中",
                    "is_main": "主力推",
                    "created_at": "建立時間",
                }
            )

            main_df = filtered[filtered["主力推"] == 1]
            other_df = filtered[filtered["主力推"] != 1]

            st.markdown("### ⭐ 我的主力推薦")
            if main_df.empty:
                st.info("目前尚未選擇主力推，請從下方列表挑一場設定。")
            else:
                m = main_df.iloc[0]
                bg = """
                <div style="
                    border-radius: 8px;
                    padding: 12px 16px;
                    margin-bottom: 16px;
                    background: linear-gradient(90deg, #ff9a3c, #ffcc70);
                    color: #000000;
                    font-weight: 600;
                ">
                    <div style="font-size: 18px; margin-bottom: 4px;">
                        ⭐ 主力推：{away} @ {home}
                    </div>
                    <div style="font-size: 14px;">
                        開賽時間：{dt}　｜　比分：{away_score}-{home_score}　｜　勝負：{pick}　讓分：{spread}　信心：{conf}⭐
                    </div>
                    <div style="font-size: 12px; margin-top: 4px;">
                        建立時間：{created}
                    </div>
                </div>
                """.format(
                    away=m["客隊"],
                    home=m["主隊"],
                    dt=m["開賽時間"],
                    away_score=("" if pd.isna(m["客隊得分"]) else int(m["客隊得分"])),
                    home_score=("" if pd.isna(m["主隊得分"]) else int(m["主隊得分"])),
                    pick=m["勝負預測"],
                    spread=m["讓分預測"],
                    conf=m["信心星數"],
                    created=m["建立時間"][:19],
                )
                st.markdown(bg, unsafe_allow_html=True)

            st.markdown("### 📋 全部預測紀錄")
            if other_df.empty and main_df.empty:
                st.info("你目前沒有任何預測紀錄。")
            else:
                display_df = pd.concat([main_df, other_df])
                show_cols = [
                    "紀錄ID",
                    "比賽編號",
                    "客隊",
                    "主隊",
                    "開賽時間",
                    "客隊得分",
                    "主隊得分",
                    "勝負預測",
                    "讓分預測",
                    "信心星數",
                    "勝負命中",
                    "讓分命中",
                    "主力推",
                    "建立時間",
                ]

                st.dataframe(display_df[show_cols], use_container_width=True)

            st.markdown("### 🔍 從列表選擇主力推")
            if not filtered.empty:
                record_choices = [
                    f"{row['紀錄ID']} | {row['客隊']} @ {row['主隊']} | {row['開賽時間']}"
                    for _, row in filtered.iterrows()
                ]
                selected = st.selectbox("選擇一筆預測作為主力推", record_choices)
                if selected:
                    rec_id = int(selected.split('|')[0].strip())
                    if st.button("設定為主力推"):
                        set_main_pick(current_user, rec_id)
                        st.success("已更新主力推！")
                        st.rerun()

# ===================== 我的勝率 =====================

elif active_page == "我的勝率":
    st.header("📈 我的勝率")
    if not current_user:
        st.warning("請先登入後再查看。")
    else:
        # 先顯示自己的徽章與賽季積分
        badges = compute_player_badges(current_user)
        season_score = compute_season_score(current_user, days=365)

        st.subheader("我的成就與賽季積分")
        if badges:
            st.write(f"🥇 徽章：{'、'.join(badges)}")
        else:
            st.write("目前尚未取得任何徽章，加油！")
        st.write(f"🏆 賽季積分（最近一年）：{season_score} 分")

        st.markdown("---")

        st.subheader("總成績（全部已結算比賽）")
        df_my = get_leaderboard(" AND player=?", (current_user,), use_spread=False)
        if df_my.empty:
            st.info("尚無已結算的勝負盤資料。")
        else:
            row = df_my.iloc[0]
            st.write(
                f"總場次：{int(row['total_games'])}，命中：{int(row['win_games'])}，"
                f"勝率：{row['win_rate']}%，平均信心：{row['avg_conf']:.2f}⭐"
            )

        week_start = get_week_start_today()
        month_start = get_month_start_today()

        st.subheader("本週表現")
        df_week = get_leaderboard(
            " AND player=? AND created_at >= ?",
            (current_user, week_start),
            use_spread=False,
        )
        if df_week.empty:
            st.info("本週尚無已結算比賽。")
        else:
            row = df_week.iloc[0]
            st.write(
                f"本週場次：{int(row['total_games'])}，命中：{int(row['win_games'])}，勝率：{row['win_rate']}%。"
            )

        st.subheader("本月表現")
        df_month = get_leaderboard(
            " AND player=? AND created_at >= ?",
            (current_user, month_start),
            use_spread=False,
        )
        if df_month.empty:
            st.info("本月尚無已結算比賽。")
        else:
            row = df_month.iloc[0]
            st.write(
                f"本月場次：{int(row['total_games'])}，命中：{int(row['win_games'])}，勝率：{row['win_rate']}%。"
            )

# ===================== 管理員後台 =====================

elif active_page == "管理員後台":
    st.header("👑 管理員後台")
    if not is_admin:
        st.warning("此區僅限管理員登入使用。")
    else:
        # --- 待審核帳號 ---
        st.subheader("🧾 待審核帳號")
        conn_pending = get_db()
        pending_df = pd.read_sql_query(
            """
            SELECT id, username, points, is_active
            FROM users
            WHERE is_admin = 0
            ORDER BY id ASC
            """,
            conn_pending,
        )
        conn_pending.close()

        # 只顯示 is_active = 0 的帳號
        pending_df = pending_df[pending_df["is_active"] == 0]

        if pending_df.empty:
            st.info("目前沒有待審核的帳號。")
        else:
            for _, row in pending_df.iterrows():
                cols = st.columns([2, 2, 2, 2])
                with cols[0]:
                    st.write(f"暱稱：**{row['username']}**")
                with cols[1]:
                    init_points = st.number_input(
                        f"初始點數（{row['username']}）",
                        min_value=0,
                        max_value=100000,
                        value=100,
                        key=f"init_points_{row['id']}",
                    )
                with cols[2]:
                    st.write(f"目前點數：{row['points']}")
                with cols[3]:
                    if st.button(f"通過 {row['username']}", key=f"approve_{row['id']}"):
                        conn2 = get_db()
                        c2 = conn2.cursor()
                        # 設為啟用並給初始點數
                        c2.execute(
                            "UPDATE users SET is_active=1, points=? WHERE id=?",
                            (int(init_points), int(row["id"])),
                        )
                        # 寫一筆點數異動紀錄
                        c2.execute(
                            """
                            INSERT INTO points_logs (username, delta, reason, created_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                row["username"],
                                int(init_points) - int(row["points"]),
                                f"管理員審核通過，設定初始點數 {int(init_points)} 點",
                                datetime.now().isoformat(),
                            ),
                        )
                        conn2.commit()
                        conn2.close()
                        st.success(f"已通過 {row['username']}，設定初始點數 {int(init_points)} 點。")
                        st.rerun()

        st.markdown("---")

        # --- 原本的使用者清單 & 封鎖管理 ---
        st.subheader("使用者清單 & 封鎖管理")
        users_df = get_all_users()
        if not users_df.empty:
            users_show = users_df.rename(
                columns={
                    "id": "ID",
                    "username": "玩家帳號",
                    "is_admin": "是否管理員",
                    "is_blocked": "是否封鎖",
                    "points": "點數",
                }
            )
            st.dataframe(users_show, use_container_width=True)

            st.markdown("### 封鎖 / 解除封鎖 玩家")
            normal_users = users_df[users_df["is_admin"] == 0]
            if normal_users.empty:
                st.info("目前沒有一般玩家帳號可封鎖。")
            else:
                target_user = st.selectbox(
                    "選擇玩家帳號",
                    normal_users["username"].tolist(),
                )
                target_blocked = bool(
                    normal_users[normal_users["username"] == target_user]["is_blocked"].iloc[0]
                )
                if target_blocked:
                    if st.button("解除封鎖"):
                        update_user_block(target_user, False)
                        st.success(f"已解除封鎖：{target_user}")
                        st.rerun()
                else:
                    if st.button("封鎖此玩家"):
                        update_user_block(target_user, True)
                        st.success(f"已封鎖：{target_user}")
                        st.rerun()
        else:
            st.info("目前尚無使用者資料。")
            
            st.markdown("---")
            st.markdown("### 📂 重新同步賽程 games 資料表")

            st.caption("說明：會清空 games 表並以目前 get_games() 抓到的賽程重建，"
                   "不會動到 users 或 predictions。正式賽前或測試階段可用。")

        if  st.button("重新同步 games 表（請謹慎使用）"):
            resync_games_table()
            st.success("已重新同步 games 資料表，接下來的賽程 / 自動結算會使用新資料。")


                # --- 管理員手動調整點數 ---
        st.markdown("---")
        st.markdown("### 💰 手動補充 / 扣除玩家點數")
        if not users_df.empty:
            target_user2 = st.selectbox(
                "選擇玩家帳號（調整點數用）",
                users_df["username"].tolist(),
                key="points_adjust_user",
            )
            delta = st.number_input("調整點數（正數=補點，負數=扣點）", value=10, step=10)
            reason = st.text_input("備註原因（可選填，例如活動獎勵、補償等）")

            if st.button("執行點數調整"):
                update_user_points(target_user2, delta)
                log_reason = reason if reason else "管理員手動調整點數"
                log_points_change(target_user2, delta, log_reason)
                st.success(f"已為 {target_user2} 調整點數 {delta} 點。" + (f" 備註：{reason}" if reason else ""))
                st.rerun()

        # -------- 排行榜區 --------
        st.markdown("---")
        st.subheader("🏆 排行榜")

        tab1, tab2, tab3, tab4 = st.tabs(
            ["總勝率排行榜", "本週勝率", "本月勝率", "玩家點數排行榜"]
        )

        with tab1:
            st.write("全部已結算比賽的勝負盤表現（至少一場）。")
            lb_all = get_leaderboard()
            if lb_all.empty:
                st.info("尚無已結算的資料。")
            else:
                # 加上賽季積分與徽章
                lb_all["season_score"] = lb_all["player"].apply(
                    lambda p: compute_season_score(p, days=365)
                )
                lb_all["badges"] = lb_all["player"].apply(
                    lambda p: "、".join(compute_player_badges(p)) if compute_player_badges(p) else ""
                )

                lb_show = lb_all.rename(
                    columns={
                        "player": "玩家",
                        "total_games": "總場次",
                        "win_games": "命中場次",
                        "win_rate": "勝率%",
                        "avg_conf": "平均信心",
                        "season_score": "賽季積分",
                        "badges": "徽章",
                    }
                )

                # 排序：先看賽季積分，再看勝率與出手場次
                lb_show = lb_show.sort_values(
                    by=["賽季積分", "勝率%", "總場次"],
                    ascending=[False, False, False],
                )

                st.dataframe(lb_show, use_container_width=True)

        with tab2:
            st.write("本週（從本週一開始）勝率排行榜。")
            week_start = get_week_start_today()
            lb_week = get_leaderboard(
                " AND created_at >= ?",
                (week_start,),
                use_spread=False,
            )
            if lb_week.empty:
                st.info("本週尚無已結算的資料。")
            else:
                lb_show = lb_week.rename(
                    columns={
                        "player": "玩家",
                        "total_games": "本週場次",
                        "win_games": "命中場次",
                        "win_rate": "勝率%",
                        "avg_conf": "平均信心",
                    }
                )
                st.dataframe(lb_show, use_container_width=True)

        with tab3:
            st.write("本月（從本月 1 號開始）勝率排行榜。")
            month_start = get_month_start_today()
            lb_month = get_leaderboard(
                " AND created_at >= ?",
                (month_start,),
                use_spread=False,
            )
            if lb_month.empty:
                st.info("本月尚無已結算的資料。")
            else:
                lb_show = lb_month.rename(
                    columns={
                        "player": "玩家",
                        "total_games": "本月場次",
                        "win_games": "命中場次",
                        "win_rate": "勝率%",
                        "avg_conf": "平均信心",
                    }
                )
                st.dataframe(lb_show, use_container_width=True)

        with tab4:
            st.write("依點數由高到低顯示目前最有錢的玩家。")
            if users_df.empty:
                st.info("目前尚無使用者資料。")
            else:
                pts_df = users_df.sort_values("points", ascending=False).rename(
                    columns={
                        "username": "玩家",
                        "points": "點數",
                        "is_blocked": "是否封鎖",
                        "is_admin": "是否管理員",
                    }
                )
                st.dataframe(pts_df[["玩家", "點數", "是否封鎖", "是否管理員"]], use_container_width=True)

    # ===================== 單場比賽自動結算 =====================
    with st.expander("⚙ 單場比賽自動結算", expanded=False):
        # 從 predictions 抓出所有有預測紀錄的 game_id
        game_ids = get_all_predicted_game_ids()

        if not game_ids:
            st.info("目前尚無任何有預測紀錄的比賽。")
        else:
            # 做一個 map：game_id -> 顯示文字（包含當前狀態＆比分）
            options = []
            label_dict = {}

            for gid in game_ids:
                try:
                    # 用你原本的工具抓比分＆狀態
                    away_score, home_score, status_str = fetch_game_final_score_from_statsapi(str(gid))
                    if away_score is not None and home_score is not None:
                        label = f"{gid} - 比分 {away_score} : {home_score}（{status_str}）"
                    else:
                        label = f"{gid} - 尚無完整比分（{status_str}）"
                except Exception as e:
                    label = f"{gid} - 取得比分失敗：{e}"

                options.append(gid)
                label_dict[gid] = label

            selected_gid = st.selectbox(
                "選擇要自動結算的比賽（只列出曾經有被預測過的 game_id）",
                options=options,
                format_func=lambda x: label_dict.get(x, str(x)),
            )

            if st.button("對此單場比賽執行自動結算"):
                if selected_gid is None:
                    st.warning("請先選擇一場比賽。")
                else:
                    with st.spinner(f"正在自動結算比賽 {selected_gid} ..."):
                        try:
                            away_score, home_score, status_str = fetch_game_final_score_from_statsapi(str(selected_gid))

                            if status_str != "Final":
                                st.error(f"比賽狀態為「{status_str}」，尚未 Final，無法自動結算。")
                            elif away_score is None or home_score is None:
                                st.error("目前無法取得完整比分，請稍後再試。")
                            else:
                                if away_score > home_score:
                                    winner_pick = "away"
                                    spread_winner = "away"
                                elif home_score > away_score:
                                    winner_pick = "home"
                                    spread_winner = "home"
                                else:
                                    winner_pick = "push"
                                    spread_winner = "push"

                                set_game_result(str(selected_gid), winner_pick, spread_winner)
                                st.success(
                                    f"比賽 {selected_gid} 已根據比分 {away_score} : {home_score} 完成自動結算。"
                                )
                        except Exception as e:
                            st.error(f"自動結算過程發生錯誤：{e}")

                                
    # ===================== 指定日期一鍵自動結算 =====================
    with st.expander("📅 指定日期一鍵自動結算（依玩家預測時間）", expanded=False):
        st.write("說明：依照 predictions.created_at（台灣時間）判斷當天有哪些比賽被預測過，並對已 Final 的比賽執行自動結算。")

        target_date = st.date_input("選擇要一鍵結算的日期（台灣時間）")

        if st.button("一鍵結算該日期所有已結束比賽"):
            date_str = target_date.strftime("%Y-%m-%d")
            game_ids_for_day = get_game_ids_by_date_from_created_at(date_str)

            if not game_ids_for_day:
                st.info(f"{date_str} 這一天沒有任何玩家預測紀錄。")
            else:
                st.write(f"{date_str} 共有 {len(game_ids_for_day)} 場「曾被預測」的比賽，開始自動檢查並結算 Final 場次...")

                settled = []
                skipped = []

                for gid in game_ids_for_day:
                    try:
                        away_score, home_score, status_str = fetch_game_final_score_from_statsapi(str(gid))

                        if status_str != "Final":
                            skipped.append((gid, status_str))
                            continue
                        if away_score is None or home_score is None:
                            skipped.append((gid, f"{status_str} / 無比分"))
                            continue

                        if away_score > home_score:
                            winner_pick = "away"
                            spread_winner = "away"
                        elif home_score > away_score:
                            winner_pick = "home"
                            spread_winner = "home"
                        else:
                            winner_pick = "push"
                            spread_winner = "push"

                        set_game_result(str(gid), winner_pick, spread_winner)
                        settled.append((gid, away_score, home_score, status_str))
                    except Exception as e:
                        skipped.append((gid, f"發生錯誤：{e}"))

                if settled:
                    st.success(f"已自動結算 {len(settled)} 場比賽：")
                    for gid, a, h, st_str in settled:
                        st.write(f"- 比賽 {gid}：客 {a} 分，主 {h} 分（{st_str}）")

                if skipped:
                    st.warning("以下比賽未自動結算（比分未知、尚未 Final 或其他原因）：")
                    for gid, st_str in skipped:
                        st.write(f"- 比賽 {gid}（狀態：{st_str}）")

                # -------- 點數異動紀錄 --------
        st.markdown("---")
        st.markdown("### 📜 點數異動紀錄（最近 100 筆）")
        logs_all = get_recent_points_logs_all(100)
        if logs_all:
            logs_df = pd.DataFrame(logs_all)
            logs_df = logs_df.rename(
                columns={
                    "username": "玩家",
                    "delta": "變動點數",
                    "reason": "原因",
                    "created_at": "時間",
                }
            )
            st.dataframe(
                logs_df[["玩家", "變動點數", "原因", "時間"]],
                use_container_width=True,
            )
        else:
            st.write("尚無點數異動紀錄。")

        st.markdown("---")
        st.subheader("📤 匯出所有預測為 CSV")
        if st.button("匯出 predictions_export.csv"):
            path = export_predictions_to_csv()
            st.success(f"已匯出到 {path}")

