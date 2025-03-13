from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "user": "user",
    "password": "password",
    "database": "forum_logs_db",
    "port": 3307
}

DB_URI = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URI)

def execute_query(query):
    """
    Выполняет SQL-запрос и возвращает DataFrame.
    :param query: SQL-запрос (str)
    :return: DataFrame с результатами или None в случае ошибки
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)
            if df.empty:
                print("⚠️ Запрос вернул пустой результат.")
                return None
            return df
    except Exception as e:
        print(f"❌ Ошибка при выполнении SQL-запроса: {e}")
        return None

def get_aggregated_data(start_date, end_date):
    """
    Извлекает агрегированные данные за указанный период.
    :param start_date: Начальная дата в формате YYYY-MM-DD.
    :param end_date: Конечная дата в формате YYYY-MM-DD.
    :return: DataFrame с агрегированными данными или None в случае ошибки.
    """
    query = f"""
            WITH daily_topics_created AS (
        SELECT DATE(`time`) AS day, COUNT(*) AS topics_count
        FROM logs
        WHERE action_id = (SELECT id FROM action_type WHERE title = 'create_topic')
        AND `time` >= '{start_date}' 
        AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)
        AND status_type_id = (SELECT id FROM status_type WHERE `name` = 'success')
        GROUP BY day
    ),
    daily_topics_deleted AS (
        SELECT DATE(`time`) AS day, COUNT(*) AS topics_count
        FROM logs
        WHERE action_id = (SELECT id FROM action_type WHERE title = 'delete_topic')
        AND `time` >= '{start_date}' 
        AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)
        AND status_type_id = (SELECT id FROM status_type WHERE `name` = 'success')
        GROUP BY day
    ),
    total_topics_on_t AS (
        SELECT d.day, (SELECT COUNT(*) FROM topics WHERE DATE(`created_time`) <= d.day) AS topics_count_t
        FROM (SELECT DISTINCT DATE(time) AS day FROM logs WHERE `time` >= '{start_date}' 
        AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)) d
    ),
    daily_messages AS (
        SELECT DATE(`time`) AS day, COUNT(*) AS total_messages, SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS anonymous_messages
        FROM logs
        WHERE action_id = (SELECT id FROM action_type WHERE title = 'write_message')
        AND `time` >= '{start_date}' 
        AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)
        AND status_type_id = (SELECT id FROM status_type WHERE `name` = 'success')
        GROUP BY day
    ),
    daily_users AS (
        SELECT DATE(time) AS day, COUNT(user_id) AS new_users
        FROM logs
        WHERE action_id = (SELECT id FROM action_type WHERE title = 'registration')
        AND `time` >= '{start_date}' 
        AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)
        AND status_type_id = (SELECT id FROM status_type WHERE `name` = 'success')
        GROUP BY day
    )
    SELECT 
        d.day,
        COALESCE(u.new_users, 0) AS new_users,
        COALESCE(m.total_messages, 0) AS total_messages,
        COALESCE(m.anonymous_messages, 0) AS anonymous_messages,
        COALESCE(t.topics_count, 0) AS topics_today_created,
        COALESCE(td.topics_count, 0) AS topics_today_deleted,
        COALESCE(tt.topics_count_t, 0) AS topics_today_total,
        LAG(COALESCE(tt.topics_count_t, 0), 1, 0) OVER (ORDER BY d.day) AS topics_yes_total
    FROM 
        (SELECT DISTINCT DATE(time) AS day FROM logs WHERE `time` >= '{start_date}' AND `time` < DATE_ADD('{end_date}', INTERVAL 1 DAY)) d
    LEFT JOIN daily_users u ON d.day = u.day
    LEFT JOIN daily_messages m ON d.day = m.day
    LEFT JOIN daily_topics_created t ON d.day = t.day
    LEFT JOIN daily_topics_deleted td ON d.day = td.day
    LEFT JOIN total_topics_on_t tt ON d.day = tt.day
    ORDER BY d.day;
    """

    df = execute_query(query)
    if df is None:
        return None

    df["anonymous_messages"] = df["anonymous_messages"].astype(int)

    df["total_topics_today"] = (df["topics_today_created"] - df["topics_today_deleted"])
    df["total_topics_today"] = df["total_topics_today"].fillna(0)
    df["total_topics_on_yesterday"] = (df["topics_yes_total"])
    df["total_topics_on_yesterday"] = df["total_topics_on_yesterday"].fillna(0).astype(int)

    df["anonymous_percentage"] = (df["anonymous_messages"] / df["total_messages"]) * 100
    df["anonymous_percentage"] = df["anonymous_percentage"].fillna(0)

    df["topics_change_percent"] = (df["total_topics_today"] / df["total_topics_on_yesterday"].mask(df["total_topics_on_yesterday"] == 0, 1)) * 100
    df["topics_change_percent"] = df["topics_change_percent"].fillna(0)
    df = df.drop(columns=["topics_today_total","topics_yes_total"])
    df = df.drop(columns=["topics_today_created", "topics_today_deleted"])
    return df


def save_to_csv(df, filename="aggregated_data.csv"):
    """
    Сохраняет DataFrame в CSV-файл.
    :param df: DataFrame, содержащий данные.
    :param filename: Имя файла для сохранения (по умолчанию "aggregated_data.csv").
    """
    df.to_csv(filename, index=False)
    print(f"Файл сохранен: {filename}")


if __name__ == "__main__":
    start_date = input("Введите начальную дату (YYYY-MM-DD): ")
    end_date = input("Введите конечную дату (YYYY-MM-DD): ")

    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        df = get_aggregated_data(start_date, end_date)
        save_to_csv(df)
        print(df)
    except ValueError:
        print("Ошибка: Неверный формат даты. Введите в формате YYYY-MM-DD.")