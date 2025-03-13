import pymysql


DB_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "user",
    "password": "password",
    "database": "forum_logs_db"
}

def get_db_connection():
    """
    Устанавливает соединение с базой данных.
    :return: Объект соединения MySQL
    """
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.Error as e:
        print("❌ Ошибка подключения к БД:", e)
        raise

def delete_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    delete_query = "TRUNCATE TABLE logs"
    cursor.execute(delete_query)

    cursor.execute("DELETE FROM topics")
    cursor.execute("ALTER TABLE topics AUTO_INCREMENT = 1")

    cursor.execute("DELETE FROM messages")
    cursor.execute("ALTER TABLE messages AUTO_INCREMENT = 1")

    delete_users = "DELETE FROM users"
    cursor.execute(delete_users)
    reset_autoincrement = "ALTER TABLE users AUTO_INCREMENT = 1"
    cursor.execute(reset_autoincrement)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Данные успешно удалены из таблицы logs!")

if __name__ == "__main__":
    delete_data()