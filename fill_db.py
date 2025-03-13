import mysql.connector
import random
import string
from datetime import datetime, timedelta

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
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print("❌ Ошибка подключения к БД:", e)
        raise

def generate_word():
    """
    Генерирует случайное слово из 8 строчных латинских букв.
    :return: Строка из 8 символов
    """
    return ''.join(random.choices(string.ascii_lowercase, k=8))

def insert_logs(total_actions_per_day, start_date):
    """
    Генерирует и вставляет в базу данных случайные логи пользовательских действий.

    :param total_actions_per_day: Количество логов, создаваемых в день.
    :param start_date: Дата начала генерации логов (datetime).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, title FROM action_type")
    actions = {row[1]: row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT id, name FROM status_type")
    statuses = {row[1]: row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT id, name FROM entity_type")
    entities = {row[1]: row[0] for row in cursor.fetchall()}

    users = []
    user_state = {}

    visited_ip = []

    topics = []
    topic_count = 0
    messages = []
    message_count = 0

    logs = []
    user_count = 0

    days = 30

    for i in range(days):
        current_date = start_date + timedelta(days=i)
        log_time = current_date
        min_actions_per_type = 5
        action_counts = {action: min_actions_per_type if action != 'first_visit' else 0 for action in actions }
        remaining_actions = total_actions_per_day - sum(action_counts.values())

        for _ in range(remaining_actions):
            action = random.choice(list(actions.keys()))
            action_counts[action] += 1

        for action, count in action_counts.items():
            for _ in range(count):
                user_id = random.choice(users) if users and random.random() > 0.5 else None
                status_id = statuses["success"]
                entity_id = None
                entity_type_id = None
                log_time += timedelta(minutes=random.randint(0, 1), seconds=random.randint(0, 3))
                user_ip_address = f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"

                if user_ip_address not in visited_ip:
                    visited_ip.append(user_ip_address)
                    logs.append((None, actions["first_visit"], entity_type_id, entity_id, status_id, log_time))
                    continue

                if action == "registration":
                    if not user_id or not user_state.get(user_id, {}).get('registered', False):
                        username = generate_word()
                        cursor.execute("SELECT COUNT(*) FROM users WHERE name = %s", (username,))
                        result = cursor.fetchone()
                        while result[0] > 0:
                            username = generate_word()
                            cursor.execute("SELECT COUNT(*) FROM users WHERE name = %s", (username,))
                            result = cursor.fetchone()
                        user_count += 1
                        cursor.execute("INSERT INTO users (name) VALUES (%s)", (username,))
                        conn.commit()
                        user_id = cursor.lastrowid
                        users.append(user_id)
                        user_state[user_id] = {'logged_in': True, 'registered': True, 'name': username, 'ip': user_ip_address}
                    else:
                        status_id = statuses["error"]

                elif action == "login":
                    if user_id and user_state.get(user_id, {}).get('registered', False):
                        if not user_state[user_id]['logged_in']:
                            user_state[user_id]['logged_in'] = True
                        else:
                            status_id = statuses["error"]
                    else:
                        status_id = statuses["error"]

                elif action == "logout":
                    if user_id and user_state.get(user_id, {}).get('logged_in', False):
                        user_state[user_id]['logged_in'] = False
                    else:
                        status_id = statuses["error"]

                elif action == "create_topic":
                    if user_id and user_state.get(user_id, {}).get('logged_in', False):
                        topic_count += 1
                        topic_title = generate_word()
                        cursor.execute("INSERT INTO topics (user_id, title, created_time) VALUES (%s, %s, %s)", (user_id, topic_title, log_time))
                        conn.commit()
                        topic_id = cursor.lastrowid
                        topics.append(topic_id)
                        entity_type_id = entities["topic"]
                        entity_id = topic_id
                    else:
                        status_id = statuses["error"]

                elif action == "view_topic":
                    if not topics:
                        status_id = statuses["error"]

                elif action == "delete_topic":
                    if user_id and user_state.get(user_id, {}).get('logged_in', False):
                        cursor.execute("SELECT id FROM topics WHERE user_id = %s ORDER BY RAND() LIMIT 1", (user_id,))
                        topic_to_delete = cursor.fetchone()
                        if topic_to_delete:
                            entity_type_id = entities["topic"]
                            entity_id = topic_to_delete[0]
                            topics.remove(topic_to_delete[0])
                            cursor.execute("DELETE FROM topics WHERE id = %s", (topic_to_delete[0],))
                            conn.commit()
                        else:
                            status_id = statuses["error"]
                    else:
                        status_id = statuses["error"]

                elif action == "write_message":
                    if topics:
                        message_count += 1
                        topic_id_for_message = random.choice(topics)
                        messages_content = generate_word()
                        cursor.execute("INSERT INTO messages (user_id, topic_id, content, created_time) VALUES (%s, %s, %s, %s)",
                                       (user_id, topic_id_for_message, messages_content, log_time))
                        conn.commit()
                        message_id = cursor.lastrowid
                        messages.append(message_id)
                        entity_type_id = entities["message"]
                        entity_id = message_id
                    else:
                        status_id = statuses["error"]

                logs.append((user_id, actions[action], entity_type_id, entity_id, status_id, log_time))

    cursor.executemany(
        "INSERT INTO logs (user_id, action_id, entity_type_id, entity_id, status_type_id, time) VALUES (%s, %s, %s, %s, %s, %s)", logs
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Добавлено {len(logs)} логов! ✅ Зарегистрировано {user_count} новых пользователей!")


if __name__ == "__main__":
    start_date = input("Введите начальную дату (YYYY-MM-DD): ")
    datetime.strptime(start_date, "%Y-%m-%d")
    insert_logs(800, datetime.strptime(start_date, "%Y-%m-%d"))