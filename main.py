import psycopg2


# Функция создания таблиц
def create_table_structure():
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                email VARCHAR(40) UNIQUE
            )
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phone_numbers(
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id),
                phone_number VARCHAR(40) UNIQUE
            )
            """)
        conn.commit()

# Функция добавления клиента
def add_client(first_name, last_name, email):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s)
            RETURNING id
            """, (first_name, last_name, email))
        return cur.fetchone()[0]
        conn.commit()

# Функция добавления телефона
def add_phone(client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phone_numbers (client_id, phone_number) VALUES (%s, %s)
            """, (client_id, phone_number))
        conn.commit()

# Функция изменения данных
def change_client_data(id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        # Создание строки с частями sql запроса
        sql_parts = []
        params = []

        # для каждого не None параметра добавляем его в запрос
        if first_name is not None:
            sql_parts.append("first_name=%s")
            params.append(first_name)
        if last_name is not None:
            sql_parts.append("last_name=%s")
            params.append(last_name)
        if email is not None:
            sql_parts.append("email=%s")
            params.append(email)

        # если нет изменений, выходим из функции
        if not sql_parts:
            return

        # формируем запрос
        sql_query = "UPDATE clients SET " + ", ".join(sql_parts) + " WHERE id=%s"
        params.append(id)
        cur.execute(sql_query, params)
        conn.commit()

# Функция удаления данных
def delete_phone(phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone_numbers WHERE phone_number=%s
            """, (phone_number,))
        conn.commit()

# Функция удаления клиента
def delete_client(id):
    # Удаление всех телефонов
    delele_phones_by_client(id)

    # Удаление самого клиента
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM clients WHERE id=%s
            """, (id,))
        conn.commit()

# Функция удаления всех телефонов клиента
def delele_phones_by_client(client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone_numbers WHERE client_id=%s
            """, (client_id,))
        conn.commit()

# Функция поиска клиента
def find_client(first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        # Начальная часть sql запроса
        sql_query = """
            SELECT * FROM clients
            LEFT JOIN phone_numbers ON clients.id = phone_numbers.client_id
            """
        # Список параметров и их значений для поиска
        search_conditions = []
        params = []

        # Добавляем условия поиска для каждого параметра
        if first_name is not None:
            search_conditions.append("first_name=%s")
            params.append(first_name)
        if last_name is not None:
            search_conditions.append("last_name=%s")
            params.append(last_name)
        if email is not None:
            search_conditions.append("email=%s")
            params.append(email)

        # Если есть условия поиска, то добавляем их в запрос
        if search_conditions:
            sql_query += " WHERE " + " AND ".join(search_conditions)
        else:
            # Если нет условий поиска, то возвращаем пустный список
            return []
        

        # Выполняем запрос
        cur.execute(sql_query, tuple(params))
        return cur.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(database="netology_db", user="postgres", password="password") as conn:
        # Добавление таблицы
        # create_table_structure()

        # Добавление клиента
        # id = add_client('Den', 'Kuznetsov', 'k@k.k')

        # # Добавление телефона
        # add_phone(20, '111-111-111')

        # Изменение данных
        # change_client_data(20,last_name='Pec')

        # Удаление телефона
        # delete_phone('777-777-777')

        # Удаление клиента
        # delete_client(20)

        # Поиск клиента
        # print(find_client(email='k@k.k'))