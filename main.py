import psycopg2
from psycopg2.sql import SQL, Identifier

# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            Client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(40),
            last_name VARCHAR(40),
            email VARCHAR(40)
            );
            """),
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            phone_id SERIAL PRIMARY KEY,
            phone bigint,
            Client_id INTEGER not null references clients(Client_id)
            );
            """),
    conn.commit()

# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(first_name, last_name, email)
        VALUES(%s, %s, %s)
        RETURNING Client_id;
        """, (first_name, last_name, email))
        id = cur.fetchone()[0]
        if len(phones) > 0:
            for p in phones:
                print(p)
                cur.execute("""
                INSERT INTO phones(phone, Client_id) VALUES(%s, %s);
                """, (p, id))
        else:
            cur.execute("""
            INSERT INTO phones(Client_id) VALUES(%s);
            """, (id))
    conn.commit()

# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, Client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT EXISTS (SELECT * FROM phones WHERE phone = %s AND Client_id = %s);
        """, (phone, Client_id))
        if cur.fetchone()[0] == False:
            cur.execute("""
            INSERT INTO phones(phone, Client_id)
            VALUES(%s, %s);
            """, (phone, Client_id))
        else:
            print(f"номер {phone} уже существует")
    conn.commit()

# Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone=None):
    arg_list ={'first_name':first_name, 'last_name':last_name, 'email':email}
    for key, arg in arg_list.items():
        if arg:
            with conn.cursor() as cur:
                cur.execute(SQL("""
                UPDATE clients SET {} = %s
                WHERE client_id = %s""").format(Identifier(key)), (arg, client_id))
            conn.commit()

# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE phone = %s AND Client_id = %s;
        """, (phone, client_id))
    conn.commit()

# Функция, позволяющая удалить существующего клиента.
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE Client_id = %s;
        """, (client_id,))
        cur.execute("""
        DELETE FROM clients
        WHERE Client_id = %s;
        """, (client_id,))
    conn.commit()

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    arg_list ={'first_name':first_name, 'last_name':last_name, 'email':email, 'phone':phone}
    for key, arg in arg_list.items():
        if arg:
            with conn.cursor() as cur:
                cur.execute(SQL("""
                SELECT * FROM clients
                JOIN phones ON clients.Client_id=phones.Client_id
                WHERE {} = %s;
                """).format(Identifier(key)), (arg,))
            conn.commit()

with psycopg2.connect(database="clients_db", user="postgres", password="239302") as conn:
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE clients CASCADE;
        DROP TABLE phones CASCADE;
        """)
    create_db(conn)

    first_name = "Иван"
    last_name = "Иванов"
    email = "ivan@mail.ru"
    phones = [88005553535, 4242]


    add_client(conn, first_name, last_name, email, phones)

    first_name = "Макс"
    last_name = "Максимов"
    email = "max1@mail.ru"
    phones = [1556]

    add_client(conn, first_name, last_name, email, phones)

    # Проверка на добавление номера телефона
    Client_id = 1
    phone = "3553535"
    add_phone(conn, Client_id, phone)

    Client_id = 1
    phone = "88005553535"
    add_phone(conn, Client_id, phone)

    Client_id = 2
    phone = "65"
    add_phone(conn, Client_id, phone)

    # Функция, позволяющая изменить данные о клиенте
    change_client(conn, "2", first_name="Томка", email="tom@mail.ru")

    # Функция, позволяющая удалить телефон для существующего клиента.
    delete_phone(conn, 1, 3553535)

    # Функция, позволяющая удалить существующего клиента.
    delete_client(conn, 2)

    # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
    find_client(conn, first_name="Иван")
    find_client(conn, phone="88005553535")

conn.close()

