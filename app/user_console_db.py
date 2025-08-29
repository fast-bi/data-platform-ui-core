import psycopg2
import json
from psycopg2 import extras

class UserConsoleMetadataHandler:
    def __init__(self, db_config):
        self.db_config = db_config
        self.user_table = 'users'
        self.ensure_user_table_exists()

    def get_connection(self):
        return psycopg2.connect(**self.db_config)

    def ensure_user_table_exists(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.user_table} (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(64) NOT NULL,
                        email VARCHAR(64),
                        first_login_time TIMESTAMP,
                        last_login_time TIMESTAMP,
                        follow_mode VARCHAR(6) DEFAULT 'off',
                        iframe_mode VARCHAR(16) DEFAULT 'enabled',
                        light_dark_mode VARCHAR(6) DEFAULT 'light'
                    );
                """)
                connection.commit()

    def add_user(self, username, email, follow_mode='off', iframe_mode='enabled', light_dark_mode='light'):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {self.user_table} (username, email, follow_mode, iframe_mode, light_dark_mode)
                    VALUES (%s, %s, %s, %s, %s) RETURNING id;
                """, (username, email, follow_mode, iframe_mode, light_dark_mode))
                user_id = cur.fetchone()[0]
                connection.commit()
                return user_id

    def update_login_time(self, user_id):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.user_table}
                    SET last_login_time = CURRENT_TIMESTAMP,
                        first_login_time = COALESCE(first_login_time, CURRENT_TIMESTAMP)
                    WHERE id = %s;
                """, (user_id,))
                connection.commit()

    def get_user_by_email(self, email):
        with self.get_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(f"""
                    SELECT * FROM {self.user_table} WHERE email = %s;
                """, (email,))
                user = cur.fetchone()
                return user

    def get_user_by_id(self, user_id):
        with self.get_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                try:
                    cur.execute(f"""
                        SELECT * FROM {self.user_table} WHERE id = %s;
                    """, (user_id,))
                    return cur.fetchone()
                except Exception as e:
                    # Rollback the transaction on error
                    connection.rollback()
                    print(f"Error fetching user by ID: {e}")
                    return None
        
    def update_user(self, user_id, **kwargs):
        set_clause = ', '.join([f"{key} = %s" for key in kwargs])
        values = list(kwargs.values())
        values.append(user_id)
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.user_table}
                    SET {set_clause}
                    WHERE id = %s;
                """, values)
                connection.commit()
