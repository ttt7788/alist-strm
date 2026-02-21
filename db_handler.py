import os
import sqlite3
import uuid
from urllib.parse import urlparse

class DBHandler:
    def __init__(self, db_file=None):
        self.db_file = db_file or os.getenv('DB_FILE', '/config/config.db')
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.initialize_tables()

    def initialize_tables(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS config (
                                config_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                config_name TEXT, 
                                url TEXT, 
                                username TEXT, 
                                password TEXT, 
                                rootpath TEXT,
                                target_directory TEXT,
                                download_enabled INTEGER DEFAULT 1,
                                download_interval_range TEXT
                                )''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS user_config (
                                video_formats TEXT,
                                subtitle_formats TEXT,
                                image_formats TEXT,
                                metadata_formats TEXT,
                                size_threshold INTEGER DEFAULT 100)''')

        # 初始化日志表
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
                                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                log_name TEXT,
                                task_id TEXT,
                                level TEXT,
                                message TEXT,
                                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                )''')

        self.conn.commit()

        self.add_column_if_not_exists('config', 'config_name', 'TEXT')
        self.add_column_if_not_exists('config', 'url', 'TEXT')
        self.add_column_if_not_exists('config', 'download_enabled', 'INTEGER', default_value=1)
        self.add_column_if_not_exists('config', 'target_directory', 'TEXT')
        self.add_column_if_not_exists('config', 'update_mode', 'TEXT')
        self.add_column_if_not_exists('config', 'download_interval_range', 'TEXT', default_value='1-3')
        self.add_column_if_not_exists('user_config', 'size_threshold', 'INTEGER', default_value=100)
        self.add_column_if_not_exists('user_config', 'username', 'TEXT')
        self.add_column_if_not_exists('user_config', 'password', 'TEXT')
        self.add_column_if_not_exists('user_config', 'download_threads', 'INTEGER', default_value=1)

        self.insert_default_user_config()

    def add_column_if_not_exists(self, table_name, column_name, column_type, default_value=None):
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in self.cursor.fetchall()]
        if column_name not in columns:
            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            self.conn.commit()
            print(f"列 '{column_name}' 已添加到 '{table_name}' 表中。")
            if default_value is not None:
                self.cursor.execute(f"UPDATE {table_name} SET {column_name} = ?", (default_value,))
                self.conn.commit()

    def insert_default_user_config(self):
        self.cursor.execute("SELECT COUNT(*) FROM user_config")
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute(
                '''INSERT INTO user_config (video_formats, subtitle_formats, image_formats, metadata_formats, size_threshold, download_threads) 
                   VALUES (?, ?, ?, ?, ?, ?)''',
                ('mp4,mkv,avi,mov,flv,wmv,ts,m2ts', 'srt,ass,sub', 'jpg,png,bmp', 'nfo', 100, 1))
            self.conn.commit()

    # --- 新增日志存取功能 ---
    def insert_log(self, log_name, task_id, level, message):
        try:
            self.cursor.execute('''INSERT INTO logs (log_name, task_id, level, message) 
                                   VALUES (?, ?, ?, ?)''', (log_name, task_id, level, message))
            self.conn.commit()
            
            # 自动清理：限制 logs 表最大条数（例如只保留最近 10000 条），防止数据库无限膨胀
            self.cursor.execute('''DELETE FROM logs WHERE log_id NOT IN (
                                   SELECT log_id FROM logs ORDER BY log_id DESC LIMIT 10000)''')
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"日志写入失败: {e}")

    def get_logs(self, log_name=None, task_id=None, limit=2000):
        query = "SELECT * FROM logs WHERE 1=1"
        params = []
        if log_name:
            query += " AND log_name = ?"
            params.append(log_name)
        if task_id:
            query += " AND task_id = ?"
            params.append(task_id)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        return self.execute_query(query, tuple(params), fetch_all=True)
    # -----------------------

    def execute_query(self, query, params=None, fetch_all=False, fetch_one=False):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            if fetch_all:
                return self.cursor.fetchall()
            if fetch_one:
                return self.cursor.fetchone()
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"SQLite 错误: {e}")
            return None

    def get_all_configurations(self):
        self.cursor.execute("SELECT config_id, config_name FROM config")
        return self.cursor.fetchall()

    def get_webdav_config(self, config_id):
        self.cursor.execute('''
            SELECT config_name, url, username, password, rootpath, target_directory, download_enabled, update_mode,  download_interval_range
            FROM config
            WHERE config_id=? LIMIT 1
        ''', (config_id,))
        result = self.cursor.fetchone()
        if result:
            config_name, url, username, password, rootpath, target_directory, download_enabled, update_mode, download_interval_range = result
            parsed_url = urlparse(url)
            protocol = parsed_url.scheme
            host = parsed_url.hostname
            port = parsed_url.port if parsed_url.port else (80 if protocol == 'http' else 443)
            if download_enabled is None:
                download_enabled = 1
            if download_interval_range:
                min_interval, max_interval = map(int, download_interval_range.replace(',', '-').split('-'))
            else:
                min_interval, max_interval = 1, 3
            return {
                'config_name': config_name, 'host': host, 'port': int(port), 'username': username,
                'password': password, 'rootpath': rootpath, 'protocol': protocol,
                'target_directory': target_directory, 'download_enabled': download_enabled,
                'update_mode': update_mode, 'download_interval_range': (min_interval, max_interval)
            }
        return None

    def get_script_config(self):
        self.cursor.execute("SELECT video_formats, subtitle_formats, image_formats, metadata_formats, size_threshold, download_threads FROM user_config LIMIT 1")
        result = self.cursor.fetchone()
        if result is None:
            self.insert_default_user_config()
            result = ('mp4,mkv,avi', 'srt,ass,sub', 'jpg,png', 'nfo', 100, 1)
        video_formats, subtitle_formats, image_formats, metadata_formats, size_threshold, download_threads = result
        self.cursor.execute("SELECT download_enabled FROM config LIMIT 1")
        download_enabled = self.cursor.fetchone()
        if download_enabled is None:
            download_enabled = (1,)
        if download_threads is None:
            download_threads = 1
        return {
            'video_formats': video_formats.split(','),
            'subtitle_formats': subtitle_formats.split(','),
            'image_formats': image_formats.split(','),
            'metadata_formats': metadata_formats.split(','),
            'size_threshold': size_threshold,
            'download_enabled': bool(download_enabled[0]),
            'download_threads': download_threads
        }

    def get_user_credentials(self):
        self.cursor.execute('SELECT username, password FROM user_config LIMIT 1')
        result = self.cursor.fetchone()
        return (result[0], result[1]) if result else (None, None)

    def set_user_credentials(self, username, password_hash):
        self.cursor.execute('SELECT COUNT(*) FROM user_config')
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute('INSERT INTO user_config (username, password) VALUES (?, ?)', (username, password_hash))
        else:
            self.cursor.execute('UPDATE user_config SET username = ?, password = ?', (username, password_hash))
        self.conn.commit()

    def close(self):
        self.conn.close()