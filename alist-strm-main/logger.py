import logging
import os
import glob
from datetime import datetime
from db_handler import DBHandler

class DBLogHandler(logging.Handler):
    def __init__(self, log_name, task_id=None):
        super().__init__()
        self.log_name = log_name
        self.task_id = str(task_id) if task_id else ''
        self.db_handler = DBHandler()

    def emit(self, record):
        try:
            msg = self.format(record)
            # 将日志写入数据库
            self.db_handler.insert_log(self.log_name, self.task_id, record.levelname, msg)
        except Exception:
            self.handleError(record)

def setup_logger(log_name, task_id=None):
    # 保留 log 目录创建以防其他地方存在硬编码依赖
    log_dir = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    # 避免重复添加处理器
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')

        # 创建控制台处理器 (用于在终端直观查看)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 创建数据库日志处理器 (替代原本的 FileHandler)
        db_handler = DBLogHandler(log_name, task_id)
        db_handler.setFormatter(formatter)
        logger.addHandler(db_handler)

    # 返回格式保持与旧版兼容，但 log_file 参数不再有实际文件路径意义
    return logger, None