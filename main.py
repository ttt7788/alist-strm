import random
import sys
import easywebdav
import json
import os
from urllib.parse import unquote
import requests
import time
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import threading
from db_handler import DBHandler
from logger import setup_logger

# 初始化全局计数器
strm_file_counter = 0
video_file_counter = 0
download_file_counter = 0
total_download_file_counter = 0
directory_strm_file_counter = {}
existing_strm_file_counter = 0
download_queue = Queue()
found_video_files = set()
counter_lock = threading.Lock()

def connect_webdav(config):
    return easywebdav.connect(
        host=config['host'],
        port=config['port'],
        username=config['username'],
        password=config['password'],
        protocol=config['protocol']
    )

def load_cached_tree(config_id, logger):
    cache_dir = 'cache'
    cache_file = os.path.join(cache_dir, f'webdav_directory_cache_{config_id}.json')
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 优化：将列表转为路径映射字典，方便 O(1) 查找
                return {item['name']: item for item in data}
        except Exception as e:
            logger.info(f"加载缓存文件出错: {e}")
    return {}

def save_tree_to_cache(file_tree_list, config_id, logger):
    cache_dir = 'cache'
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_file = os.path.join(cache_dir, f'webdav_directory_cache_{config_id}.json')
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(file_tree_list, f, ensure_ascii=False, indent=4)
        logger.info(f"目录树缓存已更新: {cache_file}")
    except Exception as e:
        logger.info(f"保存缓存出错: {e}")

def build_local_directory_tree(local_root, script_config, logger):
    local_tree = {}
    for root, dirs, files in os.walk(local_root):
        relative_root = os.path.relpath(root, local_root)
        if relative_root == ".": relative_root = ""
        local_tree[relative_root] = set(files)
    logger.info("本地目录树预载完成。")
    return local_tree

def list_files_recursive_incremental(webdav, directory, config, script_config, logger, local_tree, cached_tree_dict, interval_range, visited=None):
    global video_file_counter, strm_file_counter, total_download_file_counter, existing_strm_file_counter
    decoded_directory = unquote(directory)
    min_int, max_int = interval_range
    
    if visited is None: visited = set()
    if directory in visited: return []
    visited.add(directory)

    try:
        logger.info(f"扫描目录: {decoded_directory}")
        files = webdav.ls(directory)
        current_level_tree = []

        local_relative_path = decoded_directory.replace(config['rootpath'], '').lstrip('/')
        local_directory = os.path.join(config['target_directory'], local_relative_path)
        os.makedirs(local_directory, exist_ok=True)
        
        # 获取该目录已存在的本地文件集合
        existing_files = local_tree.get(local_relative_path, set())

        for f in files:
            # 基础信息
            decoded_name = unquote(f.name)
            is_dir = f.name.endswith('/')
            file_info = {
                'name': decoded_name,
                'size': f.size,
                'modified': f.mtime,
                'is_directory': is_dir
            }

            if is_dir:
                # 递归子目录
                time.sleep(random.randint(min_int, max_int))
                children = list_files_recursive_incremental(webdav, f.name, config, script_config, logger, local_tree, cached_tree_dict, interval_range, visited)
                file_info['children'] = children
            else:
                ext = os.path.splitext(f.name)[1].lower().lstrip('.')
                strm_name = os.path.splitext(os.path.basename(decoded_name))[0] + ".strm"
                
                # 核心增量逻辑：对比缓存和本地文件
                cached_item = cached_tree_dict.get(decoded_name)
                needs_update = False
                
                # 如果缓存不存在，或者文件大小/时间变了，或者本地strm丢了
                if not cached_item or cached_item['size'] != f.size or cached_item['modified'] != f.mtime:
                    needs_update = True
                
                if ext in script_config['video_formats']:
                    video_file_counter += 1
                    if strm_name not in existing_files or needs_update:
                        create_strm_file(f.name, f.size, config, script_config['video_formats'], local_directory, decoded_directory, script_config['size_threshold'], logger)
                    else:
                        existing_strm_file_counter += 1
                
                elif config.get('download_enabled') and (ext in script_config['subtitle_formats'] or ext in script_config['image_formats']):
                    if os.path.basename(decoded_name) not in existing_files or needs_update:
                        total_download_file_counter += 1
                        download_queue.put((webdav, f.name, local_directory, f.size, config))

            current_level_tree.append(file_info)
        return current_level_tree
    except Exception as e:
        logger.error(f"遍历出错 {decoded_directory}: {e}")
        return []

def create_strm_file(file_name, file_size, config, video_formats, local_directory, directory, size_threshold, logger):
    global strm_file_counter
    if file_size < (size_threshold * 1024 * 1024):
        return

    clean_name = file_name.replace('/dav', '')
    http_link = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_name}"
    
    decoded_base = unquote(os.path.basename(file_name))
    strm_file_name = os.path.splitext(decoded_base)[0] + ".strm"
    strm_path = os.path.join(local_directory, strm_file_name)

    try:
        with open(strm_path, 'w', encoding='utf-8') as f:
            f.write(http_link)
        os.chmod(strm_path, 0o777)
        strm_file_counter += 1
        logger.info(f"已生成/更新: {strm_file_name}")
    except Exception as e:
        logger.error(f"创建STRM失败: {e}")

def download_task(item, interval_range, logger):
    global download_file_counter
    webdav, file_name, local_path, expected_size, config = item
    try:
        local_file_path = os.path.join(local_path, os.path.basename(unquote(file_name)))
        clean_name = file_name.replace('/dav', '')
        file_url = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_name}"
        
        response = requests.get(file_url, auth=(config['username'], config['password']), stream=True)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
            os.chmod(local_file_path, 0o777)
            with counter_lock: download_file_counter += 1
    except Exception as e:
        logger.error(f"下载失败 {file_name}: {e}")
    time.sleep(random.randint(*interval_range))

def main(config_id, task_id=None):
    global strm_file_counter, video_file_counter, download_file_counter, total_download_file_counter, existing_strm_file_counter
    db_handler = DBHandler()
    logger, _ = setup_logger(f'config_{config_id}', task_id=task_id)

    try:
        config = db_handler.get_webdav_config(config_id)
        script_config = db_handler.get_script_config()
        if not config: return

        webdav = connect_webdav(config)
        cached_tree_dict = load_cached_tree(config_id, logger)
        local_tree = build_local_directory_tree(config['target_directory'], script_config, logger)
        
        interval_range = config.get('download_interval_range', (1, 3))
        
        logger.info(f"开始增量扫描: {config['rootpath']}")
        new_tree_list = list_files_recursive_incremental(
            webdav, config['rootpath'], config, script_config, logger, 
            local_tree, cached_tree_dict, interval_range
        )
        
        save_tree_to_cache(new_tree_list, config_id, logger)
        
        logger.info(f"处理完成: 新增 {strm_file_counter} 个, 跳过 {existing_strm_file_counter} 个已存在视频")
        
        if not download_queue.empty():
            logger.info(f"开始下载任务: {total_download_file_counter} 个文件")
            with ThreadPoolExecutor(max_workers=script_config.get('download_threads', 1)) as executor:
                while not download_queue.empty():
                    executor.submit(download_task, download_queue.get(), interval_range, logger)

    except Exception as e:
        logger.error(f"运行异常: {e}")
    finally:
        db_handler.close()

if __name__ == '__main__':
    cid = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    tid = sys.argv[2] if len(sys.argv) > 2 else None
    main(cid, tid)
