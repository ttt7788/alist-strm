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
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_file = os.path.join(cache_dir, f'webdav_directory_cache_{config_id}.json')
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.info(f"加载缓存文件出错: {e}")
    return None

def save_tree_to_cache(file_tree, config_id, logger):
    cache_dir = 'cache'
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_file = os.path.join(cache_dir, f'webdav_directory_cache_{config_id}.json')
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(file_tree, f, ensure_ascii=False, indent=4)
        logger.info(f"目录树缓存文件 '{cache_file}' 保存成功。")
    except Exception as e:
        logger.info(f"保存目录树缓存文件出错: {e}")

def build_local_directory_tree(local_root, script_config, logger):
    local_tree = {}
    for root, dirs, files in os.walk(local_root):
        relative_root = os.path.relpath(root, local_root)
        local_tree[relative_root] = set()
        for file in files:
            file_extension = os.path.splitext(file)[1].lower().lstrip('.')
            if file.lower().endswith('.strm') or \
               file_extension in script_config['subtitle_formats'] or \
               file_extension in script_config['image_formats'] or \
               file_extension in script_config['metadata_formats']:
                local_tree[relative_root].add(file)
    return local_tree

def list_files_incremental(webdav, directory, config, script_config, size_threshold, download_enabled, logger, local_tree, cached_subtree, min_interval, max_interval, visited=None):
    """
    核心增量逻辑：递归比对 WebDAV 节点与缓存子树
    """
    global video_file_counter, strm_file_counter, directory_strm_file_counter, total_download_file_counter
    decoded_directory = unquote(directory)
    interval = random.randint(min_interval, max_interval)

    if visited is None: visited = set()
    if directory in visited: return []
    visited.add(directory)

    try:
        logger.info(f"增量检查目录: {decoded_directory}")
        files = webdav.ls(directory)
        current_file_tree = []
        
        # 将缓存子树转为字典以便快速查找
        cache_lookup = {item['name']: item for item in (cached_subtree or [])}

        local_relative_path = decoded_directory.replace(config['rootpath'], '').lstrip('/')
        local_directory = os.path.join(config['target_directory'], local_relative_path)
        os.makedirs(local_directory, exist_ok=True)
        os.chmod(local_directory, 0o777)

        directory_strm_file_counter[decoded_directory] = 0

        for f in files:
            decoded_file_name = unquote(f.name)
            is_directory = f.name.endswith('/')
            
            file_info = {
                'name': decoded_file_name,
                'size': f.size,
                'modified': f.mtime,
                'is_directory': is_directory,
                'children': [] if is_directory else None
            }

            cached_item = cache_lookup.get(decoded_file_name)
            # 变动判断标准：缓存不存在 OR 修改时间变化 OR 大小变化
            needs_update = not cached_item or \
                           cached_item.get('modified') != f.mtime or \
                           cached_item.get('size') != f.size

            if is_directory:
                sub_cache = cached_item.get('children') if cached_item else None
                file_info['children'] = list_files_incremental(
                    webdav, f.name, config, script_config, size_threshold, 
                    download_enabled, logger, local_tree, sub_cache, 
                    min_interval, max_interval, visited
                )
                time.sleep(interval)
            else:
                file_extension = os.path.splitext(f.name)[1].lower().lstrip('.')
                time.sleep(interval)
                
                if file_extension in script_config['video_formats']:
                    video_file_counter += 1
                    # 检查本地文件是否真正丢失
                    strm_name = os.path.splitext(os.path.basename(decoded_file_name))[0] + ".strm"
                    relative_dir = os.path.relpath(local_directory, config['target_directory'])
                    strm_exists = relative_dir in local_tree and strm_name in local_tree[relative_dir]

                    if needs_update or not strm_exists:
                        create_strm_file(f.name, f.size, config, script_config['video_formats'], local_directory,
                                         decoded_directory, size_threshold, logger, local_tree)
                
                elif download_enabled and (
                        file_extension in script_config['subtitle_formats'] or
                        file_extension in script_config['image_formats'] or
                        file_extension in script_config['metadata_formats']):
                    
                    if needs_update:
                        logger.info(f"任务更新: {decoded_file_name}")
                        total_download_file_counter += 1
                        download_queue.put((webdav, f.name, local_directory, f.size, config))

            current_file_tree.append(file_info)
        return current_file_tree
    except Exception as e:
        logger.error(f"递归处理错误: {e}")
        return []

def download_task(item, min_interval, max_interval, logger):
    global download_file_counter
    webdav, file_name, local_path, expected_size, config = item
    try:
        download_file(webdav, file_name, local_path, expected_size, config, logger)
    finally:
        with counter_lock:
            download_file_counter += 1
            logger.info(f"文件下载进度: {download_file_counter}/{total_download_file_counter}")
    time.sleep(random.randint(min_interval, max_interval))

def download_files_with_interval(min_interval, max_interval, logger, max_workers=1):
    items = []
    while not download_queue.empty():
        items.append(download_queue.get())
        download_queue.task_done()
    if not items: return
    logger.info(f"开始多线程下载，线程数: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for item in items:
            executor.submit(download_task, item, min_interval, max_interval, logger)

def create_strm_file(file_name, file_size, config, video_formats, local_directory, directory, size_threshold, logger, local_tree):
    global strm_file_counter, directory_strm_file_counter
    size_threshold_bytes = size_threshold * (1024 * 1024)
    file_extension = os.path.splitext(file_name)[1].lower().lstrip('.')
    if file_extension not in video_formats: return
    if file_size < size_threshold_bytes: return

    clean_file_name = file_name.replace('/dav', '')
    http_link = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_file_name}"
    decoded_file_name = unquote(file_name).replace('/dav/', '')
    strm_file_name = os.path.splitext(os.path.basename(decoded_file_name))[0] + ".strm"
    strm_file_path = os.path.join(local_directory, strm_file_name)

    try:
        with open(strm_file_path, 'w', encoding='utf-8') as strm_file:
            strm_file.write(http_link)
        os.chmod(strm_file_path, 0o777)
        strm_file_counter += 1
        directory_strm_file_counter[directory] += 1
        logger.info(f"已更新 .strm: {unquote(strm_file_name)}")
    except Exception as e:
        logger.info(f"创建 .strm 出错: {e}")

def download_file(webdav, file_name, local_path, expected_size, config, logger):
    try:
        local_file_path = os.path.join(local_path, os.path.basename(unquote(file_name)))
        clean_file_name = file_name.replace('/dav', '')
        file_url = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_file_name}"
        response = requests.get(file_url, auth=(config['username'], config['password']), stream=True)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
            os.chmod(local_file_path, 0o777)
    except Exception as e:
        logger.info(f"下载出错: {e}")

def get_jwt_token(url, username, password, logger):
    api_url = f"{url}/api/auth/login"
    try:
        response = requests.post(api_url, json={"username": username, "password": password})
        if response.status_code == 200: return response.json()['data']['token']
    except Exception as e:
        logger.error(f"JWT Token 失败: {e}")
    return None

def refresh_webdav_directory(url, token, path, logger):
    refresh_url = f"{url}/api/fs/list"
    try:
        requests.post(refresh_url, headers={"Authorization": f"Bearer {token}"}, json={"path": path, "refresh": True})
        logger.info(f"强制刷新成功: {path}")
    except Exception: pass

def process_with_cache(webdav, config, script_config, config_id, size_threshold, logger, min_interval, max_interval):
    global video_file_counter, strm_file_counter, download_file_counter, total_download_file_counter
    download_enabled = config.get('download_enabled', 1)
    cached_tree = load_cached_tree(config_id, logger)
    root_directory = config['rootpath']

    # 预刷新 AList
    url = f"{config['protocol']}://{config['host']}:{config['port']}"
    token = get_jwt_token(url, config['username'], config['password'], logger)
    if token: refresh_webdav_directory(url, token, root_directory, logger)

    local_tree = build_local_directory_tree(config['target_directory'], script_config, logger)

    # 深度增量模式
    if config.get('update_mode') == 'incremental' and cached_tree:
        logger.info("正在进入深度增量比对模式...")
        current_tree = list_files_incremental(
            webdav, root_directory, config, script_config, size_threshold, 
            download_enabled, logger, local_tree, cached_tree, 
            min_interval, max_interval
        )
    else:
        logger.info("执行全量扫描...")
        # 此处使用旧函数或直接重用 incremental 函数不传缓存
        current_tree = list_files_incremental(
            webdav, root_directory, config, script_config, size_threshold, 
            download_enabled, logger, local_tree, None, 
            min_interval, max_interval
        )

    save_tree_to_cache(current_tree, config_id, logger)
    
    if download_enabled:
        download_threads = script_config.get('download_threads', 1)
        download_files_with_interval(min_interval, max_interval, logger, max_workers=download_threads)

    logger.info(f"同步总结: 创建/更新 {strm_file_counter} 个strm，下载 {download_file_counter} 个文件。")

def main(config_id, task_id=None, **kwargs):
    global strm_file_counter, video_file_counter, download_file_counter, total_download_file_counter
    global directory_strm_file_counter, existing_strm_file_counter, download_queue, found_video_files
    strm_file_counter = video_file_counter = download_file_counter = total_download_file_counter = 0
    directory_strm_file_counter = {}
    download_queue = Queue()

    db_handler = DBHandler()
    logger, _ = setup_logger('config_' + str(config_id), task_id=task_id) if task_id else setup_logger('config_' + str(config_id))

    try:
        config = db_handler.get_webdav_config(config_id)
        if not config: return
        script_config = db_handler.get_script_config()
        webdav = connect_webdav(config)
        
        interval_range = config.get('download_interval_range', (1, 3))
        min_i, max_i = interval_range
        
        process_with_cache(webdav, config, script_config, config_id, script_config['size_threshold'], logger, min_i, max_i)
        logger.info("任务执行完毕。")
    except Exception as e:
        logger.error(f"主程序异常: {e}")
    finally:
        db_handler.close()

if __name__ == '__main__':
    config_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    task_id = sys.argv[2] if len(sys.argv) > 2 else None
    main(config_id, task_id)
