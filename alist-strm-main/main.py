import random
import sys
import easywebdav
import json
import os
from urllib.parse import unquote
import requests
import time
from queue import Queue
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from db_handler import DBHandler
from logger import setup_logger

# 全局计数与锁
strm_file_counter = 0  
video_file_counter = 0  
download_file_counter = 0  
total_download_file_counter = 0  
directory_strm_file_counter = {}  
existing_strm_file_counter = 0  
download_queue = Queue()  
strm_tasks = [] 
counter_lock = threading.Lock()

# 线程局部存储，确保每个线程拥有独立的 WebDAV 客户端连接
thread_local = threading.local()

def connect_webdav(config):
    return easywebdav.connect(
        host=config['host'], port=config['port'], username=config['username'],
        password=config['password'], protocol=config['protocol']
    )

def get_webdav_client(config):
    if not hasattr(thread_local, 'client'):
        thread_local.client = connect_webdav(config)
    return thread_local.client

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
    logger.info("本地目录树已快速加载完毕。")
    return local_tree

# 供线程池调用的单目录拉取任务
def fetch_dir_task(directory, config, min_interval, max_interval):
    try:
        client = get_webdav_client(config)
        if max_interval > 0:
            time.sleep(random.uniform(min_interval, max_interval))
        return directory, client.ls(directory)
    except Exception as e:
        return directory, e

# 核心优化：多线程并发扫描目录，极大提升对比速度
def scan_directories_concurrently(config, script_config, size_threshold, download_enabled, logger, local_tree, update_mode, min_interval, max_interval):
    global video_file_counter, existing_strm_file_counter, total_download_file_counter, strm_tasks, directory_strm_file_counter
    
    root_dir = config['rootpath']
    logger.info(f"开启多线程扫描云端目录并比对: {unquote(root_dir)}")
    
    max_workers = script_config.get('download_threads', 4)
    # 扫描操作属于 I/O 密集型，可以分配更多线程（线程数翻倍）来提升网络并发请求速度
    scan_workers = max(4, max_workers * 2) 
    
    futures = set()
    visited = set()
    
    with ThreadPoolExecutor(max_workers=scan_workers) as executor:
        visited.add(root_dir)
        futures.add(executor.submit(fetch_dir_task, root_dir, config, min_interval, max_interval))
        
        while futures:
            # 等待任意一个扫描任务完成
            done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                current_dir, result = future.result()
                
                if isinstance(result, Exception):
                    logger.error(f"读取云端目录失败 {current_dir}: {result}")
                    continue
                    
                files = result
                decoded_directory = unquote(current_dir)
                local_relative_path = decoded_directory.replace(config['rootpath'], '').lstrip('/')
                local_directory = os.path.join(config['target_directory'], local_relative_path)
                os.makedirs(local_directory, exist_ok=True)
                
                with counter_lock:
                    if decoded_directory not in directory_strm_file_counter:
                        directory_strm_file_counter[decoded_directory] = 0

                for f in files:
                    is_directory = f.name.endswith('/')
                    
                    if is_directory:
                        # 避免包含自身造成死循环
                        if f.name != current_dir and f.name not in visited:
                            visited.add(f.name)
                            futures.add(executor.submit(fetch_dir_task, f.name, config, min_interval, max_interval))
                    else:
                        file_extension = os.path.splitext(f.name)[1].lower().lstrip('.')
                        
                        if file_extension in script_config['video_formats']:
                            with counter_lock:
                                video_file_counter += 1
                            
                            decoded_file_name = unquote(f.name).replace('/dav/', '')
                            strm_file_name = os.path.splitext(os.path.basename(decoded_file_name))[0] + ".strm"
                            relative_dir = os.path.relpath(local_directory, config['target_directory'])
                            
                            file_exists_locally = (relative_dir in local_tree and strm_file_name in local_tree[relative_dir])
                            
                            # 增量模式逻辑比对
                            if update_mode == 'incremental' and file_exists_locally:
                                with counter_lock:
                                    existing_strm_file_counter += 1
                            else:
                                with counter_lock:
                                    # 记录缺失的、需要生成的文件
                                    strm_tasks.append((f.name, f.size, local_directory, decoded_directory))
                                    
                        elif download_enabled and (file_extension in script_config['subtitle_formats'] or file_extension in script_config['image_formats'] or file_extension in script_config['metadata_formats']):
                            relative_dir = os.path.relpath(local_directory, config['target_directory'])
                            if relative_dir in local_tree and os.path.basename(unquote(f.name)) in local_tree[relative_dir]:
                                continue
                            with counter_lock:
                                total_download_file_counter += 1
                            download_queue.put((f.name, local_directory, f.size, config))

def create_strm_file(file_name, file_size, config, local_directory, directory, size_threshold, logger):
    global strm_file_counter, directory_strm_file_counter
    if file_size < size_threshold * (1024 * 1024):
        return

    clean_file_name = file_name.replace('/dav', '')
    http_link = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_file_name}"

    decoded_file_name = unquote(file_name).replace('/dav/', '')
    strm_file_name = os.path.splitext(os.path.basename(decoded_file_name))[0] + ".strm"
    strm_file_path = os.path.join(local_directory, strm_file_name)

    try:
        with open(strm_file_path, 'w', encoding='utf-8') as strm_file:
            strm_file.write(http_link)
        os.chmod(strm_file_path, 0o777)
        with counter_lock:
            strm_file_counter += 1
            directory_strm_file_counter[directory] += 1
    except Exception as e:
        logger.info(f"生成STRM文件出错: {e}")

def process_with_cache(webdav, config, script_config, config_id, size_threshold, logger, min_interval, max_interval):
    global strm_tasks, video_file_counter, strm_file_counter, existing_strm_file_counter
    strm_tasks = []
    existing_strm_file_counter = 0
    update_mode = config.get('update_mode', 'incremental')
    download_enabled = config.get('download_enabled', 1)

    logger.info(f"--- 开始同步 --- 模式: {'增量更新' if update_mode == 'incremental' else '全量覆盖'} ---")

    # 1. 秒级加载本地文件树
    local_tree = build_local_directory_tree(config['target_directory'], script_config, logger)
    
    # 2. 多线程全速扫描云端，并同步进行比对
    scan_directories_concurrently(config, script_config, size_threshold, download_enabled, logger, local_tree, update_mode, min_interval, max_interval)

    total_tasks = len(strm_tasks)
    
    # 3. 判断是否需要执行生成：无需生成时直接结束
    if total_tasks == 0:
        logger.info(f"增量比对完成：共扫描到 {video_file_counter} 个视频文件，本地已存在 {existing_strm_file_counter} 个。未发现新文件，无需生成，任务结束。")
        return

    logger.info(f"扫描完毕，跳过 {existing_strm_file_counter} 个已存在文件，发现 {total_tasks} 个缺少的新文件。开始多线程生成 STRM...")

    # 4. 对比发现缺失文件，开启多线程补齐生成
    max_workers = script_config.get('download_threads', 4)
    completed_tasks = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(create_strm_file, task[0], task[1], config, task[2], task[3], size_threshold, logger) for task in strm_tasks]
        
        for future in as_completed(futures):
            completed_tasks += 1
            if completed_tasks % 50 == 0 or completed_tasks == total_tasks:
                percent = (completed_tasks / total_tasks) * 100
                logger.info(f"STRM 生成进度: {completed_tasks} / {total_tasks} [{percent:.1f}%]")
                
            try:
                future.result()
            except Exception as e:
                logger.error(f"线程执行异常: {e}")

    logger.info(f"同步结束！本次新增生成了 {strm_file_counter} 个 .strm 文件。")

def download_task(item, min_interval, max_interval, logger):
    global download_file_counter
    file_name, local_path, expected_size, config = item
    try:
        download_file(file_name, local_path, expected_size, config, logger)
    finally:
        with counter_lock:
            download_file_counter += 1
            if download_file_counter % 10 == 0 or download_file_counter == total_download_file_counter:
                logger.info(f"附属文件下载进度: {download_file_counter}/{total_download_file_counter}")
    if max_interval > 0:
        time.sleep(random.uniform(min_interval, max_interval))

def download_files_with_interval(min_interval, max_interval, logger, max_workers=1):
    items = []
    while not download_queue.empty():
        items.append(download_queue.get())
        download_queue.task_done()
    if not items: return
    
    logger.info(f"开始多线程下载附属文件，共 {len(items)} 个任务...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for item in items:
            executor.submit(download_task, item, min_interval, max_interval, logger)

def download_file(file_name, local_path, expected_size, config, logger):
    if config.get('download_enabled', 1) == 0: return
    try:
        local_file_path = os.path.join(local_path, os.path.basename(unquote(file_name)))
        if os.path.exists(local_file_path): return
        clean_file_name = file_name.replace('/dav', '')
        file_url = f"{config['protocol']}://{config['host']}:{config['port']}/d{clean_file_name}"
        response = requests.get(file_url, auth=(config['username'], config['password']), stream=True, allow_redirects=True)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
            os.chmod(local_file_path, 0o777)
        else:
            logger.info(f"下载失败: {file_name}")
    except Exception as e:
        logger.info(f"下载异常: {e}")

def main(config_id, task_id=None, **kwargs):
    global strm_file_counter, video_file_counter, download_file_counter, total_download_file_counter
    strm_file_counter = 0; video_file_counter = 0; download_file_counter = 0; total_download_file_counter = 0
    
    db_handler = DBHandler()
    logger, _ = setup_logger(f'config_{config_id}', task_id=task_id)

    try:
        config = db_handler.get_webdav_config(config_id)
        if not config: return
        script_config = db_handler.get_script_config()
        webdav = connect_webdav(config)
        
        min_interval, max_interval = config.get('download_interval_range', (1, 3))
        process_with_cache(webdav, config, script_config, config_id, script_config['size_threshold'], logger, min_interval, max_interval)
        
        if config.get('download_enabled', 1) == 1:
            download_files_with_interval(min_interval, max_interval, logger, max_workers=script_config.get('download_threads', 1))

    except Exception as e:
        logger.error(f"未捕获异常: {e}")
    finally:
        db_handler.close()

if __name__ == '__main__':
    config_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    task_id = sys.argv[2] if len(sys.argv) > 2 else None
    main(config_id, task_id)