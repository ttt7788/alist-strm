import os
import re
import sys
import random
import glob
import json
import subprocess
import zipfile
import requests
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, session, g, abort, jsonify
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from db_handler import DBHandler
from logger import setup_logger
from scheduler import scheduler_manager, convert_to_cron_time

# 确保配置目录存在
config_dir = '/config'
if not os.path.exists(config_dir):
    try:
        os.makedirs(config_dir)
    except Exception as e:
        print(f"Failed to create config directory: {e}")

# 初始化调度器
try:
    scheduler_manager.init_scheduler()
except Exception as e:
    print(f"Failed to initialize scheduler: {e}")
    pass

app = Flask(__name__)
app.secret_key = 'www.tefuir0829.cn'

# 初始化日志
logger, _ = setup_logger('app')

# 定义图片文件夹路径
IMAGE_FOLDER = 'static/images'
db_handler = DBHandler()
local_version = "6.0.7"
ENV_FILE = os.path.join(config_dir, 'app.env')


# ================== 拦截器与鉴权 ==================
@app.before_request
def check_user_config():
    if request.endpoint in ['login', 'register', 'static', 'random_image', 'forgot_password']:
        return
    username, password = db_handler.get_user_credentials()
    if not username or not password:
        return redirect(url_for('register'))
    if 'logged_in' not in session:
        return redirect(url_for('login'))

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('请先登录', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def before_request():
    g.local_version = local_version


# ================== 登录与注册 ==================
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = generate_password_hash(password)
        db_handler.set_user_credentials(username, password_hash)
        flash('注册成功，请登录', 'success')
        return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        stored_username, stored_password_hash = db_handler.get_user_credentials()
        if username == stored_username and check_password_hash(stored_password_hash, password):
            session['logged_in'] = True
            session['username'] = username
            flash('登录成功', 'success')
            return redirect(url_for('index'))
        else:
            flash('用户名或密码错误', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('您已退出登录', 'success')
    return redirect(url_for('login'))

@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        security_code = request.form['security_code']
        new_password = request.form['new_password']
        confirm_password = request.form.get('confirm_password', new_password)
        
        stored_security_code = os.getenv('SECURITY_CODE', 'alist-strm')
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, 'r') as f:
                for line in f:
                    if line.startswith('SECURITY_CODE='):
                        stored_security_code = line.split('=')[1].strip()

        if stored_security_code != security_code:
            flash('安全码不正确', 'error')
            return redirect(url_for('forgot_password'))
        if new_password != confirm_password:
            flash('两次输入的密码不一致', 'error')
            return redirect(url_for('forgot_password'))

        stored_username, _ = db_handler.get_user_credentials()
        new_password_hash = generate_password_hash(new_password)
        db_handler.set_user_credentials(username=stored_username, password_hash=new_password_hash)
        flash('密码重置成功，请登录', 'success')
        return redirect(url_for('login'))
    return render_template('forgot_password.html')


# ================== 首页 ==================
@app.route('/')
@login_required
def index():
    invalid_file_trees = []
    invalid_tree_dir = 'invalid_file_trees'
    if os.path.exists(invalid_tree_dir):
        for json_file in os.listdir(invalid_tree_dir):
            if json_file.endswith('.json'):
                with open(os.path.join(invalid_tree_dir, json_file), 'r', encoding='utf-8') as file:
                    invalid_file_trees.append({
                        'name': json_file,
                        'structure': json.load(file)
                    })
    return render_template('home.html', invalid_file_trees=invalid_file_trees)

@app.route('/random_image')
def random_image():
    images = os.listdir(IMAGE_FOLDER)
    if not images:
        return "", 404
    random_image = random.choice(images)
    return send_from_directory(IMAGE_FOLDER, random_image)


# ================== 配置管理 ==================
def validate_download_interval_range(interval_range):
    pattern = re.compile(r'^(\d+)-(\d+)$')
    match = pattern.match(interval_range)
    if not match: return False
    min_val, max_val = int(match.group(1)), int(match.group(2))
    return min_val <= max_val

@app.route('/configs')
@login_required
def configs():
    try:
        db_handler.cursor.execute("SELECT config_id, config_name, url, username, rootpath, target_directory FROM config")
        configs = db_handler.cursor.fetchall()
        return render_template('configs.html', configs=configs)
    except Exception as e:
        flash(f"加载配置时出错: {e}", 'error')
        return render_template('configs.html', configs=[])

@app.route('/new', methods=['GET', 'POST'])
@login_required
def new_config():
    if request.method == 'POST':
        try:
            config_name = request.form['config_name']
            url = request.form['url']
            username = request.form['username']
            password = request.form['password']
            rootpath = request.form['rootpath']
            target_directory = request.form['target_directory']
            download_interval_range = request.form.get('download_interval_range', '1-3')
            download_enabled = int(request.form.get('download_enabled', 0))
            update_mode = request.form['update_mode']

            if not validate_download_interval_range(download_interval_range):
                flash("下载间隔范围无效。请使用 'min-max' 格式。", 'error')
                return redirect(url_for('new_config'))

            if not rootpath.startswith('/dav/'):
                rootpath = '/dav/' + rootpath.lstrip('/')

            db_handler.cursor.execute('''
                INSERT INTO config (config_name, url, username, password, rootpath, target_directory, download_interval_range, download_enabled, update_mode) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (config_name, url, username, password, rootpath, target_directory, download_interval_range, download_enabled, update_mode))
            db_handler.conn.commit()

            flash('新配置已成功添加！', 'success')
            return redirect(url_for('configs'))
        except Exception as e:
            flash(f"添加新配置时出错: {e}", 'error')
    return render_template('new_config.html')

@app.route('/edit/<int:config_id>', methods=['GET', 'POST'])
@login_required
def edit_config(config_id):
    try:
        if request.method == 'POST':
            config_name = request.form['config_name']
            url = request.form['url']
            username = request.form['username']
            password = request.form['password']
            rootpath = request.form['rootpath']
            target_directory = request.form['target_directory']
            download_interval_range = request.form.get('download_interval_range', '1-3')
            download_enabled = int(request.form.get('download_enabled', 0))
            update_mode = request.form['update_mode']

            if not validate_download_interval_range(download_interval_range):
                flash("下载间隔范围无效。请使用 'min-max' 格式。", 'error')
                return redirect(url_for('edit_config', config_id=config_id))

            if not rootpath.startswith('/dav/'):
                rootpath = '/dav/' + rootpath.lstrip('/')

            db_handler.cursor.execute('''
                UPDATE config 
                SET config_name=?, url=?, username=?, password=?, rootpath=?, target_directory=?, download_enabled=?, update_mode=?, download_interval_range=?
                WHERE config_id=?
            ''', (config_name, url, username, password, rootpath, target_directory, download_enabled, update_mode, download_interval_range, config_id))
            db_handler.conn.commit()

            flash('配置已成功更新！', 'success')
            return redirect(url_for('configs'))

        db_handler.cursor.execute('''
            SELECT config_name, url, username, password, rootpath, target_directory, download_enabled, update_mode, download_interval_range 
            FROM config WHERE config_id = ?
        ''', (config_id,))
        config = db_handler.cursor.fetchone()
        if config and config[8] is None:
            config = list(config)
            config[8] = '1-3'

        return render_template('edit_config.html', config=config)
    except Exception as e:
        flash(f"编辑配置时出错: {e}", 'error')
        return redirect(url_for('configs'))

@app.route('/copy_config/<int:config_id>')
@login_required
def copy_config(config_id):
    try:
        db_handler.cursor.execute('SELECT config_name, url, username, password, rootpath, target_directory, download_interval_range, download_enabled, update_mode FROM config WHERE config_id = ?', (config_id,))
        config = db_handler.cursor.fetchone()
        if not config:
            flash("未找到配置文件。", 'error')
            return redirect(url_for('configs'))

        new_name = config[0] + " - 复制"
        db_handler.cursor.execute('''
            INSERT INTO config (config_name, url, username, password, rootpath, target_directory, download_interval_range, download_enabled, update_mode) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (new_name, config[1], config[2], config[3], config[4], config[5], config[6], config[7], config[8]))
        db_handler.conn.commit()
        flash("配置已成功复制！", 'success')
    except Exception as e:
        flash(f"复制配置时出错: {e}", 'error')
    return redirect(url_for('configs'))

@app.route('/delete/<int:config_id>')
@login_required
def delete_config(config_id):
    try:
        db_handler.cursor.execute("DELETE FROM config WHERE config_id = ?", (config_id,))
        db_handler.conn.commit()
        flash('配置已成功删除！', 'success')
    except Exception as e:
        flash(f"删除配置时出错: {e}", 'error')
    return redirect(url_for('configs'))

def run_config(config_id):
    main_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main.py')
    if os.path.exists(main_script_path):
        command = f"{sys.executable} {main_script_path} {config_id}"
        logger.info(f"启动配置ID: {config_id} 的命令: {command}")
        subprocess.Popen(command, shell=True)
    else:
        logger.error(f"无法找到 main.py 文件: {main_script_path}")

@app.route('/run_selected_configs', methods=['POST'])
@login_required
def run_selected_configs():
    selected_configs = request.form.getlist('selected_configs')
    action = request.form.get('action')

    if not selected_configs:
        flash('请选择至少一个配置', 'error')
        return redirect(url_for('configs'))

    if action == 'copy_selected':
        for cid in selected_configs: copy_config(int(cid))
        flash('选定的配置已成功复制！', 'success')
    elif action == 'delete_selected':
        for cid in selected_configs:
            db_handler.cursor.execute('DELETE FROM config WHERE config_id = ?', (cid,))
        db_handler.conn.commit()
        flash('选定的配置已成功删除！', 'success')
    elif action == 'run_selected':
        for cid in selected_configs: run_config(int(cid))
        flash('选定的配置已开始运行！请查看日志', 'success')

    return redirect(url_for('configs'))


# ================== 脚本设置 ==================
@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    if request.method == 'POST':
        try:
            video_formats = request.form['video_formats']
            subtitle_formats = request.form['subtitle_formats']
            image_formats = request.form['image_formats']
            metadata_formats = request.form['metadata_formats']
            size_threshold = int(request.form['size_threshold'])
            download_threads = int(request.form.get('download_threads', 1))
            
            db_handler.cursor.execute('''
                UPDATE user_config 
                SET video_formats=?, subtitle_formats=?, image_formats=?, metadata_formats=?, size_threshold=?, download_threads=?
            ''', (video_formats, subtitle_formats, image_formats, metadata_formats, size_threshold, download_threads))
            db_handler.conn.commit()
            flash('设置已成功更新！', 'success')
        except Exception as e:
            flash(f"更新设置时出错: {e}", 'error')
        return redirect(url_for('settings'))

    script_config = db_handler.get_script_config()
    db_handler.cursor.execute('SELECT download_enabled FROM config LIMIT 1')
    result = db_handler.cursor.fetchone()
    script_config['download_enabled'] = bool(result[0] if result else 1)
    return render_template('settings.html', script_config=script_config)


# ================== 日志查看 ==================
def get_log_content_tail(config_id, max_lines=2000):
    try:
        db = DBHandler()
        log_name = f'config_{config_id}'
        logs = db.get_logs(log_name=log_name, limit=max_lines)
        if not logs:
            return "暂无日志或任务尚未执行。"
        log_lines = []
        for log in reversed(logs):
            log_lines.append(f"{log[5]} - [{log[1]}] - {log[3]} - {log[4]}")
        return '<br>'.join(log_lines)
    except Exception as e:
        logger.error(f"读取日志文件时出错: {e}")
        return None

@app.route('/logs/<int:config_id>')
@login_required
def logs(config_id):
    log_content = get_log_content_tail(config_id)
    if log_content is None:
         abort(404, description="没有找到相关的日志文件")
    return render_template('logs_single.html', log_content=log_content, config_id=config_id)

@app.route('/api/logs/<int:config_id>')
@login_required
def api_logs(config_id):
    log_content = get_log_content_tail(config_id)
    if log_content is None:
        return jsonify({'error': 'Log file not found'}), 404
    return jsonify({'content': log_content})


# ================== 定时任务 ==================
@app.route('/scheduled_tasks')
@login_required
def scheduled_tasks():
    tasks = scheduler_manager.list_tasks()
    return render_template('scheduled_tasks.html', tasks=tasks)

@app.route('/new_task', methods=['GET', 'POST'])
@login_required
def new_task():
    if request.method == 'POST':
        task_name = request.form['task_name']
        config_ids = request.form.getlist('config_ids')
        interval_type = request.form['interval_type']
        interval_value = request.form['interval_value']
        task_mode = request.form['task_mode']
        is_enabled = request.form['is_enabled'] == '1'

        try:
            val = int(interval_value)
            if interval_type == 'minute' and not (1 <= val <= 59): raise ValueError('分钟须在 1-59 之间')
            elif interval_type == 'hourly' and not (1 <= val <= 23): raise ValueError('小时须在 1-23 之间')
        except Exception as e:
            flash(str(e), 'error')
            return redirect(url_for('new_task'))

        cron_time = convert_to_cron_time(interval_type, interval_value)
        for config_id in config_ids:
            name_for_task = f"{task_name} (Config {config_id})" if len(config_ids) > 1 else task_name
            scheduler_manager.add_task(task_name=name_for_task, cron_expression=cron_time, config_id=config_id, task_mode=task_mode, is_enabled=is_enabled)
        
        flash('任务已成功添加！', 'success')
        return redirect(url_for('scheduled_tasks'))

    configs = db_handler.get_all_configurations()
    return render_template('new_task.html', configs=configs)

@app.route('/update_task/<task_id>', methods=['GET', 'POST'])
@login_required
def update_task(task_id):
    if request.method == 'POST':
        task_name = request.form['task_name']
        config_ids = request.form.getlist('config_ids')
        interval_type = request.form['interval_type']
        interval_value = request.form['interval_value']
        task_mode = request.form['task_mode']
        is_enabled = request.form['is_enabled'] == '1'
        
        cron_time = convert_to_cron_time(interval_type, interval_value)
        target_config_id = config_ids[0] if config_ids else None
        
        scheduler_manager.update_task(task_id=task_id, cron_expression=cron_time, config_id=target_config_id, task_mode=task_mode, task_name=task_name, is_enabled=is_enabled)
        flash('任务已成功更新！', 'success')
        return redirect(url_for('scheduled_tasks'))

    tasks = scheduler_manager.list_tasks()
    task = next((t for t in tasks if t.get('task_id') == task_id), None)
    if not task: return redirect(url_for('scheduled_tasks'))
    configs = db_handler.get_all_configurations()
    return render_template('edit_task.html', task=task, configs=configs, selected_config_ids=[str(task.get('config_id'))])

@app.route('/delete_task/<task_id>', methods=['POST'])
@login_required
def delete_task(task_id):
    scheduler_manager.delete_task(task_id)
    flash('任务已成功删除！', 'success')
    return redirect(url_for('scheduled_tasks'))

@app.route('/delete_selected_tasks', methods=['POST'])
@login_required
def delete_selected_tasks():
    try:
        task_ids = request.get_json().get('task_ids', [])
        for tid in task_ids: scheduler_manager.delete_task(tid)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/run_task_now/<task_id>', methods=['POST'])
@login_required
def run_task_now(task_id):
    scheduler_manager.run_task_now(task_id)
    flash('任务已触发立即运行！', 'success')
    return redirect(url_for('scheduled_tasks'))

@app.route('/stop_task/<task_id>', methods=['POST'])
@login_required
def stop_task_route(task_id):
    success, message = scheduler_manager.stop_task(task_id)
    if success: flash(message, 'success')
    else: flash(message, 'warning')
    return redirect(url_for('scheduled_tasks'))

@app.route('/view_logs/<task_id>')
@login_required
def view_logs(task_id):
    log_dir = os.path.join(os.getcwd(), 'logs')
    log_pattern = os.path.join(log_dir, f'task_{task_id}_*.log')
    log_files = glob.glob(log_pattern)
    log_contents = None
    if log_files:
        log_files.sort(key=os.path.getmtime, reverse=True)
        with open(log_files[0], 'r', encoding='utf-8') as f:
            content = f.read()
        log_contents = [{'filename': os.path.basename(log_files[0]), 'content': content}]
    return render_template('view_logs.html', log_contents=log_contents, task_id=task_id)


# ================== 库浏览器与失效清理 ==================
def get_target_directory_by_config_id(config_id):
    config = db_handler.get_webdav_config(config_id)
    return config['target_directory'] if config else None

@app.route('/strm_browser')
@login_required
def strm_browser():
    configs = db_handler.get_all_configurations()
    return render_template('strm_browser.html', configs=configs)

@app.route('/api/browse_strm')
@login_required
def browse_strm():
    config_id = request.args.get('config_id')
    relative_path = request.args.get('path', '')
    if not config_id: return jsonify({'error': '未提供配置 ID'}), 400
    try:
        target_dir = get_target_directory_by_config_id(int(config_id))
        if not target_dir: return jsonify({'error': '未找到目标目录'}), 404
        full_path = os.path.join(target_dir, relative_path)
        if not os.path.abspath(full_path).startswith(os.path.abspath(target_dir)): return jsonify({'error': '非法路径'}), 403
        if not os.path.exists(full_path): return jsonify({'error': '目录不存在'}), 404
        
        items = []
        with os.scandir(full_path) as entries:
            for entry in entries:
                if entry.is_dir(): items.append({'name': entry.name, 'type': 'directory'})
                elif entry.is_file() and entry.name.endswith('.strm'): items.append({'name': entry.name, 'type': 'file'})
        items.sort(key=lambda x: (x['type'] != 'directory', x['name']))
        return jsonify({'items': items})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/invalid_file_trees')
@login_required
def invalid_file_trees():
    invalid_file_trees = []
    invalid_tree_dir = 'invalid_file_trees'
    if os.path.exists(invalid_tree_dir):
        for json_file in os.listdir(invalid_tree_dir):
            if json_file.endswith('.json'):
                invalid_file_trees.append({'name': json_file})
    configs = db_handler.get_all_configurations()
    return render_template('invalid_file_trees.html', invalid_file_trees=invalid_file_trees, configs=configs)

@app.route('/start_manual_validation', methods=['POST'])
@login_required
def start_manual_validation():
    try:
        config_id = request.form.get('config_id')
        scan_mode = request.form.get('scan_mode', 'quick')
        if not config_id: return jsonify({"error": "未选择配置"}), 400
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'strm_validator.py')
        if os.path.exists(script_path):
            subprocess.Popen(f"{sys.executable} {script_path} {config_id} {scan_mode}", shell=True)
            flash('手动检测任务已启动！', 'success')
        else:
            flash('无法找到验证脚本', 'error')
    except Exception as e:
        flash(f"启动检测时出错: {e}", 'error')
    return redirect(url_for('invalid_file_trees'))

@app.route('/delete_invalid_directory/<path:json_filename>', methods=['POST'])
@login_required
def delete_invalid_directory(json_filename):
    # 保持原有删除逻辑不变
    return jsonify({"message": "暂未开启删除API"}), 200


# ================== 其他工具箱 (域名替换) ==================
def run_replace_domain_script(target_directory, old_domain, new_domain):
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'replace_domain.py')
    try:
        command = [sys.executable, script_path, target_directory, old_domain, new_domain]
        subprocess.Popen(command)
        logger.info(f"已启动域名批量替换脚本: {' '.join(command)}")
        return True
    except Exception as e:
        logger.error(f"运行替换脚本出错：{e}")
        return False

def get_script_log():
    log_file = os.path.join(os.getcwd(), 'logs', 'replace_domain.log')
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return ''.join(lines[-1000:])
    return '暂无域名替换运行日志。'

@app.route('/other', methods=['GET', 'POST'])
@login_required
def other():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'edit':
            session['script_params'] = {
                'target_directory': request.form.get('target_directory', ''),
                'old_domain': request.form.get('old_domain', ''),
                'new_domain': request.form.get('new_domain', '')
            }
            flash('域名替换参数已保存。', 'success')
            return redirect(url_for('other'))
        elif action == 'run':
            script_params = session.get('script_params')
            if not script_params or not script_params.get('target_directory'):
                flash('请先设置并保存参数。', 'error')
                return redirect(url_for('other'))
            result = run_replace_domain_script(
                script_params['target_directory'],
                script_params['old_domain'],
                script_params['new_domain']
            )
            if result:
                flash('域名替换脚本已在后台启动！请查看下方日志。', 'success')
            else:
                flash('脚本启动失败，请检查系统日志。', 'error')
            return redirect(url_for('other'))
            
    script_params = session.get('script_params', {})
    log_content = get_script_log()
    return render_template('other.html', script_params=script_params, log_content=log_content)
# ================== 实时任务进度 API ==================
@app.route('/api/task_progress')
@login_required
def api_task_progress():
    import time  # 确保引入 time 模块
    log_dir = os.path.join(os.getcwd(), 'logs')
    
    if not os.path.exists(log_dir):
        return jsonify({'status': 'idle'})
        
    log_files = glob.glob(os.path.join(log_dir, '*.log'))
    if not log_files:
        return jsonify({'status': 'idle'})
        
    # 获取最近修改的日志文件
    latest_file = max(log_files, key=os.path.getmtime)
    
    # 如果最新的日志文件在 5 分钟内没有被修改过，视为当前无任务运行
    if time.time() - os.path.getmtime(latest_file) > 300:
        return jsonify({'status': 'idle'})
        
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # 从最后 50 行中反向查找包含进度或状态的日志
            for line in reversed(lines[-50:]):
                if any(keyword in line for keyword in ['进度', '开始同步', '扫描完毕', '同步结束']):
                    # 简单截取掉前面的时间戳，只保留核心信息
                    parts = line.split(' - ')
                    msg = parts[-1].strip() if len(parts) > 1 else line.strip()
                    
                    # 如果匹配到结束语，说明任务刚刚跑完
                    if '同步结束' in msg or '无需生成' in msg:
                        return jsonify({'status': 'idle', 'message': msg})
                        
                    return jsonify({'status': 'running', 'message': msg})
                    
            # 有日志写入，但没匹配到进度关键字
            return jsonify({'status': 'running', 'message': '正在后台执行扫描或比对工作...'})
    except Exception:
        return jsonify({'status': 'idle'})
# ==============================================================

# ================== 系统信息与异常处理 ==================
@app.route('/about', methods=['GET'])
def about():
    return render_template('about.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

@app.errorhandler(400)
def bad_request_error(e):
    return render_template('400.html'), 400

def ensure_env_file():
    if not os.path.exists(config_dir): os.makedirs(config_dir)
    port = os.getenv('WEB_PORT', '5000')
    security_code = os.getenv('SECURITY_CODE', 'alist-strm')
    if not os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'w') as f:
            f.write(f"WEB_PORT={port}\nSECURITY_CODE={security_code}\n")

def load_port_from_env():
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                if line.startswith('WEB_PORT='): return int(line.split('=')[1].strip())
    return 5000

if __name__ == '__main__':
    ensure_env_file()
    port = load_port_from_env()
    app.run(host="0.0.0.0", port=port, debug=True)