from flask import Flask, request, render_template, redirect, url_for, session, jsonify
from datetime import datetime
import psycopg2
from psycopg2 import Error

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_12345!@#$%'

def log_message(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def get_db_connection():
    try:
        connection = psycopg2.connect(
            host="rc1b-31bfnlmu4csa6hn5.mdb.yandexcloud.net",
            port=6432,
            database="db1",
            user="user1",
            password="12345678",
            sslmode="verify-full",
            sslrootcert="/home/yc-user/.postgrsql/root.crt"
        )
        log_message("INFO: Подключение к базе данных установлено.")
        return connection
    except Error as e:
        log_message(f"ERROR: Ошибка при подключении к базе данных: {e}")
        return None

def init_database():
    """
    Инициализация базы данных - создание таблиц если они не существуют.
    """
    conn = get_db_connection()
    if conn is None:
        log_message("ERROR: Не удалось подключиться к БД для инициализации")
        return False
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Создаем таблицу production_plan
        create_table_query = """
        CREATE TABLE IF NOT EXISTS production_plan (
            id SERIAL PRIMARY KEY,
            part_name VARCHAR(255) NOT NULL,
            planned_quantity INTEGER NOT NULL,
            machine_number INTEGER NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL
        );
        """
        cursor.execute(create_table_query)
        
        # Создаем таблицу production для отчетов
        create_production_table_query = """
        CREATE TABLE IF NOT EXISTS production (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES production_plan(id),
            part_number VARCHAR(255),
            actual_quantity INTEGER NOT NULL,
            bubble_count INTEGER DEFAULT 0,
            underfill_count INTEGER DEFAULT 0,
            inclusion_count INTEGER DEFAULT 0,
            defect_count INTEGER DEFAULT 0,
            actual_start_time TIMESTAMP NOT NULL,
            actual_end_time TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(order_id)
        );
        """
        cursor.execute(create_production_table_query)
        
        # Создаем таблицу events для событий с русскими названиями столбцов
        create_events_table_query = """
        CREATE TABLE IF NOT EXISTS events (
            event_id SERIAL PRIMARY KEY,
            event_name VARCHAR(255) NOT NULL,
            batch INTEGER NOT NULL REFERENCES production_plan(id),
            "Фактические время начала события" TIMESTAMP NOT NULL,
            "Фактические время конца события" TIMESTAMP NOT NULL,
            "Time_group" VARCHAR(50) NOT NULL CHECK ("Time_group" IN ('Planned pause time', 'Utilization hours', 'Breakdown time'))
        );
        """
        cursor.execute(create_events_table_query)
        
        conn.commit()
        
        log_message("SUCCESS: Таблицы production_plan, production и events проверены/созданы успешно")
        return True
        
    except (Exception, Error) as error:
        log_message(f"ERROR: Ошибка при создании таблиц: {error}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_parts_list():
    """Получение списка деталей из таблицы parts"""
    conn = get_db_connection()
    if conn is None:
        return []
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        select_query = """
        SELECT "Наименование детали" 
        FROM parts 
        ORDER BY "Наименование детали";
        """
        
        cursor.execute(select_query)
        parts = cursor.fetchall()
        
        # Преобразуем список кортежей в список строк
        parts_list = [part[0] for part in parts]
        
        log_message(f"INFO: Получено {len(parts_list)} деталей из БД")
        return parts_list
        
    except (Exception, Error) as error:
        log_message(f"ERROR: Ошибка при получении списка деталей: {error}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def check_auth(username, password):
    """Проверка логина и пароля"""
    return username == 'admin' and password == '1234'

def login_required(f):
    """Декоратор для проверки авторизации"""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/', methods=['GET', 'POST'])
def login():
    """Страница входа в систему"""
    if 'logged_in' in session and session['logged_in']:
        return redirect(url_for('home'))
    
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if check_auth(username, password):
            session['logged_in'] = True
            session['username'] = username
            log_message(f"INFO: Пользователь {username} вошел в систему")
            return redirect(url_for('home'))
        else:
            error = 'Неверные данные! Используйте admin/1234'
            log_message(f"WARNING: Неудачная попытка входа с логином {username}")
    
    return render_template('login.html', error=error)

@app.route('/home')
@login_required
def home():
    """Главная страница после авторизации"""
    parts_list = get_parts_list()
    return render_template('home.html', username=session.get('username'), parts_list=parts_list)

@app.route('/edit/<int:id>')
@login_required
def edit_record(id):
    """Страница редактирования записи"""
    parts_list = get_parts_list()
    conn = get_db_connection()
    if conn is None:
        return "Ошибка подключения к базе данных", 500
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        select_query = """
        SELECT id, part_name, planned_quantity, machine_number, 
               start_time, end_time 
        FROM production_plan 
        WHERE id = %s;
        """
        
        cursor.execute(select_query, (id,))
        record = cursor.fetchone()
        
        if record is None:
            return "Запись не найдена", 404
        
        data = {
            'id': record[0],
            'part_name': record[1],
            'planned_quantity': record[2],
            'machine_number': record[3],
            'start_time': record[4].strftime('%Y-%m-%dT%H:%M'),
            'end_time': record[5].strftime('%Y-%m-%dT%H:%M')
        }
        
        return render_template('edit.html', data=data, username=session.get('username'), parts_list=parts_list)
        
    except (Exception, Error) as error:
        log_message(f"ERROR: Ошибка при получении записи: {error}")
        return f"Ошибка при получении данных: {error}", 500
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/production_report/<int:order_id>')
@login_required
def production_report(order_id):
    """Страница для создания отчета по производству"""
    conn = get_db_connection()
    if conn is None:
        return "Ошибка подключения к базе данных", 500
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        select_order_query = """
        SELECT id, part_name, planned_quantity, machine_number, 
               start_time, end_time 
        FROM production_plan 
        WHERE id = %s;
        """
        
        cursor.execute(select_order_query, (order_id,))
        order = cursor.fetchone()
        
        if order is None:
            return "Заказ не найден", 404
        
        select_report_query = """
        SELECT id, part_number, actual_quantity, bubble_count, underfill_count, 
               inclusion_count, defect_count, actual_start_time, actual_end_time
        FROM production 
        WHERE order_id = %s;
        """
        
        cursor.execute(select_report_query, (order_id,))
        existing_report = cursor.fetchone()
        
        order_data = {
            'id': order[0],
            'part_name': order[1],
            'planned_quantity': order[2],
            'machine_number': order[3],
            'start_time': order[4].strftime('%Y-%m-%d %H:%M'),
            'end_time': order[5].strftime('%Y-%m-%d %H:%M')
        }
        
        report_data = None
        if existing_report:
            report_data = {
                'id': existing_report[0],
                'part_number': existing_report[1] or '',
                'actual_quantity': existing_report[2],
                'bubble_count': existing_report[3],
                'underfill_count': existing_report[4],
                'inclusion_count': existing_report[5],
                'defect_count': existing_report[6],
                'actual_start_time': existing_report[7].strftime('%Y-%m-%dT%H:%M') if existing_report[7] else '',
                'actual_end_time': existing_report[8].strftime('%Y-%m-%dT%H:%M') if existing_report[8] else ''
            }
        
        return render_template('production_report.html', 
                             order=order_data, 
                             report=report_data,
                             username=session.get('username'))
        
    except (Exception, Error) as error:
        log_message(f"ERROR: Ошибка при получении данных отчета: {error}")
        return f"Ошибка при получении данных: {error}", 500
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/save_production_report', methods=['POST'])
@login_required
def save_production_report():
    """Сохранение отчета по производству"""
    try:
        data = request.get_json()
        
        order_id = data.get('order_id')
        part_number = data.get('part_number', '')
        actual_quantity = data.get('actual_quantity')
        bubble_count = data.get('bubble_count', 0)
        underfill_count = data.get('underfill_count', 0)
        inclusion_count = data.get('inclusion_count', 0)
        actual_start_time = data.get('actual_start_time')
        actual_end_time = data.get('actual_end_time')
        
        try:
            bubble = int(bubble_count)
            underfill = int(underfill_count)
            inclusion = int(inclusion_count)
            defect_count = bubble + underfill + inclusion
        except (ValueError, TypeError):
            defect_count = 0
        
        if not order_id:
            return jsonify({'success': False, 'error': 'ID заказа не указан'}), 400
        
        if not actual_quantity:
            return jsonify({'success': False, 'error': 'Фактическое количество не может быть пустым'}), 400
        
        if not actual_start_time:
            return jsonify({'success': False, 'error': 'Фактическое время начала не может быть пустым'}), 400
        
        if not actual_end_time:
            return jsonify({'success': False, 'error': 'Фактическое время окончания не может быть пустым'}), 400
        
        try:
            order_id_int = int(order_id)
            actual_quantity_int = int(actual_quantity)
            if actual_quantity_int <= 0:
                return jsonify({'success': False, 'error': 'Фактическое количество должно быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Неверный формат числового значения'}), 400
        
        try:
            actual_start_time_dt = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M')
            actual_end_time_dt = datetime.strptime(actual_end_time, '%Y-%m-%dT%H:%M')
            
            if actual_end_time_dt <= actual_start_time_dt:
                return jsonify({'success': False, 'error': 'Фактическое время окончания должно быть позже времени начала'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Неверный формат времени. Используйте YYYY-MM-DDTHH:MM'}), 400
        
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            check_order_query = "SELECT id FROM production_plan WHERE id = %s;"
            cursor.execute(check_order_query, (order_id_int,))
            if not cursor.fetchone():
                return jsonify({'success': False, 'error': 'Заказ не найден'}), 404
            
            check_report_query = "SELECT id FROM production WHERE order_id = %s;"
            cursor.execute(check_report_query, (order_id_int,))
            existing_report = cursor.fetchone()
            
            if existing_report:
                update_query = """
                UPDATE production 
                SET part_number = %s,
                    actual_quantity = %s, 
                    bubble_count = %s, 
                    underfill_count = %s, 
                    inclusion_count = %s,
                    defect_count = %s,
                    actual_start_time = %s, 
                    actual_end_time = %s
                WHERE order_id = %s
                RETURNING id;
                """
                
                cursor.execute(update_query, (
                    part_number.strip() if part_number else None,
                    actual_quantity_int,
                    int(bubble_count),
                    int(underfill_count),
                    int(inclusion_count),
                    defect_count,
                    actual_start_time_dt,
                    actual_end_time_dt,
                    order_id_int
                ))
                action = 'обновлен'
            else:
                insert_query = """
                INSERT INTO production 
                (order_id, part_number, actual_quantity, bubble_count, underfill_count, 
                 inclusion_count, defect_count, actual_start_time, actual_end_time) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING id;
                """
                
                cursor.execute(insert_query, (
                    order_id_int,
                    part_number.strip() if part_number else None,
                    actual_quantity_int,
                    int(bubble_count),
                    int(underfill_count),
                    int(inclusion_count),
                    defect_count,
                    actual_start_time_dt,
                    actual_end_time_dt
                ))
                action = 'сохранен'
            
            conn.commit()
            report_id = cursor.fetchone()[0]
            
            log_message(f"SUCCESS: Отчет по производству {action} с ID={report_id} для заказа {order_id_int}")
            
            return jsonify({
                'success': True,
                'message': f'Отчет по производству успешно {action} (ID отчета: {report_id})'
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при сохранении отчета: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/save_data', methods=['POST'])
@login_required
def save_data():
    """Сохранение новых данных в базу данных"""
    try:
        data = request.get_json()
        
        part_name = data.get('part_name')
        planned_quantity = data.get('planned_quantity')
        machine_number = data.get('machine_number')
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        
        if not part_name or not part_name.strip():
            return jsonify({'success': False, 'error': 'Part name не может быть пустым'}), 400
        
        if not planned_quantity:
            return jsonify({'success': False, 'error': 'Planned quantity не может быть пустым'}), 400
        
        if not machine_number:
            return jsonify({'success': False, 'error': 'Machine number не может быть пустым'}), 400
        
        if not start_time:
            return jsonify({'success': False, 'error': 'Start time не может быть пустым'}), 400
        
        if not end_time:
            return jsonify({'success': False, 'error': 'End time не может быть пустым'}), 400
        
        try:
            planned_quantity_int = int(planned_quantity)
            if planned_quantity_int <= 0:
                return jsonify({'success': False, 'error': 'Planned quantity должен быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Planned quantity должен быть числом'}), 400
        
        try:
            machine_number_int = int(machine_number)
            if machine_number_int <= 0:
                return jsonify({'success': False, 'error': 'Machine number должен быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Machine number должен быть числом'}), 400
        
        try:
            start_time_dt = datetime.strptime(start_time, '%Y-%m-%dT%H:%M')
            end_time_dt = datetime.strptime(end_time, '%Y-%m-%dT%H:%M')
            
            if end_time_dt <= start_time_dt:
                return jsonify({'success': False, 'error': 'End time должен быть позже start time'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Неверный формат времени. Используйте YYYY-MM-DDTHH:MM'}), 400
        
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO production_plan 
            (part_name, planned_quantity, machine_number, start_time, end_time) 
            VALUES (%s, %s, %s, %s, %s) 
            RETURNING id;
            """
            
            cursor.execute(insert_query, (
                part_name.strip(),
                planned_quantity_int, 
                machine_number_int,
                start_time_dt,
                end_time_dt
            ))
            conn.commit()
            
            inserted_id = cursor.fetchone()[0]
            
            log_message(f"SUCCESS: Добавлена запись с ID={inserted_id}")
            
            return jsonify({
                'success': True,
                'message': f'Данные успешно сохранены (ID: {inserted_id})',
                'data': {
                    'id': inserted_id,
                    'part_name': part_name.strip(),
                    'planned_quantity': planned_quantity_int,
                    'machine_number': machine_number_int,
                    'start_time': start_time_dt.strftime('%Y-%m-%d %H:%M'),
                    'end_time': end_time_dt.strftime('%Y-%m-%d %H:%M')
                }
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при сохранении данных: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/update_data/<int:id>', methods=['POST'])
@login_required
def update_data(id):
    """Обновление существующей записи в базе данных"""
    try:
        data = request.get_json()
        
        part_name = data.get('part_name')
        planned_quantity = data.get('planned_quantity')
        machine_number = data.get('machine_number')
        start_time = data.get('start_time')
        end_time = data.get('end_time')
        
        if not part_name or not part_name.strip():
            return jsonify({'success': False, 'error': 'Part name не может быть пустым'}), 400
        
        if not planned_quantity:
            return jsonify({'success': False, 'error': 'Planned quantity не может быть пустым'}), 400
        
        if not machine_number:
            return jsonify({'success': False, 'error': 'Machine number не может быть пустым'}), 400
        
        if not start_time:
            return jsonify({'success': False, 'error': 'Start time не может быть пустым'}), 400
        
        if not end_time:
            return jsonify({'success': False, 'error': 'End time не может быть пустым'}), 400
        
        try:
            planned_quantity_int = int(planned_quantity)
            if planned_quantity_int <= 0:
                return jsonify({'success': False, 'error': 'Planned quantity должен быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Planned quantity должен быть числом'}), 400
        
        try:
            machine_number_int = int(machine_number)
            if machine_number_int <= 0:
                return jsonify({'success': False, 'error': 'Machine number должен быть положительным числом'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Machine number должен быть числом'}), 400
        
        try:
            start_time_dt = datetime.strptime(start_time, '%Y-%m-%dT%H:%M')
            end_time_dt = datetime.strptime(end_time, '%Y-%m-%dT%H:%M')
            
            if end_time_dt <= start_time_dt:
                return jsonify({'success': False, 'error': 'End time должен быть позже start time'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Неверный формат времени. Используйте YYYY-MM-DDTHH:MM'}), 400
        
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            update_query = """
            UPDATE production_plan 
            SET part_name = %s,
                planned_quantity = %s, 
                machine_number = %s, 
                start_time = %s, 
                end_time = %s
            WHERE id = %s
            RETURNING id;
            """
            
            cursor.execute(update_query, (
                part_name.strip(),
                planned_quantity_int, 
                machine_number_int,
                start_time_dt,
                end_time_dt,
                id
            ))
            conn.commit()
            
            if cursor.rowcount == 0:
                return jsonify({'success': False, 'error': 'Запись не найдена'}), 404
            
            updated_id = cursor.fetchone()[0]
            
            log_message(f"SUCCESS: Обновлена запись с ID={updated_id}")
            
            return jsonify({
                'success': True,
                'message': f'Данные успешно обновлены (ID: {updated_id})',
                'data': {
                    'id': updated_id,
                    'part_name': part_name.strip(),
                    'planned_quantity': planned_quantity_int,
                    'machine_number': machine_number_int,
                    'start_time': start_time_dt.strftime('%Y-%m-%d %H:%M'),
                    'end_time': end_time_dt.strftime('%Y-%m-%d %H:%M')
                }
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при обновлении данных: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/get_filtered_data', methods=['GET'])
@login_required
def get_filtered_data():
    """Получение отфильтрованных данных по диапазону дат"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # Базовый запрос
            base_query = """
            SELECT 
                pp.id, 
                pp.part_name, 
                pp.planned_quantity, 
                pp.machine_number, 
                pp.start_time, 
                pp.end_time,
                CASE 
                    WHEN p.id IS NOT NULL THEN true 
                    ELSE false 
                END as has_report,
                CASE 
                    WHEN e.event_id IS NOT NULL AND e."Time_group" = 'Utilization hours' THEN true 
                    ELSE false 
                END as has_utilization_event
            FROM production_plan pp
            LEFT JOIN production p ON pp.id = p.order_id
            LEFT JOIN events e ON pp.id = e.batch AND e."Time_group" = 'Utilization hours'
            """
            
            where_conditions = []
            params = []
            
            # Добавляем условия фильтрации, если указаны даты
            if start_date:
                where_conditions.append("pp.start_time >= %s")
                params.append(start_date + ' 00:00:00')
            
            if end_date:
                where_conditions.append("pp.start_time <= %s")
                params.append(end_date + ' 23:59:59')
            
            # Собираем полный запрос
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
                select_query = base_query + " " + where_clause
            else:
                select_query = base_query
            
            # Добавляем GROUP BY и сортировку
            select_query += """
            GROUP BY pp.id, pp.part_name, pp.planned_quantity, pp.machine_number, 
                     pp.start_time, pp.end_time, p.id, e.event_id, e."Time_group"
            ORDER BY pp.start_time DESC, pp.id DESC
            """
            
            cursor.execute(select_query, params)
            records = cursor.fetchall()
            
            data_list = []
            for record in records:
                data_list.append({
                    'id': record[0],
                    'part_name': record[1],
                    'planned_quantity': record[2],
                    'machine_number': record[3],
                    'start_time': record[4].strftime('%Y-%m-%d %H:%M'),
                    'end_time': record[5].strftime('%Y-%m-%d %H:%M'),
                    'has_report': record[6],
                    'has_utilization_event': record[7]
                })
            
            return jsonify({
                'success': True,
                'count': len(data_list),
                'data': data_list
            })
            
        except (Exception, Error) as error:
            log_message(f"ERROR: Ошибка при получении отфильтрованных данных: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/get_data', methods=['GET'])
@login_required
def get_data():
    """Получение списка сохраненных данных - только 10 последних записей с информацией о наличии отчета и событий"""
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # Измененный запрос с LEFT JOIN для проверки наличия отчета и события с Utilization hours
            select_query = """
            SELECT 
                pp.id, 
                pp.part_name, 
                pp.planned_quantity, 
                pp.machine_number, 
                pp.start_time, 
                pp.end_time,
                CASE 
                    WHEN p.id IS NOT NULL THEN true 
                    ELSE false 
                END as has_report,
                CASE 
                    WHEN e.event_id IS NOT NULL AND e."Time_group" = 'Utilization hours' THEN true 
                    ELSE false 
                END as has_utilization_event
            FROM production_plan pp
            LEFT JOIN production p ON pp.id = p.order_id
            LEFT JOIN events e ON pp.id = e.batch AND e."Time_group" = 'Utilization hours'
            GROUP BY pp.id, pp.part_name, pp.planned_quantity, pp.machine_number, 
                     pp.start_time, pp.end_time, p.id, e.event_id, e."Time_group"
            ORDER BY pp.id DESC
            LIMIT 10;
            """
            
            cursor.execute(select_query)
            records = cursor.fetchall()
            
            data_list = []
            for record in records:
                data_list.append({
                    'id': record[0],
                    'part_name': record[1],
                    'planned_quantity': record[2],
                    'machine_number': record[3],
                    'start_time': record[4].strftime('%Y-%m-%d %H:%M'),
                    'end_time': record[5].strftime('%Y-%m-%d %H:%M'),
                    'has_report': record[6],  # True если отчет есть, False если нет
                    'has_utilization_event': record[7]  # True если есть событие с Utilization hours
                })
            
            return jsonify({
                'success': True,
                'count': len(data_list),
                'data': data_list
            })
            
        except (Exception, Error) as error:
            log_message(f"ERROR: Ошибка при получении данных: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/delete_data/<int:id>', methods=['DELETE'])
@login_required
def delete_data(id):
    """Удаление записи из базы данных"""
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            check_query = "SELECT id FROM production_plan WHERE id = %s;"
            cursor.execute(check_query, (id,))
            if not cursor.fetchone():
                return jsonify({'success': False, 'error': 'Запись не найдена'}), 404
            
           # УДАЛЕНИЕ связанных событий из таблицы events
            delete_events_query = "DELETE FROM events WHERE batch = %s RETURNING event_id;"
            cursor.execute(delete_events_query, (id,))
            deleted_events = cursor.fetchall()
            
            # УДАЛЕНИЕ связанных отчетов из таблицы production
            delete_production_query = "DELETE FROM production WHERE order_id = %s RETURNING id;"
            cursor.execute(delete_production_query, (id,))
            deleted_reports = cursor.fetchall()
            
            # УДАЛЕНИЕ из таблицы production_plan
            delete_plan_query = "DELETE FROM production_plan WHERE id = %s RETURNING id;"
            cursor.execute(delete_plan_query, (id,))
            conn.commit()
            
            deleted_id = cursor.fetchone()[0]
            
            log_message(f"SUCCESS: Удалена запись с ID={deleted_id}")
            
            return jsonify({
                'success': True,
                'message': f'Запись с ID {deleted_id} успешно удалена'
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при удалении данных: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/events/<int:batch_id>')
@login_required
def events_page(batch_id):
    """Страница для внесения событий"""
    conn = get_db_connection()
    if conn is None:
        return "Ошибка подключения к базе данных", 500
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Получаем информацию о батче
        select_batch_query = """
        SELECT id, part_name, planned_quantity, machine_number, 
               start_time, end_time 
        FROM production_plan 
        WHERE id = %s;
        """
        
        cursor.execute(select_batch_query, (batch_id,))
        batch = cursor.fetchone()
        
        if batch is None:
            return "Батч не найден", 404
        
        # Получаем существующие события для этого батча с русскими названиями столбцов
        select_events_query = """
        SELECT event_id, event_name, 
               "Фактические время начала события", "Фактические время конца события", 
               "Time_group", responsible, comments
        FROM events 
        WHERE batch = %s 
        ORDER BY "Фактические время начала события" DESC;
        """
        
        cursor.execute(select_events_query, (batch_id,))
        events = cursor.fetchall()
        
        # Форматируем данные батча
        batch_data = {
            'id': batch[0],
            'part_name': batch[1],
            'planned_quantity': batch[2],
            'machine_number': batch[3],
            'start_time': batch[4].strftime('%Y-%m-%d %H:%M'),
            'end_time': batch[5].strftime('%Y-%m-%d %H:%M')
        }
        
        # Форматируем события
        events_list = []
        for event in events:
            events_list.append({
                'event_id': event[0],
                'event_name': event[1],
                'actual_start_time': event[2].strftime('%Y-%m-%d %H:%M'),
                'actual_end_time': event[3].strftime('%Y-%m-%d %H:%M'),
                'time_group': event[4],
                'responsible': event[5],
                'comments': event[6]
            })
        
        return render_template('events.html', 
                             batch=batch_data, 
                             events=events_list,
                             username=session.get('username'))
        
    except (Exception, Error) as error:
        log_message(f"ERROR: Ошибка при получении данных событий: {error}")
        return f"Ошибка при получении данных: {error}", 500
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/save_event', methods=['POST'])
@login_required
def save_event():
    """Сохранение нового события"""
    try:
        data = request.get_json()
        
        batch_id = data.get('batch_id')
        event_name = data.get('event_name')
        actual_start_time = data.get('actual_start_time')
        actual_end_time = data.get('actual_end_time')
        time_group = data.get('time_group')
        responsible = data.get('responsible')
        comments = data.get('comments')
        
        if not batch_id:
            return jsonify({'success': False, 'error': 'ID батча не указан'}), 400
        
        if not event_name or not event_name.strip():
            return jsonify({'success': False, 'error': 'Название события не может быть пустым'}), 400
        
        if not actual_start_time:
            return jsonify({'success': False, 'error': 'Время начала события не может быть пустым'}), 400
        
        if not actual_end_time:
            return jsonify({'success': False, 'error': 'Время окончания события не может быть пустым'}), 400
        
        if not time_group or time_group not in ['Planned pause time', 'Utilization hours', 'Breakdown time']:
            return jsonify({'success': False, 'error': 'Неверная группа времени'}), 400
        
        try:
            actual_start_time_dt = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M')
            actual_end_time_dt = datetime.strptime(actual_end_time, '%Y-%m-%dT%H:%M')
            
            if actual_end_time_dt <= actual_start_time_dt:
                return jsonify({'success': False, 'error': 'Время окончания должно быть позже времени начала'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'Неверный формат времени. Используйте YYYY-MM-DDTHH:MM'}), 400
        
        # Дополнительная валидация для Breakdown time
        if time_group == 'Breakdown time':
            if not responsible:
                return jsonify({'success': False, 'error': 'Для Breakdown time необходимо указать ответственного'}), 400
            
            if responsible not in ['FMNTC', 'Production', 'Engineering', 'DMNTC']:
                return jsonify({'success': False, 'error': 'Неверное значение ответственного'}), 400
        
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # Проверяем, существует ли батч
            check_batch_query = "SELECT id FROM production_plan WHERE id = %s;"
            cursor.execute(check_batch_query, (batch_id,))
            if not cursor.fetchone():
                return jsonify({'success': False, 'error': 'Батч не найден'}), 404
            
            # Вставляем новое событие с русскими названиями столбцов
            insert_query = """
            INSERT INTO events 
            (event_name, batch, "Фактические время начала события", "Фактические время конца события", 
             "Time_group", responsible, comments) 
            VALUES (%s, %s, %s, %s, %s, %s, %s) 
            RETURNING event_id;
            """
            
            cursor.execute(insert_query, (
                event_name.strip(),
                batch_id,
                actual_start_time_dt,
                actual_end_time_dt,
                time_group,
                responsible if time_group == 'Breakdown time' else None,
                comments.strip() if comments else None
            ))
            conn.commit()
            
            event_id = cursor.fetchone()[0]
            
            log_message(f"SUCCESS: Событие сохранено с event_id={event_id} для батча {batch_id}")
            
            # Получаем обновленный список событий
            select_events_query = """
            SELECT event_id, event_name, 
                   "Фактические время начала события", "Фактические время конца события", 
                   "Time_group", responsible, comments
            FROM events 
            WHERE batch = %s 
            ORDER BY "Фактические время начала события" DESC;
            """
            
            cursor.execute(select_events_query, (batch_id,))
            events = cursor.fetchall()
            
            events_list = []
            for event in events:
                events_list.append({
                    'event_id': event[0],
                    'event_name': event[1],
                    'actual_start_time': event[2].strftime('%Y-%m-%d %H:%M'),
                    'actual_end_time': event[3].strftime('%Y-%m-%d %H:%M'),
                    'time_group': event[4],
                    'responsible': event[5],
                    'comments': event[6]
                })
            
            return jsonify({
                'success': True,
                'message': f'Событие успешно сохранено (ID: {event_id})',
                'events': events_list
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при сохранении события: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/delete_event/<int:event_id>', methods=['DELETE'])
@login_required
def delete_event(event_id):
    """Удаление события"""
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'error': 'Ошибка подключения к базе данных'}), 500
        
        cursor = None
        try:
            cursor = conn.cursor()
            
            # Сначала получаем batch_id для обновления списка
            select_batch_query = "SELECT batch FROM events WHERE event_id = %s;"
            cursor.execute(select_batch_query, (event_id,))
            result = cursor.fetchone()
            
            if not result:
                return jsonify({'success': False, 'error': 'Событие не найдено'}), 404
            
            batch_id = result[0]
            
            # Удаляем событие
            delete_query = "DELETE FROM events WHERE event_id = %s RETURNING event_id;"
            cursor.execute(delete_query, (event_id,))
            conn.commit()
            
            deleted_id = cursor.fetchone()[0]
            
            # Получаем обновленный список событий
            select_events_query = """
            SELECT event_id, event_name, 
                   "Фактические время начала события", "Фактические время конца события", 
                   "Time_group"
            FROM events 
            WHERE batch = %s 
            ORDER BY "Фактические время начала события" DESC;
            """
            
            cursor.execute(select_events_query, (batch_id,))
            events = cursor.fetchall()
            
            events_list = []
            for event in events:
                events_list.append({
                    'event_id': event[0],
                    'event_name': event[1],
                    'actual_start_time': event[2].strftime('%Y-%m-%d %H:%M'),
                    'actual_end_time': event[3].strftime('%Y-%m-%d %H:%M'),
                    'time_group': event[4]
                })
            
            log_message(f"SUCCESS: Удалено событие с event_id={deleted_id}")
            
            return jsonify({
                'success': True,
                'message': f'Событие с ID {deleted_id} успешно удалено',
                'events': events_list
            })
            
        except (Exception, Error) as error:
            if conn:
                conn.rollback()
            log_message(f"ERROR: Ошибка при удалении события: {error}")
            return jsonify({'success': False, 'error': str(error)}), 500
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        log_message(f"ERROR: Ошибка обработки запроса: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/logout')
def logout():
    """Выход из системы"""
    username = session.get('username', 'Неизвестный пользователь')
    session.pop('logged_in', None)
    session.pop('username', None)
    log_message(f"INFO: Пользователь {username} вышел из системы")
    return redirect(url_for('login'))

if __name__ == '__main__':
    log_message("INFO: Инициализация базы данных...")
    if init_database():
        log_message("INFO: База данных готова к работе")
    else:
        log_message("WARNING: Возникли проблемы с инициализацией базы данных")
    
    app.run(debug=True, host='0.0.0.0', port=5000)