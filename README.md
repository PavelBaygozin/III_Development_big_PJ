```markdown
# Airflow DAG для расчёта витрины активности клиентов

Этот проект представляет собой DAG (Directed Acyclic Graph) для Apache Airflow, который автоматизирует процесс ETL (Extract, Transform, Load) для расчёта витрины активности клиентов на основе их транзакций.

## Описание задачи

### Краткое описание
Необходимо создать DAG в Apache Airflow, который будет:
1. Извлекать данные из файла `profit_table.csv`.
2. Преобразовывать данные с использованием функции `transform` из файла `transform_script.py`.
3. Сохранять результаты в файл `flags_activity.csv` без перезаписи старых данных.

### Подробности
- **Extract**: Данные хранятся в файле `profit_table.csv`. В этом файле содержатся суммы и количества транзакций для каждого клиента по 10 продуктам за каждый месяц.
- **Transform**: Используется функция `transform` из файла `transform_script.py`, которая рассчитывает флаги активности клиентов по продуктам. Клиент считается активным по продукту, если в предыдущие 3 месяца у него были ненулевые сумма и количество транзакций.
- **Load**: Результаты сохраняются в файл `flags_activity.csv`. Новые данные добавляются в конец файла без перезаписи старых данных.

### Расписание
DAG запускается каждый месяц 5-го числа.

---

## Структура проекта

```
airflow-dag-task/
├── dags/
│   └── Baygozin_pavel_dag.py          # Файл с DAG
├── data/
│   ├── profit_table.csv               # Исходные данные
│   └── flags_activity.csv             # Результаты расчётов
├── scripts/
│   └── transform_script.py            # Скрипт для преобразования данных
├── screenshots/                       # Скриншоты выполнения DAG
├── README.md                          # Описание проекта
└── requirements.txt                   # Зависимости
```

---

## Установка и настройка

### 1. Установка Apache Airflow
Установите Apache Airflow, выполнив следующие команды:
```bash
pip install apache-airflow
```

### 2. Инициализация Airflow
Инициализируйте базу данных Airflow:
```bash
airflow db init
```

### 3. Запуск Airflow
Запустите веб-сервер и планировщик:
```bash
airflow webserver --port 8080
airflow scheduler
```

### 4. Установка зависимостей
Установите необходимые библиотеки:
```bash
pip install pandas numpy
```

---

## Использование

1. Поместите файл `profit_table.csv` в папку `data/`.
2. Запустите DAG через веб-интерфейс Airflow:
   - Откройте веб-интерфейс по адресу `http://localhost:8080`.
   - Найдите DAG с названием `Baygozin_pavel_dag`.
   - Включите DAG (если он выключен) и нажмите **Trigger DAG** для ручного запуска.
3. После выполнения DAG проверьте файл `flags_activity.csv` в папке `data/`. Новые данные будут добавлены в конец файла.

---

## Скриншоты

### Граф DAG
![Граф DAG](screenshots/dag_graph.png)

### Успешное выполнение задач
![Успешное выполнение](screenshots/dag_success.png)

---

## Параллельное выполнение (дополнительно)

Для параллельного выполнения задач по продуктам модифицируйте DAG, добавив `TaskGroup`. Пример кода:
```python
from flow.utils.task_group import TaskGroup

with dag:
    with TaskGroup('product_tasks') as product_tasks:
        for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
            transform_task = PythonOperator(
                task_id=f'transform_{product}',
                python_callable=transform,
                op_kwargs={'product': product},
                provide_context=True,
            )
            extract_task >> transform_task >> load_task
```
