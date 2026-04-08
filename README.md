# Redmine

Python-проект для работы с Redmine.

## Структура проекта

```text
Redmine/
├─ src/
│  └─ redmine/
│     ├─ __init__.py
│     ├─ main.py
│     └─ config.py
├─ tests/
│  └─ test_basic.py
├─ .gitignore
├─ pyproject.toml
├─ requirements.txt
└─ README.md
```

## Быстрый старт

### 1. Создать виртуальное окружение

```bash
python -m venv .venv
```

### 2. Активировать окружение

**Windows**

```bash
.venv\Scripts\activate
```

**Linux / macOS**

```bash
source .venv/bin/activate
```

### 3. Установить зависимости

```bash
pip install -r requirements.txt
```

### 4. Запустить проект

```bash
python -m src.redmine.main
```

### 5. Запустить тесты

```bash
pytest
```

## Планы

- интеграция с API Redmine
- работа с задачами и проектами
- конфигурация через переменные окружения
- покрытие тестами
