# Redmine

Python backend для интеграции с Redmine, готовый к деплою на Render.

## Что есть в проекте

- FastAPI-приложение
- PostgreSQL через SQLAlchemy
- healthcheck endpoint
- конфигурация через переменные окружения
- `render.yaml` для деплоя на Render

## Структура проекта

```text
Redmine/
├─ src/
│  └─ redmine/
│     ├─ __init__.py
│     ├─ app.py
│     ├─ config.py
│     ├─ db.py
│     └─ main.py
├─ tests/
│  └─ test_basic.py
├─ .env.example
├─ .gitignore
├─ pyproject.toml
├─ render.yaml
├─ requirements.txt
└─ README.md
```

## Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate  # Linux / macOS
pip install -r requirements.txt
cp .env.example .env
uvicorn src.redmine.app:app --reload
```

Для Windows:

```bash
.venv\Scripts\activate
```

## Переменные окружения

```env
APP_ENV=development
APP_HOST=0.0.0.0
APP_PORT=8000
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/redmine
REDMINE_URL=https://redmine.sms-it.ru
REDMINE_API_KEY=your_api_key
```

## Полезные endpoints

- `GET /` — базовый ответ API
- `GET /health` — проверка доступности сервиса
- `GET /db-health` — проверка подключения к PostgreSQL

## Деплой на Render

1. Подключить репозиторий GitHub в Render.
2. Создать PostgreSQL.
3. Создать Web Service.
4. Убедиться, что переменная `DATABASE_URL` передаётся в сервис.
5. Render запустит приложение командой из `render.yaml`.
