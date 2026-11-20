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

## Гарантированный деплой через GitHub Actions

Чтобы сайт обновлялся предсказуемо, в репозиторий добавлен workflow
`/.github/workflows/deploy-render.yml`.

Он делает три шага:

1. Берет SHA коммита из `main`
2. Вызывает Render Deploy Hook именно для этого SHA
3. Ждет, пока `/health` на сайте не начнет возвращать тот же `RENDER_GIT_COMMIT`

Для включения нужно один раз добавить в GitHub Secret:

- `RENDER_DEPLOY_HOOK_URL`

Значение берется в Render:

- `Service` -> `Settings` -> `Deploy Hook`

После этого каждый push в `main` будет не просто "просить Render собрать сайт",
а проверять, что сайт действительно поднялся на нужном коммите.

## Модель года

`/year-model` — годовой план компании с дорожками направлений, темами,
решениями, ежемесячными затратами команд и сводными сценариями.
Страница и все `/api/year-model` endpoints доступны ролям Admin и Finance.
Все суммы — в миллионах рублей; округление до одного знака происходит
только при отображении. В итогах высокой уверенности учитываются темы
с уверенностью «Высокая» и «Факт». Целевая выручка показана отдельно и не включается в баланс.

Данные хранятся в PostgreSQL в отдельных таблицах `year_model_*`.
Изменение и удаление добавляют версии с временем UTC и автором.
Срез выбирает последние версии до конца дня в часовом поясе приложения.
При конфликте версий API возвращает 409. Таблицы создаются при первом
использовании раздела, существующие таблицы приложения не изменяются.

Первоначальные финансовые данные не включаются в публичный репозиторий.
Для пустого года на странице доступна загрузка начального плана из JSON:
`{"year": 2027, "items": [...]}`. Элемент содержит уникальный `key`, `kind`
(`direction`, `team`, `topic`, `decision`) и `data`; дочерние записи также
содержат `direction_key`, ссылающийся на ключ направления. Ключ `company`
зарезервирован для автоматически создаваемого направления «Компания».
`POST /api/year-model/import` валидирует весь пакет в одной транзакции;
повторная загрузка успешно импортированного года не создает дубликаты.
Тесты используют только вымышленные финансовые данные.

Год и свернутые направления сохраняются отдельно для каждого пользователя в
`year_model_view_settings` и восстанавливаются при следующем открытии раздела.
