"""Annual company model: validation, scenario totals and protected HTTP routes."""

from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from src.redmine.dates import getSnapshotBusinessDate
from src.redmine import year_model_store as store

CONFIDENCE = {"none": "Нет", "low": "Низкая", "medium": "Средняя", "high": "Высокая", "fact": "Факт"}
DEFAULT_CONFIDENCE = ["low", "medium", "high", "fact"]
KINDS = {"direction", "team", "topic", "decision"}
TEMPLATE = Path(__file__).with_name("templates") / "year_model.html"
MONTHS = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]


def money(value):
    try:
        amount = Decimal(str(value).replace(" ", "").replace("\u00a0", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        raise ValueError("Введите корректную сумму в млн ₽") from None
    if not amount.is_finite() or abs(amount) > Decimal("1000000000"):
        raise ValueError("Сумма должна быть конечным числом в пределах ±1 000 000 000 млн ₽")
    if amount.as_tuple().exponent < -6:
        raise ValueError("Допустимо не более 6 знаков после запятой")
    return amount


def team_costs(item):
    values = item.get("monthly_costs", [item.get("monthly_cost", 0)] * 12)
    if not isinstance(values, list) or len(values) != 12:
        raise ValueError("Укажите затраты для всех 12 месяцев")
    costs = [money(value) for value in values]
    if any(value > 0 for value in costs):
        raise ValueError("Затраты команды указываются отрицательным числом или нулем")
    return costs


def boolean_value(value, label):
    if not isinstance(value, bool):
        raise ValueError(f"{label}: укажите логическое значение")
    return value


def validate_item(kind, year, data, items, old, delete):
    if kind not in KINDS:
        raise ValueError("Неизвестный тип записи")
    if delete:
        if old.get("company"):
            raise ValueError("Направление «Компания» нельзя удалить")
        if kind == "direction" and any(item.get("direction_id") == old["id"] for item in items):
            raise ValueError("Сначала перенесите или удалите команды, темы и решения направления")
        return {key: value for key, value in {**old, "deleted": True}.items()
                if key not in {"id", "kind", "year", "revision", "recorded_at", "actor"}}
    name = str(data.get("name") or "").strip()
    if not name or len(name) > 250:
        raise ValueError("Название обязательно и должно содержать не более 250 символов")
    clean = {"name": name}
    if kind == "direction":
        company = bool(old and old.get("company"))
        if company and name != "Компания":
            raise ValueError("Название направления «Компания» нельзя изменить")
        if not company and name.casefold() == "компания":
            raise ValueError("Направление «Компания» создается автоматически")
        if any(item["kind"] == kind and item["id"] != (old or {}).get("id")
               and item["name"].casefold() == name.casefold() for item in items):
            raise ValueError("Направление с таким названием уже существует")
        target = money(data.get("target", 0))
        if target < 0:
            raise ValueError("Целевая выручка не может быть отрицательной")
        clean.update(target=str(target), company=company, order=(old or {}).get("order", len(items)))
        return clean
    direction = next((item for item in items if item["kind"] == "direction"
                      and item["id"] == data.get("direction_id")), None)
    if direction is None:
        raise ValueError("Выберите существующее направление этого года")
    note = str(data.get("note") or "").strip()
    if len(note) > 10000:
        raise ValueError("Примечание не должно превышать 10 000 символов")
    clean.update(direction_id=direction["id"], note=note)
    if kind == "team":
        if direction.get("company"):
            raise ValueError("В направлении «Компания» нельзя размещать команды")
        if "cost_month" in data:
            if str(data["cost_month"]) not in {str(month) for month in range(1, 13)}:
                raise ValueError("Выберите месяц от января до декабря")
            month = int(data["cost_month"]) - 1
            copy_forward = boolean_value(data.get("copy_forward", True), "Копирование на новые месяцы")
            costs = team_costs(old or {})
            cost = money(data.get("monthly_cost", 0))
            for index in range(month, 12 if copy_forward else month + 1):
                costs[index] = cost
            costs = team_costs({"monthly_costs": costs})
        elif "monthly_costs" in data or "monthly_cost" in data:
            costs = team_costs(data)
        else:
            costs = team_costs(old or {})
        clean.update(monthly_cost=str(costs[0]), monthly_costs=[str(value) for value in costs])
    else:
        confidence = data.get("confidence")
        if confidence not in CONFIDENCE:
            raise ValueError("Выберите уверенность: нет, низкая, средняя, высокая или факт")
        try:
            expected = date.fromisoformat(str(data.get("expected_date")))
        except ValueError:
            raise ValueError("Укажите корректную ожидаемую дату") from None
        if expected.year != year:
            raise ValueError("Ожидаемая дата должна находиться в выбранном году")
        precision = data.get("date_precision", "month")
        if precision not in {"month", "day"}:
            raise ValueError("Неизвестная точность даты")
        amount = money(data.get("amount", 0))
        if kind == "topic" and amount < 0:
            raise ValueError("Стоимость темы не может быть отрицательной")
        clean.update(confidence=confidence, amount=str(amount), expected_date=expected.isoformat(), date_precision=precision)
        clean["attention"] = boolean_value(data.get("attention", (old or {}).get("attention", False)), "Обратить внимание")
        if kind == "topic":
            url = str(data.get("url") or "").strip()
            try:
                parts = urlsplit(url)
                valid = parts.scheme in {"https", "http"} and parts.hostname and not parts.username and not parts.password
            except ValueError:
                valid = False
            if url and (len(url) > 2000 or not valid):
                raise ValueError("Ссылка CRM должна быть полным адресом http:// или https:// без логина и пароля")
            clean["url"] = url
    return clean


def summarize(items, confidence):
    """Keep full precision; round only at the presentation boundary."""
    directions = sorted([dict(item) for item in items if item["kind"] == "direction"],
                        key=lambda item: (bool(item.get("company")), item.get("order", 0), item["name"]))
    zero = Decimal(0)
    for direction in directions:
        children = [item for item in items if item.get("direction_id") == direction["id"]]
        teams = []
        monthly = [zero] * 12
        for item in sorted([item for item in children if item["kind"] == "team"], key=lambda item: item["name"]):
            values = team_costs(item)
            monthly = [a + b for a, b in zip(monthly, values)]
            teams.append({**item, "monthly_costs": [float(value) for value in values], "annual_cost": float(sum(values, zero))})
        events = sorted([item for item in children if item["kind"] in {"topic", "decision"}
                         and item["confidence"] in confidence], key=lambda item: (item["expected_date"], item["name"]))
        costs = [sum(monthly, zero), zero, zero]
        revenue = [zero, zero, zero]
        decisions = [zero, zero, zero]
        for event in events:
            bucket = {"high": 0, "fact": 0, "medium": 1, "low": 2}.get(event["confidence"])
            if bucket is None:
                continue
            amount = money(event["amount"])
            if event["kind"] == "topic":
                revenue[bucket] += amount
            elif amount < 0:
                costs[bucket] += amount
            else:
                revenue[bucket] += amount
                # Keep this subtotal for existing API clients; it is already part of revenue.
                decisions[bucket] += amount
        direction.update(teams=teams, events=events, monthly_costs=monthly,
                         summary={"target": [money(direction["target"]), zero, zero],
                                  "costs": costs, "revenue": revenue, "decisions": decisions})
    company = next((direction for direction in directions if direction.get("company")), None)
    if company:
        for direction in directions:
            if direction is company:
                continue
            for key in ("target", "costs", "revenue", "decisions"):
                company["summary"][key] = [a + b for a, b in zip(company["summary"][key], direction["summary"][key])]
            company["monthly_costs"] = [a + b for a, b in zip(company["monthly_costs"], direction["monthly_costs"])]
    for direction in directions:
        summary = direction["summary"]
        balance = revenue_total = zero
        summary["balance"] = []
        summary["revenue_total"] = []
        for index in range(3):
            balance += summary["costs"][index] + summary["revenue"][index]
            summary["balance"].append(balance)
            revenue_total += summary["revenue"][index]
            summary["revenue_total"].append(revenue_total)
        direction["summary"] = {key: [float(value) for value in values] for key, values in summary.items()}
        direction["team_annual_cost"] = float(sum(direction["monthly_costs"], zero))
        direction["monthly_costs"] = [float(value) for value in direction["monthly_costs"]]
    return directions


def query_model(year, as_of, confidence):
    today = getSnapshotBusinessDate()
    year = year or today.year
    as_of = as_of or today
    if not 2000 <= as_of.year <= 2100:
        raise HTTPException(422, "Дата среза должна быть в диапазоне 2000–2100 годов")
    selected = DEFAULT_CONFIDENCE if confidence is None else [v for v in confidence.split(",") if v]
    if any(value not in CONFIDENCE for value in selected):
        raise HTTPException(422, "Неизвестное значение уверенности")
    items, years = store.read_snapshot(year, as_of)
    return {"year": year, "as_of": as_of.isoformat(), "today": today.isoformat(), "confidence": selected,
            "years": sorted(set(years + [year, today.year, today.year + 1])),
            "directions": summarize(items, selected), "items": items,
            "unit": "млн ₽"}


class WritePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    year: int = Field(ge=2000, le=2100)
    kind: str
    data: dict
    revision: int | None = Field(default=None, ge=1)


class ImportItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str = Field(min_length=1, max_length=250)
    kind: str
    direction_key: str | None = None
    data: dict


class ImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    year: int = Field(ge=2000, le=2100)
    items: list[ImportItem] = Field(min_length=1, max_length=1000)


class ViewSettingsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    selected_year: int | None = Field(default=None, ge=2000, le=2100)
    direction_id: str | None = Field(default=None, min_length=1, max_length=36)
    collapsed: bool | None = None


def register_year_model(app, require_user, has_roles, render_html):
    def access(request: Request, response: Response):
        response.headers["Cache-Control"] = "private, no-store"
        user = require_user(request)
        if not has_roles(user, ("Admin", "Finance")):
            raise HTTPException(403, "Модель года доступна только пользователям с правами Admin или Finance")
        if request.method not in {"GET", "HEAD"}:
            origin = request.headers.get("origin")
            if origin and origin != str(request.base_url).rstrip("/"):
                raise HTTPException(403, "Недопустимый источник запроса")
        return user

    @app.get("/year-model", dependencies=[Depends(access)])
    def page():
        response = render_html(TEMPLATE.read_text(encoding="utf-8"))
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.get("/api/year-model", dependencies=[Depends(access)])
    def model(year: int | None = Query(None, ge=2000, le=2100), as_of: date | None = None, confidence: str | None = None):
        return query_model(year, as_of, confidence)

    @app.get("/api/year-model/view-settings")
    def get_view(user=Depends(access)):
        return store.read_view_settings(str(user["login"]))

    @app.put("/api/year-model/view-settings")
    def save_view(payload: ViewSettingsPayload, user=Depends(access)):
        if (payload.direction_id is None) != (payload.collapsed is None):
            raise HTTPException(422, "Передайте направление и его состояние вместе")
        if payload.selected_year is None and payload.direction_id is None:
            raise HTTPException(422, "Нет настроек для сохранения")
        try:
            return store.save_view_settings(str(user["login"]), **payload.model_dump())
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/year-model/items/{entity_id}/history", dependencies=[Depends(access)])
    def history(entity_id: str, as_of: date | None = None):
        if as_of is not None and not 2000 <= as_of.year <= 2100:
            raise HTTPException(422, "Дата среза должна быть в диапазоне 2000–2100 годов")
        rows = store.read_history(entity_id, as_of or getSnapshotBusinessDate())
        if not rows:
            raise HTTPException(404, "Запись не найдена на выбранную дату")
        return {"versions": rows}

    def write(payload, user, entity_id=None, delete=False):
        try:
            item_id = store.save(payload.year, payload.kind, payload.data, str(user["login"]),
                lambda data, items, old, deleted: validate_item(payload.kind, payload.year, data, items, old, deleted),
                entity_id, payload.revision, delete)
            return {"id": item_id}
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except FileExistsError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/year-model/items")
    def create(payload: WritePayload, user=Depends(access)):
        return write(payload, user)

    @app.post("/api/year-model/import")
    def initial_import(payload: ImportPayload, user=Depends(access)):
        try:
            return store.import_initial(payload.year, [item.model_dump() for item in payload.items], str(user["login"]),
                lambda kind, data, current: validate_item(kind, payload.year, data, current, None, False))
        except FileExistsError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.put("/api/year-model/items/{entity_id}")
    def update(entity_id: str, payload: WritePayload, user=Depends(access)):
        return write(payload, user, entity_id)

    @app.delete("/api/year-model/items/{entity_id}")
    def delete(entity_id: str, payload: WritePayload, user=Depends(access)):
        return write(payload, user, entity_id, True)

    @app.get("/api/year-model/export", dependencies=[Depends(access)])
    def export(year: int | None = Query(None, ge=2000, le=2100), as_of: date | None = None, confidence: str | None = None):
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        payload = query_model(year, as_of, confidence)
        book = Workbook()
        summary = book.active
        summary.title = "Резюме"
        summary.append([f"Модель {payload['year']} · на {payload['as_of']} · млн ₽"])
        summary.append(["Направление", "Показатель", "Высокая + факт", "Средняя", "Низкая"])
        events = book.create_sheet("Темы и решения")
        events.append(["Направление", "Тип", "Название", "Уверенность", "Сумма, млн ₽", "Ожидаемая дата", "CRM", "Примечание", "Обратить внимание"])
        teams = book.create_sheet("Команды")
        teams.append(["Направление", "Команда", *[f"{month}, млн ₽" for month in MONTHS], "Затраты за год, млн ₽", "Примечание"])
        for direction in payload["directions"]:
            for key, label in [("target", "Цель по выручке"), ("costs", "Затраты"), ("revenue", "Выручка"),
                               ("revenue_total", "Выручка (нар. итог)"), ("balance", "Баланс")]:
                values = [None if index > 0 and (key == "target" or key == "costs" and value == 0) else value
                          for index, value in enumerate(direction["summary"][key])]
                summary.append([direction["name"], label, *values])
            for item in direction["events"]:
                events.append([direction["name"], "Тема" if item["kind"] == "topic" else "Решение", item["name"],
                               CONFIDENCE[item["confidence"]], float(money(item["amount"])), item["expected_date"], item.get("url", ""), item.get("note", ""), "Да" if item.get("attention") else "Нет"])
            for item in direction["teams"]:
                teams.append([direction["name"], item["name"], *item["monthly_costs"],
                              item["annual_cost"], item.get("note", "")])
        for sheet in book:
            sheet.freeze_panes = "C3" if sheet is summary else "C2"
            for row in sheet:
                for cell in row:
                    if isinstance(cell.value, str):
                        cell.data_type = "s"  # User text is never an Excel formula.
                    elif isinstance(cell.value, (int, float)):
                        cell.number_format = '#,##0.#;-#,##0.#;0'
            for cell in sheet[2 if sheet is summary else 1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="375D77")
            for column in sheet.columns:
                sheet.column_dimensions[column[0].column_letter].width = min(58, max(18, max(len(str(c.value or "")) for c in column) + 2))
        stream = BytesIO()
        book.save(stream)
        return Response(stream.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": f'attachment; filename="year-model-{payload["year"]}.xlsx"', "Cache-Control": "no-store"})
