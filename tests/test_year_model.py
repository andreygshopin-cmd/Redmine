from datetime import UTC, date, datetime
from io import BytesIO

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, update
from sqlalchemy.pool import StaticPool

from src.redmine import app as app_module
from src.redmine import db
from src.redmine import year_model as model
from src.redmine import year_model_store as store


def sample_import():
    # Entirely fictional fixtures: production financial data is imported privately.
    items = [
        {"key": "alpha", "kind": "direction", "data": {"name": "Alpha", "target": "120"}},
        {"key": "beta", "kind": "direction", "data": {"name": "Beta", "target": "80"}},
    ]
    for i, (direction, cost) in enumerate([("alpha", "-2"), ("alpha", "-3"), ("beta", "-1")]):
        items.append({"key": f"team-{i}", "kind": "team", "direction_key": direction,
                      "data": {"name": f"Team {i}", "monthly_cost": cost}})
    for i, (confidence, amount) in enumerate([("fact", "12.34"), ("high", "20"), ("medium", "15"), ("low", "7")]):
        items.append({"key": f"topic-{i}", "kind": "topic", "direction_key": "alpha" if i < 3 else "beta",
                      "data": {"name": f"Topic {i}", "confidence": confidence, "amount": amount, "expected_date": "2027-04-01"}})
    items.append({"key": "decision", "kind": "decision", "direction_key": "company",
                  "data": {"name": "Example decision", "confidence": "high", "amount": "-8", "expected_date": "2027-03-01"}})
    return {"year": 2027, "items": items}


@pytest.fixture
def environment(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(store, "_ready_engine", None)
    monkeypatch.setattr(app_module, "_ensureAuthStorage", lambda: None)
    user = {"login": "planner", "roles": ["Admin"], "must_change_password": False}
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: user)
    client = TestClient(app_module.app)
    assert client.post("/api/year-model/import", json=sample_import()).status_code == 200
    yield client, user, engine
    engine.dispose()


def snapshot(client, **params):
    response = client.get("/api/year-model", params={"year": 2027, **params})
    assert response.status_code == 200, response.text
    return response.json()


def test_seed_and_company_consolidation(environment):
    client, _, engine = environment
    data = snapshot(client)
    assert len(data["directions"]) == 3
    assert len([item for item in data["items"] if item["kind"] == "team"]) == 3
    assert len([item for item in data["items"] if item["kind"] == "topic"]) == 4
    company = data["directions"][-1]
    assert company["name"] == "Компания"
    assert company["teams"] == []
    assert company["monthly_costs"] == [-6] * 12
    assert company["summary"]["target"] == [200, 0, 0]
    assert company["summary"]["costs"] == [-80, 0, 0]
    assert company["summary"]["revenue"] == [32.34, 15, 7]
    assert company["summary"]["revenue_total"] == [32.34, 47.34, 54.34]
    assert company["summary"]["balance"] == [-47.66, -32.66, -25.66]
    assert company["team_annual_cost"] == -72
    alpha = data["directions"][0]
    assert alpha["summary"]["costs"] == [-60, 0, 0]
    assert alpha["summary"]["revenue"] == [32.34, 15, 0]
    assert alpha["summary"]["revenue_total"] == [32.34, 47.34, 47.34]
    assert [team["annual_cost"] for team in alpha["teams"]] == [-24, -36]
    # Restart and repeated reads never duplicate seeds, including edited/deleted seeds.
    store._ready_engine = None
    assert len(snapshot(client)["items"]) == len(data["items"]) == 11
    assert client.post("/api/year-model/import", json=sample_import()).json()["already_imported"] is True
    assert len(snapshot(client)["items"]) == 11


def test_reference_balance_and_decisions():
    items = [{"id": "d", "kind": "direction", "name": "Test", "target": "100"},
             {"id": "t", "kind": "team", "direction_id": "d", "name": "T", "monthly_cost": "-7.5"}]
    for confidence, amount in [("high", "60"), ("medium", "30"), ("low", "30")]:
        items.append({"id": confidence, "kind": "topic", "direction_id": "d", "name": confidence,
                      "confidence": confidence, "amount": amount, "expected_date": "2027-04-01"})
    summary = model.summarize(items, model.DEFAULT_CONFIDENCE)[0]["summary"]
    assert summary["balance"] == [-30, 0, 30]
    items[0]["target"] = "9000"
    assert model.summarize(items, model.DEFAULT_CONFIDENCE)[0]["summary"]["balance"] == [-30, 0, 30]
    items.extend([
        {"id": "r1", "kind": "decision", "direction_id": "d", "name": "Cost", "confidence": "medium", "amount": "-5", "expected_date": "2027-05-01"},
        {"id": "r2", "kind": "decision", "direction_id": "d", "name": "Return", "confidence": "low", "amount": "2", "expected_date": "2027-05-01"},
    ])
    summary = model.summarize(items, model.DEFAULT_CONFIDENCE)[0]["summary"]
    assert summary["costs"] == [-90, -5, 0]
    assert summary["revenue"] == [60, 30, 32]
    assert summary["revenue_total"] == [60, 90, 122]
    assert summary["balance"] == [-30, -5, 27]


def test_positive_decisions_are_revenue_in_company_filters_and_export(environment):
    from openpyxl import load_workbook
    client, _, _ = environment
    directions = snapshot(client)["directions"]
    for index, (direction, confidence, amount) in enumerate([
        (directions[0], "high", "3"), (directions[-1], "high", "4"),
        (directions[0], "fact", "2"), (directions[0], "medium", "5"),
        (directions[0], "low", "6"), (directions[0], "none", "100"),
    ]):
        response = client.post("/api/year-model/items", json={"year": 2027, "kind": "decision", "data": {
            "name": f"Income {index}", "direction_id": direction["id"], "confidence": confidence,
            "amount": amount, "expected_date": "2027-06-01",
        }})
        assert response.status_code == 200, response.text
    summary = snapshot(client)["directions"][-1]["summary"]
    assert summary["costs"] == [-80, 0, 0]
    assert summary["revenue"] == [41.34, 20, 13]
    assert summary["revenue_total"] == [41.34, 61.34, 74.34]
    assert summary["balance"] == [-38.66, -18.66, -5.66]
    assert snapshot(client, confidence="fact")["directions"][-1]["summary"]["revenue"] == [14.34, 0, 0]
    assert snapshot(client, confidence="none")["directions"][-1]["summary"]["revenue"] == [0, 0, 0]
    book = load_workbook(BytesIO(client.get("/api/year-model/export?year=2027").content))
    rows = list(book["Резюме"].values)[2:]
    assert all(row[1] != "Решения (+)" for row in rows)
    assert next(row[2:] for row in rows if row[:2] == ("Компания", "Выручка")) == (41.34, 20, 13)
    assert next(row[2:] for row in rows if row[:2] == ("Компания", "Выручка (нар. итог)")) == (41.34, 61.34, 74.34)
    assert rows[-1][2:] == (-38.66, -18.66, -5.66)


@pytest.mark.parametrize("confidence,revenue,balance", [
    ("fact", [12.34, 0, 0], [-59.66, -59.66, -59.66]),
    ("medium", [0, 15, 0], [-72, -57, -57]),
    ("", [0, 0, 0], [-72, -72, -72]),
    ("none", [0, 0, 0], [-72, -72, -72]),
])
def test_confidence_filters_events_and_totals(environment, confidence, revenue, balance):
    data = snapshot(environment[0], confidence=confidence)
    company = data["directions"][-1]
    assert company["summary"]["revenue"] == revenue
    assert company["summary"]["revenue_total"] == [round(sum(revenue[:i+1]), 6) for i in range(3)]
    assert company["summary"]["balance"] == balance
    assert all(event["confidence"] in confidence.split(",") for direction in data["directions"] for event in direction["events"])


def test_timestamp_snapshot_history_updates_and_deletion(environment):
    client, _, engine = environment
    data = snapshot(client)
    topic = next(item for item in data["items"] if item["kind"] == "topic")
    with engine.begin() as conn:
        conn.execute(update(store.versions).values(recorded_at=datetime(2026, 1, 1, tzinfo=UTC)))
    payload = {"year": 2027, "kind": "topic", "revision": topic["revision"], "data": {**topic, "name": "Новое имя", "amount": "99,25", "expected_date": "2027-06-15"}}
    result = client.put(f'/api/year-model/items/{topic["id"]}', json=payload)
    assert result.status_code == 200, result.text
    # Latest revision is before midnight in Samara, despite being on a different UTC day.
    with engine.begin() as conn:
        conn.execute(update(store.versions).where(store.versions.c.entity_id == topic["id"], store.versions.c.revision == 2).values(recorded_at=datetime(2026, 2, 1, 20, 1, tzinfo=UTC)))
    before = snapshot(client, as_of="2026-02-01")
    after = snapshot(client, as_of="2026-02-02")
    assert next(item for item in before["items"] if item["id"] == topic["id"])["name"] == topic["name"]
    assert next(item for item in after["items"] if item["id"] == topic["id"])["name"] == "Новое имя"
    assert len(client.get(f'/api/year-model/items/{topic["id"]}/history?as_of=2026-02-01').json()["versions"]) == 1
    assert client.put(f'/api/year-model/items/{topic["id"]}', json=payload).status_code == 409
    payload["revision"] = 2
    assert client.request("DELETE", f'/api/year-model/items/{topic["id"]}', json=payload).status_code == 200
    assert not any(item["id"] == topic["id"] for item in snapshot(client)["items"])
    assert any(item["id"] == topic["id"] for item in snapshot(client, as_of="2026-02-02")["items"])


@pytest.mark.parametrize("roles,allowed", [(["Admin"],True),(["Finance"],True),(["User"],False),([],False)])
def test_access_on_page_api_history_export_and_writes(environment, roles, allowed):
    client, user, _ = environment
    user["roles"] = roles
    links = app_module._buildIndexQuickLinks(user)
    assert ("Модель года" in links) is allowed
    if "Admin" in roles:
        assert links.index("Модель года") < links.index("Администрирование")
    response = client.get("/year-model")
    assert response.status_code == (200 if allowed else 403)
    response = client.get("/api/year-model?year=2027")
    assert response.status_code == (200 if allowed else 403)
    if allowed:
        item_id = response.json()["items"][0]["id"]
        assert client.get(f"/api/year-model/items/{item_id}/history").status_code == 200
        assert client.get("/api/year-model/export?year=2027").status_code == 200
    else:
        for path in ["/api/year-model/items/unknown/history", "/api/year-model/export", "/api/year-model/view-settings"]:
            assert client.get(path).status_code == 403
        assert client.put("/api/year-model/view-settings", json={"selected_year": 2027}).status_code == 403
        payload = {"year": 2027, "kind": "direction", "data": {"name": "Forbidden"}}
        assert client.post("/api/year-model/items", json=payload).status_code == 403
        assert client.put("/api/year-model/items/unknown", json=payload).status_code == 403
        assert client.request("DELETE", "/api/year-model/items/unknown", json=payload).status_code == 403
        assert client.post("/api/year-model/import", json=sample_import()).status_code == 403


def test_unauthenticated_and_cross_origin_requests(environment, monkeypatch):
    client, _, _ = environment
    payload = {"year": 2027, "kind": "direction", "data": {"name": "Unexpected"}}
    assert client.post("/api/year-model/items", json=payload, headers={"Origin": "https://other.test"}).status_code == 403
    monkeypatch.setattr(app_module, "_getCurrentUser", lambda request: None)
    assert client.get("/api/year-model").status_code == 401


def test_company_constraints_and_parent_integrity(environment):
    client, _, _ = environment
    data = snapshot(client)
    company = data["directions"][-1]
    first = data["directions"][0]
    payload = {"year": 2027, "kind": "team", "data": {"name": "Invalid", "direction_id": company["id"], "monthly_cost": "-1"}}
    assert client.post("/api/year-model/items", json=payload).status_code == 422
    payload = {"year": 2027, "kind": "direction", "revision": company["revision"], "data": {"name": "Renamed", "target": 0}}
    assert client.put(f'/api/year-model/items/{company["id"]}', json=payload).status_code == 422
    assert client.request("DELETE", f'/api/year-model/items/{company["id"]}', json=payload).status_code == 422
    payload["revision"] = first["revision"]
    assert client.request("DELETE", f'/api/year-model/items/{first["id"]}', json=payload).status_code == 422
    payload = {"year": 2028, "kind": "team", "data": {"name": "Cross year", "direction_id": first["id"], "monthly_cost": "-1"}}
    assert client.post("/api/year-model/items", json=payload).status_code == 422


@pytest.mark.parametrize("change", [{"amount":"NaN"}, {"amount":"Infinity"}, {"amount":"-1"}, {"url":"javascript:alert(1)"}, {"confidence":"unknown"}, {"expected_date":"2028-01-01"}, {"name":" "}])
def test_invalid_topic_values_rejected(environment, change):
    client, _, _ = environment
    direction = snapshot(client)["directions"][0]
    payload = {"year":2027,"kind":"topic","data":{"name":"Topic","direction_id":direction["id"],"amount":"1","confidence":"high","expected_date":"2027-02-01",**change}}
    assert client.post("/api/year-model/items", json=payload).status_code == 422


def test_create_new_year_and_version_team_and_direction(environment):
    client, _, _ = environment
    assert snapshot(client, year=2028)["directions"] == []
    payload={"year":2028,"kind":"direction","data":{"name":"Новое направление","target":"10"}}
    response=client.post("/api/year-model/items",json=payload)
    assert response.status_code == 200
    data=snapshot(client,year=2028)
    assert [d["name"] for d in data["directions"]] == ["Новое направление","Компания"]
    direction=data["directions"][0]
    payload={"year":2028,"kind":"team","data":{"name":"Team","direction_id":direction["id"],"monthly_cost":"-0,5"}}
    response=client.post("/api/year-model/items",json=payload)
    assert response.status_code == 200
    team_id=response.json()["id"]
    payload["revision"]=1
    payload["data"]["monthly_cost"]="-1.25"
    assert client.put(f"/api/year-model/items/{team_id}",json=payload).status_code == 200
    assert snapshot(client,year=2028)["directions"][-1]["summary"]["costs"] == [-15,0,0]
    assert len(client.get(f"/api/year-model/items/{team_id}/history").json()["versions"]) == 2


def test_defaults_empty_filters_and_export(environment):
    from openpyxl import load_workbook
    client, _, _ = environment
    data = client.get("/api/year-model").json()
    assert data["year"] == model.getSnapshotBusinessDate().year
    assert data["as_of"] == model.getSnapshotBusinessDate().isoformat()
    assert data["confidence"] == model.DEFAULT_CONFIDENCE
    assert client.get("/api/year-model?confidence=invalid").status_code == 422
    assert client.get("/api/year-model?year=100").status_code == 422
    response=client.get("/api/year-model/export?year=2027&confidence=fact")
    book=load_workbook(BytesIO(response.content))
    assert book.sheetnames == ["Резюме","Темы и решения","Команды","Направления"]
    assert book["Темы и решения"].max_row == 2
    assert book["Темы и решения"]["E2"].value == 12.34
    assert book["Команды"].max_row == 4
    assert book["Резюме"].freeze_panes == "C3"
    assert list(book["Резюме"].values)[-1][2:] == (-59.66, -59.66, -59.66)
    summary_rows = list(book["Резюме"].values)[2:]
    for row in summary_rows:
        if row[1] in {"Цель по выручке", "Затраты"}:
            assert row[3:] == (None, None)
    assert next(row[2:] for row in summary_rows if row[:2] == ("Компания", "Выручка (нар. итог)")) == (12.34, 12.34, 12.34)
    assert [row[14] for row in list(book["Команды"].values)[1:]] == [-24, -36, -12]


@pytest.mark.parametrize("kind,amount", [("topic", "12,3456"), ("decision", "-2,75")])
def test_event_defaults_to_month_and_preserves_comma_amount(environment, kind, amount):
    client, _, _ = environment
    direction = snapshot(client)["directions"][0]
    payload = {"year": 2027, "kind": kind, "data": {
        "name": "New event", "direction_id": direction["id"], "amount": amount,
        "confidence": "medium", "expected_date": "2027-07-01",
    }}
    response = client.post("/api/year-model/items", json=payload)
    assert response.status_code == 200, response.text
    item_id = response.json()["id"]
    item = next(item for item in snapshot(client)["items"] if item["id"] == item_id)
    assert item["date_precision"] == "month"
    assert item["expected_date"] == "2027-07-01"
    assert item["amount"] == amount.replace(",", ".")
    payload.update(revision=item["revision"])
    payload["data"].update(date_precision="day", expected_date="2027-07-17")
    assert client.put(f"/api/year-model/items/{item_id}", json=payload).status_code == 200
    item = next(item for item in snapshot(client)["items"] if item["id"] == item_id)
    assert item["date_precision"] == "day"
    assert item["expected_date"] == "2027-07-17"
    if kind == "decision":
        from openpyxl import load_workbook
        book = load_workbook(BytesIO(client.get("/api/year-model/export?year=2027").content))
        costs = next(row for row in book["Резюме"].values if row[:2] == (direction["name"], "Затраты"))
        assert costs[3:] == (-2.75, None)


def test_initial_import_rolls_back_invalid_batch(environment):
    client, _, _ = environment
    payload = sample_import()
    payload["year"] = 2029
    # Event dates do not belong to 2029, so the complete import must roll back.
    assert client.post("/api/year-model/import", json=payload).status_code == 422
    assert snapshot(client, year=2029)["items"] == []


def test_view_settings_survive_reopening_and_are_private_per_user(environment):
    client, user, _ = environment
    ids = [direction["id"] for direction in snapshot(client)["directions"]]
    assert client.get("/api/year-model/view-settings").json() == {"selected_year": None, "collapsed_directions": []}
    user["roles"] = ["Finance"]
    assert client.put("/api/year-model/view-settings", json={"selected_year": 2027}).status_code == 200
    for direction_id in ids[:2]:
        assert client.put("/api/year-model/view-settings", json={"direction_id": direction_id, "collapsed": True}).status_code == 200
    client.put("/api/year-model/view-settings", json={"selected_year": 2028})
    client.put("/api/year-model/view-settings", json={"direction_id": ids[0], "collapsed": False})
    store._ready_engine = None
    reopened = TestClient(app_module.app)
    expected = {"selected_year": 2028, "collapsed_directions": [ids[1]]}
    assert reopened.get("/api/year-model/view-settings").json() == expected
    user["login"] = "another-user"
    assert reopened.get("/api/year-model/view-settings").json() == {"selected_year": None, "collapsed_directions": []}
    reopened.put("/api/year-model/view-settings", json={"selected_year": 2030})
    user["login"] = "planner"
    assert reopened.get("/api/year-model/view-settings").json() == expected


def test_view_settings_validation_does_not_overwrite_saved_state(environment):
    client, _, _ = environment
    client.put("/api/year-model/view-settings", json={"selected_year": 2027})
    for patch in [{}, {"selected_year": 1999}, {"collapsed": True}, {"direction_id": "missing"}, {"user_login": "someone", "selected_year": 2028}]:
        assert client.put("/api/year-model/view-settings", json=patch).status_code == 422
    team = next(item for item in snapshot(client)["items"] if item["kind"] == "team")
    assert client.put("/api/year-model/view-settings", json={"direction_id": team["id"], "collapsed": True}).status_code == 404
    assert client.get("/api/year-model/view-settings").json()["selected_year"] == 2027


def test_team_month_edits_totals_history_and_metadata(environment):
    from openpyxl import load_workbook
    client, _, engine = environment
    team = next(item for item in snapshot(client)["items"] if item["kind"] == "team" and item["name"] == "Team 0")
    # Simulate an original stored version that only has the flat monthly amount.
    legacy = {key: value for key, value in team.items() if key not in {
        "id", "kind", "year", "revision", "recorded_at", "actor", "monthly_costs",
    }}
    with engine.begin() as conn:
        conn.execute(update(store.versions).where(store.versions.c.entity_id == team["id"]).values(
            data=legacy, recorded_at=datetime(2026, 1, 1, tzinfo=UTC)))
    payload = {"year": 2027, "kind": "team", "revision": 1, "data": {
        "name": team["name"], "direction_id": team["direction_id"],
        "monthly_cost": "-1,25", "cost_month": 7, "copy_forward": True,
    }}
    url = f'/api/year-model/items/{team["id"]}'
    assert client.put(url, json=payload).status_code == 200
    assert client.put(url, json=payload).status_code == 409
    current = snapshot(client)
    alpha = current["directions"][0]
    changed = next(item for item in alpha["teams"] if item["id"] == team["id"])
    assert changed["monthly_costs"] == [-2] * 6 + [-1.25] * 6
    assert changed["annual_cost"] == -19.5
    assert alpha["monthly_costs"] == [-5] * 6 + [-4.25] * 6
    assert current["directions"][-1]["summary"]["costs"] == [-75.5, 0, 0]
    payload["revision"] = 2
    payload["data"].update(cost_month=8, monthly_cost="-0,5", copy_forward=False)
    assert client.put(url, json=payload).status_code == 200
    payload["revision"] = 3
    payload["data"] = {"name": "Renamed team", "direction_id": team["direction_id"], "note": "Keep the schedule"}
    assert client.put(url, json=payload).status_code == 200
    updated = next(item for item in snapshot(client)["directions"][0]["teams"] if item["id"] == team["id"])
    expected = [-2] * 6 + [-1.25, -0.5] + [-1.25] * 4
    assert updated["monthly_costs"] == expected
    assert updated["annual_cost"] == -18.75
    old = next(item for item in snapshot(client, as_of="2026-01-02")["items"] if item["id"] == team["id"])
    assert "monthly_costs" not in old
    assert model.team_costs(old) == [model.money(-2)] * 12
    history = client.get(url + "/history").json()["versions"]
    assert len(history) == 4
    assert history[1]["monthly_costs"] == history[0]["monthly_costs"]
    book = load_workbook(BytesIO(client.get("/api/year-model/export?year=2027").content))
    row = next(row for row in book["Команды"].values if row[1] == "Renamed team")
    assert row[2:14] == tuple(expected)
    assert row[14] == -18.75


@pytest.mark.parametrize("month,copy_forward,expected", [
    (1, True, [-0.25] * 12), (12, True, [0] * 11 + [-0.25]),
    (5, False, [0] * 4 + [-0.25] + [0] * 7),
])
def test_new_team_starts_in_selected_month(environment, month, copy_forward, expected):
    client, _, _ = environment
    direction = snapshot(client)["directions"][0]
    response = client.post("/api/year-model/items", json={"year": 2027, "kind": "team", "data": {
        "name": "New team", "direction_id": direction["id"], "monthly_cost": "-0,25",
        "cost_month": month, "copy_forward": copy_forward,
    }})
    assert response.status_code == 200, response.text
    team = next(item for item in snapshot(client)["directions"][0]["teams"] if item["id"] == response.json()["id"])
    assert team["monthly_costs"] == expected
    assert team["annual_cost"] == sum(expected)


@pytest.mark.parametrize("change", [
    {"cost_month": 0}, {"cost_month": 13}, {"cost_month": 1.5},
    {"cost_month": True}, {"copy_forward": "false"}, {"monthly_cost": "0,1"},
    {"monthly_cost": "NaN"},
])
def test_invalid_monthly_cost_edits_do_not_create_versions(environment, change):
    client, _, _ = environment
    team = next(item for item in snapshot(client)["items"] if item["kind"] == "team")
    response = client.put(f'/api/year-model/items/{team["id"]}', json={
        "year": 2027, "kind": "team", "revision": team["revision"],
        "data": {**team, "cost_month": 7, "copy_forward": True, "monthly_cost": "-1", **change},
    })
    assert response.status_code == 422
    assert len(client.get(f'/api/year-model/items/{team["id"]}/history').json()["versions"]) == 1


@pytest.mark.parametrize("kind", ["topic", "decision"])
def test_attention_default_toggle_history_and_export(environment, kind):
    from openpyxl import load_workbook
    client, _, _ = environment
    item = next(item for item in snapshot(client)["items"] if item["kind"] == kind)
    assert item["attention"] is False
    url = f'/api/year-model/items/{item["id"]}'
    payload = {"year": 2027, "kind": kind, "revision": 1, "data": {**item, "attention": True}}
    assert client.put(url, json=payload).status_code == 200
    book = load_workbook(BytesIO(client.get("/api/year-model/export?year=2027").content))
    assert next(row[8] for row in book["Темы и решения"].values if row[2] == item["name"]) == "Да"
    payload["revision"] = 2
    payload["data"]["attention"] = False
    assert client.put(url, json=payload).status_code == 200
    assert [row["attention"] for row in client.get(url + "/history").json()["versions"]] == [False, True, False]
    payload["revision"] = 3
    payload["data"]["attention"] = "false"
    assert client.put(url, json=payload).status_code == 422


@pytest.mark.parametrize("entity", ["direction", "team", "company"])
def test_direction_and_team_comment_attention_history_and_export(environment, entity):
    from openpyxl import load_workbook
    client, _, engine = environment
    data = snapshot(client)
    item = next(item for item in data["items"] if (
        item.get("company") if entity == "company" else item["kind"] == entity and not item.get("company")
    ))
    assert item["attention"] is False
    assert item["note"] == ""
    original_totals = data["directions"][-1]["summary"]
    with engine.begin() as conn:
        conn.execute(update(store.versions).values(recorded_at=datetime(2026, 1, 1, tzinfo=UTC)))
    comment = '=SUM(1,2)\nПроверить "план" <комментарий>'
    url = f'/api/year-model/items/{item["id"]}'
    payload = {"year": 2027, "kind": item["kind"], "revision": 1,
               "data": {**item, "note": comment, "attention": True}}
    assert client.put(url, json=payload).status_code == 200
    current = snapshot(client)
    assert current["directions"][-1]["summary"] == original_totals
    before = next(row for row in snapshot(client, as_of="2026-01-02")["items"] if row["id"] == item["id"])
    assert before["note"] == ""
    assert before["attention"] is False
    book = load_workbook(BytesIO(client.get("/api/year-model/export?year=2027").content))
    sheet = book["Команды" if entity == "team" else "Направления"]
    name_index, note_index = (1, 15) if entity == "team" else (0, 2)
    row = next(row for row in sheet if row[name_index].value == item["name"])
    assert row[note_index].value == comment
    assert row[note_index].data_type == "s"
    assert row[note_index + 1].value == "Да"
    # Saving costs or other properties without these fields preserves both values.
    payload["revision"] = 2
    payload["data"] = {key: value for key, value in item.items() if key not in {"note", "attention"}}
    if entity == "team":
        payload["data"].update(monthly_cost="-0,5", cost_month=10, copy_forward=False)
    assert client.put(url, json=payload).status_code == 200
    changed = next(row for row in snapshot(client)["items"] if row["id"] == item["id"])
    assert changed["note"] == comment
    assert changed["attention"] is True
    payload.update(revision=3, data={**changed, "note": "", "attention": False})
    assert client.put(url, json=payload).status_code == 200
    history = client.get(url + "/history").json()["versions"]
    assert [row["attention"] for row in history] == [False, True, True, False]
    assert [row["note"] for row in history] == ["", comment, comment, ""]
    for change in [{"attention": "false"}, {"note": "a" * 10001}]:
        payload.update(revision=4, data={**changed, **change})
        assert client.put(url, json=payload).status_code == 422
    assert len(client.get(url + "/history").json()["versions"]) == 4
