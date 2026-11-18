"""Pruebas de la API: respuestas válidas y validación de entradas.

Ejecutar desde api/:  python -m pytest -q
"""
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.model import spark_dow


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:          # el 'with' ejecuta el lifespan (carga del modelo)
        yield c


@pytest.fixture(scope="module")
def example(client):
    """Una estación, su línea más frecuente y una fecha válida."""
    eva = "8000105"                      # Frankfurt (Main) Hbf
    route = client.get(f"/stations/{eva}/routes").json()[0]
    as_of = date.fromisoformat(client.get("/health").json()["data_as_of"])
    return {"eva": eva, "route_key": route["route_key"], "date": str(as_of + timedelta(days=20)),
            "hour": route["hours"][len(route["hours"]) // 2]}


def test_dow_matches_spark_convention():
    assert spark_dow(date(2026, 11, 15)) == 1     # domingo
    assert spark_dow(date(2026, 11, 16)) == 2     # lunes
    assert spark_dow(date(2026, 11, 21)) == 7     # sábado


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok" and r.json()["stations"] == 131


def test_openapi_documented(client):
    spec = client.get("/openapi.json").json()
    assert {"/predict", "/predict/hourly", "/stations", "/health"} <= set(spec["paths"])


def test_stations_and_routes(client):
    stations = client.get("/stations").json()
    assert len(stations) == 131 and {"eva", "name", "lat", "lon"} <= set(stations[0])
    routes = client.get("/stations/8000105/routes").json()
    assert len(routes) > 10 and routes[0]["stops_28d"] >= routes[-1]["stops_28d"]


def test_predict_ok(client, example):
    r = client.post("/predict", json=example)
    assert r.status_code == 200, r.text
    body = r.json()
    assert 0 <= body["probability"] <= 1
    assert body["delayed"] == (body["probability"] >= body["threshold"])
    assert body["risk"] in {"bajo", "medio", "alto"}


def test_predict_hourly(client, example):
    req = {k: example[k] for k in ("eva", "route_key", "date")}
    r = client.post("/predict/hourly", json=req)
    assert r.status_code == 200, r.text
    assert len(r.json()["hours"]) > 0


def test_long_distance_riskier_than_sbahn(client, example):
    """Sanity check: en promedio un ICE/IC debe tener más riesgo que un S-Bahn en la misma estación."""
    routes = client.get("/stations/8000105/routes").json()
    def avg(cat):
        rs = [r for r in routes if r["train_category"] == cat][:5]
        ps = [client.post("/predict", json={**example, "route_key": r["route_key"], "hour": r["hours"][0]})
              .json()["probability"] for r in rs]
        return sum(ps) / len(ps)
    assert avg("Larga distancia") > avg("S-Bahn")


@pytest.mark.parametrize("change, fragment", [
    ({"eva": "abc"}, "string_pattern_mismatch"),           # formato de EVA inválido
    ({"eva": "1234567"}, "estación desconocida"),          # EVA con formato válido pero inexistente
    ({"route_key": "XYZ 999"}, "no pasa por la estación"), # línea que no pasa por ahí
    ({"date": "2020-01-01"}, "la fecha debe estar entre"), # fecha fuera del horizonte
    ({"date": "2026-13-40"}, "date"),                      # fecha mal formada
    ({"hour": 24}, "less_than_equal"),                     # hora fuera de rango
    ({"hour": "ocho"}, "int_parsing"),                     # tipo incorrecto
])
def test_predict_validation(client, example, change, fragment):
    r = client.post("/predict", json={**example, **change})
    assert r.status_code == 422
    assert fragment in r.text


def test_predict_missing_field(client, example):
    body = {k: v for k, v in example.items() if k != "hour"}
    assert client.post("/predict", json=body).status_code == 422


def test_unknown_station_routes_404(client):
    assert client.get("/stations/1234567/routes").status_code == 404
