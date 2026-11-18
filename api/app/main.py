"""API REST del predictor de retrasos de Deutsche Bahn.

Documentación interactiva (OpenAPI / Swagger) en /docs y especificación en /openapi.json.
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
import datetime as dt
from datetime import timedelta
from typing import Literal

from fastapi import FastAPI, HTTPException, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .model import DelayModel

MAX_DAYS_AHEAD = int(os.environ.get("MAX_DAYS_AHEAD", "180"))
ORIGINS = [o.strip() for o in os.environ.get(
    "ALLOWED_ORIGINS", "https://samuelsanchez03.github.io,http://localhost:8000,http://127.0.0.1:8000").split(",")]

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    t0 = time.time()
    state["model"] = DelayModel()
    state["load_seconds"] = round(time.time() - t0, 2)
    yield


app = FastAPI(
    title="Predictor de retrasos · Deutsche Bahn",
    version="1.0.0",
    description=(
        "Estima la probabilidad de que un tren llegue a una estación con **más de 5 minutos de retraso**.\n\n"
        "Modelo LightGBM entrenado en Databricks con ~45 M de paradas (2024-08 → 2026-08) de las 131 estaciones "
        "principales de Alemania. AUC-PR en prueba: 0.537 (baseline: 0.407).\n\n"
        "Flujo típico: `GET /stations` → `GET /stations/{eva}/routes` → `POST /predict`."),
    lifespan=lifespan,
    contact={"name": "db-delay-predictor", "url": "https://github.com/SamuelSanchez03/deutschebahn-delay-predictor"},
    license_info={"name": "Datos: CC BY 4.0 (Deutsche Bahn vía piebro/deutsche-bahn-data)"},
)
app.add_middleware(CORSMiddleware, allow_origins=ORIGINS, allow_methods=["GET", "POST"], allow_headers=["*"])


def model() -> DelayModel:
    return state["model"]


# --- Esquemas ------------------------------------------------------------------------------------------------

EVA = Field(pattern=r"^\d{6,8}$", description="Número EVA de la estación (ver `GET /stations`)",
            examples=["8000105"])


class Station(BaseModel):
    eva: str
    name: str
    lat: float
    lon: float
    state_code: str = Field(description="Estado federal (código ISO 3166-2:DE sin prefijo)")


class Route(BaseModel):
    route_key: str = Field(description="Línea (p. ej. `RE5`, `S3`) o tren de larga distancia (`ICE 603`)")
    train_type: str
    train_category: Literal["Larga distancia", "Regional", "S-Bahn"]
    destination: str | None
    hours: list[int] = Field(description="Horas en las que esta línea pasó por la estación en las últimas 4 semanas")
    stops_28d: int = Field(description="Paradas observadas en las últimas 4 semanas")


def validate_known(eva: str, route_key: str, day: dt.date) -> None:
    """Validación de negocio compartida: estación y línea conocidas y fecha dentro del horizonte del modelo."""
    m = model()
    if m.station(eva) is None:
        raise ValueError(f"estación desconocida: {eva}. Consulta GET /stations")
    if m.route(eva, route_key) is None:
        raise ValueError(f"la línea '{route_key}' no pasa por la estación {eva}. "
                         f"Consulta GET /stations/{eva}/routes")
    first, last = m.as_of + timedelta(days=1), m.as_of + timedelta(days=MAX_DAYS_AHEAD)
    if not first <= day <= last:
        raise ValueError(f"la fecha debe estar entre {first} y {last} (datos hasta {m.as_of})")


class PredictRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"eva": "8000105", "route_key": "ICE 1001", "date": "2026-11-20", "hour": 8}]})
    eva: str = EVA
    route_key: str = Field(min_length=1, max_length=40, description="Línea o tren (ver `GET /stations/{eva}/routes`)")
    date: dt.date = Field(description="Fecha del viaje (YYYY-MM-DD)")
    hour: int = Field(ge=0, le=23, description="Hora planeada de llegada/salida (0–23, hora local alemana)")

    @model_validator(mode="after")
    def check_known(self):
        validate_known(self.eva, self.route_key, self.date)
        return self


class PredictResponse(BaseModel):
    eva: str
    station: str
    route_key: str
    train_type: str
    date: dt.date
    hour: int
    probability: float = Field(ge=0, le=1, description="Probabilidad de llegar con más de 5 min de retraso")
    delayed: bool = Field(description="`probability >= threshold` (umbral que maximiza F1 en validación)")
    threshold: float
    risk: Literal["bajo", "medio", "alto"]
    usual_service: bool = Field(description="Si la línea suele pasar por la estación a esa hora")
    is_holiday: bool
    data_as_of: dt.date
    model_version: str


class HourlyResponse(BaseModel):
    eva: str
    route_key: str
    date: dt.date
    threshold: float
    hours: list[dict] = Field(description="`[{hour, probability, delayed}]` para las horas habituales de la línea")


class Health(BaseModel):
    status: Literal["ok"]
    model_version: str
    data_as_of: dt.date
    stations: int
    station_routes: int
    model_load_seconds: float


# --- Errores: siempre JSON con el mismo formato ---------------------------------------------------------------

@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "error interno del servidor"})


def risk_level(p: float, thr: float) -> str:
    return "alto" if p >= thr + 0.15 else "medio" if p >= thr else "bajo"


# --- Endpoints -----------------------------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", response_model=Health, tags=["estado"])
def health():
    """Estado del servicio y versión del modelo. Útil para *health checks* y para despertar el servicio."""
    m = model()
    return Health(status="ok", model_version=app.version, data_as_of=m.as_of,
                  stations=len(m.lookups["stations"]),
                  station_routes=sum(len(v) for v in m.lookups["routes"].values()),
                  model_load_seconds=state["load_seconds"])


@app.get("/stations", response_model=list[Station], tags=["catálogo"])
def stations():
    """Las 131 estaciones que cubre el modelo."""
    return [Station(eva=k, **v) for k, v in model().lookups["stations"].items()]


@app.get("/stations/{eva}/routes", response_model=list[Route], tags=["catálogo"],
         responses={404: {"description": "Estación desconocida"}})
def routes(eva: str = Path(pattern=r"^\d{6,8}$", examples=["8000105"])):
    """Líneas y trenes que pasaron por la estación en las últimas 4 semanas, de más a menos frecuentes."""
    m = model()
    if m.station(eva) is None:
        raise HTTPException(404, f"estación desconocida: {eva}")
    return [Route(route_key=k, train_type=r["train_type"], train_category=r["train_category"],
                  destination=r["destination"], hours=r["hours"], stops_28d=r["n_28d"])
            for k, r in m.routes(eva).items()]


@app.post("/predict", response_model=PredictResponse, tags=["predicción"],
          responses={422: {"description": "Entrada inválida (estación, línea, fecha u hora)"}})
def predict(req: PredictRequest):
    """Probabilidad de que un tren de la línea `route_key` llegue a la estación `eva` con más de 5 min de retraso."""
    m = model()
    p = m.predict(req.eva, req.route_key, req.date, [req.hour])[0]
    rt = m.route(req.eva, req.route_key)
    return PredictResponse(
        eva=req.eva, station=m.station(req.eva)["name"], route_key=req.route_key, train_type=rt["train_type"],
        date=req.date, hour=req.hour, probability=round(p.probability, 4),
        delayed=p.probability >= m.threshold, threshold=m.threshold, risk=risk_level(p.probability, m.threshold),
        usual_service=req.hour in rt["hours"], is_holiday=bool(p.features["is_holiday"]),
        data_as_of=m.as_of, model_version=app.version)


class HourlyRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={
        "examples": [{"eva": "8000105", "route_key": "ICE 1001", "date": "2026-11-20"}]})
    eva: str = EVA
    route_key: str = Field(min_length=1, max_length=40)
    date: dt.date

    @model_validator(mode="after")
    def check_known(self):
        validate_known(self.eva, self.route_key, self.date)
        return self


@app.post("/predict/hourly", response_model=HourlyResponse, tags=["predicción"],
          responses={422: {"description": "Entrada inválida"}})
def predict_hourly(req: HourlyRequest):
    """Probabilidad de retraso para cada hora en la que la línea suele pasar por la estación (para graficar)."""
    m = model()
    hours = m.route(req.eva, req.route_key)["hours"]
    preds = m.predict(req.eva, req.route_key, req.date, hours)
    return HourlyResponse(eva=req.eva, route_key=req.route_key, date=req.date, threshold=m.threshold,
                          hours=[{"hour": h, "probability": round(p.probability, 4),
                                  "delayed": p.probability >= m.threshold} for h, p in zip(hours, preds)])
