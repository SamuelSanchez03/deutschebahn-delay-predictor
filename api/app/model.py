"""Carga del modelo y construcción de features, idéntica a la del entrenamiento (databricks/06_gold, 07_train)."""
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

import holidays
import lightgbm as lgb
import numpy as np

MODEL_DIR = Path(__file__).resolve().parent.parent / "model"


def spark_dow(d: date) -> int:
    """Día de la semana con la convención de Spark `dayofweek`: 1 = domingo … 7 = sábado."""
    return d.isoweekday() % 7 + 1


@lru_cache(maxsize=32)
def state_holidays(state_code: str, year: int) -> holidays.HolidayBase:
    return holidays.Germany(subdiv=state_code, years=year)


@dataclass
class Prediction:
    probability: float
    features: dict


class DelayModel:
    def __init__(self, model_dir: Path = MODEL_DIR):
        with gzip.open(model_dir / "model.txt.gz", "rt") as fh:
            self.booster = lgb.Booster(model_str=fh.read())
        self.meta = json.loads((model_dir / "model_meta.json").read_text())
        self.lookups = json.loads((model_dir / "lookups.json").read_text())
        self.features: list[str] = self.meta["features"]
        self.categorical: list[str] = self.meta["categorical"]
        # Vocabulario → código entero (igual que en 07_train); un valor desconocido se codifica -1 (faltante)
        self.codes = {c: {v: i for i, v in enumerate(vals)} for c, vals in self.meta["vocab"].items()}
        self.threshold: float = self.meta["threshold"]
        self.global_rate: float = self.lookups["global_rate"]
        self.as_of = date.fromisoformat(self.lookups["as_of"])

    # --- consultas -------------------------------------------------------------------------------------------
    def station(self, eva: str) -> dict | None:
        return self.lookups["stations"].get(eva)

    def routes(self, eva: str) -> dict:
        return self.lookups["routes"].get(eva, {})

    def route(self, eva: str, route_key: str) -> dict | None:
        return self.routes(eva).get(route_key)

    # --- features --------------------------------------------------------------------------------------------
    def build_features(self, eva: str, route_key: str, day: date, hour: int) -> dict:
        st = self.station(eva)
        rt = self.route(eva, route_key)
        if st is None or rt is None:
            raise KeyError("estación o línea desconocida")
        dow = spark_dow(day)
        eva_rates = self.lookups["eva_rates"].get(eva, {})
        g = self.global_rate
        return {
            "eva": eva,
            "state_code": st["state_code"],
            "train_category": rt["train_category"],
            "train_type_grp": rt["train_type_grp"],
            "hour": hour,
            "dow": dow,
            "month": day.month,
            "is_weekend": int(dow in (1, 7)),
            "is_holiday": int(day in state_holidays(st["state_code"], day.year)),
            "stop_seq": rt["stop_seq"],
            "eva_rate_7d": eva_rates.get("eva_rate_7d") or g,
            "eva_rate_28d": eva_rates.get("eva_rate_28d") or g,
            "eva_hour_rate_28d": self.lookups["eva_hour_rates"].get(f"{eva}|{hour}", g),
            "route_rate_7d": rt.get("route_rate_7d") or g,
            "route_rate_28d": rt.get("route_rate_28d") or g,
            "route_n_28d": rt.get("route_n_28d") or 0,
        }

    def encode(self, rows: list[dict]) -> np.ndarray:
        X = np.empty((len(rows), len(self.features)), dtype=np.float64)
        for i, row in enumerate(rows):
            for j, f in enumerate(self.features):
                v = row[f]
                X[i, j] = self.codes[f].get(str(v), -1) if f in self.categorical else float(v)
        return X

    def predict(self, eva: str, route_key: str, day: date, hours: list[int]) -> list[Prediction]:
        rows = [self.build_features(eva, route_key, day, h) for h in hours]
        probs = self.booster.predict(self.encode(rows))
        return [Prediction(float(p), r) for p, r in zip(probs, rows)]
