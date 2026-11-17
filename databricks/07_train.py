# Databricks notebook source
# MAGIC %pip install lightgbm==4.5.0 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 · Entrenamiento del modelo
# MAGIC
# MAGIC **Tarea:** clasificación binaria, `label = 1` si el tren llega con más de 5 min de retraso.
# MAGIC
# MAGIC | Modelo | Rol | Dónde entrena |
# MAGIC |---|---|---|
# MAGIC | Baseline: tasa histórica de la línea (28 días) | Punto de comparación mínimo | — |
# MAGIC | Regresión logística (Spark ML) | Modelo lineal distribuido | Spark, sobre millones de filas |
# MAGIC | **LightGBM** | Modelo publicado en la API | Driver, sobre una muestra grande |
# MAGIC
# MAGIC **Split temporal**, nunca aleatorio: entrenar con el pasado y evaluar con el futuro, igual que en producción.
# MAGIC Se descarta julio de 2024 porque las tasas históricas de 28 días todavía no tienen historia completa.
# MAGIC
# MAGIC **Métrica principal: AUC-PR.** Solo ~19% de las paradas son positivas. AUC-PR mide qué tan bien se ordena la
# MAGIC clase minoritaria y no se infla con los muchos verdaderos negativos, como sí puede pasar con AUC-ROC. También se
# MAGIC reporta F1 al umbral que maximiza F1 en validación, precisión, recall y calibración.

# COMMAND ----------

import json
import os
import time

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from sklearn.metrics import (average_precision_score, f1_score, precision_recall_curve,
                             precision_score, recall_score, roc_auc_score)

# En serverless MLflow no puede leer spark.mlflow.modelRegistryUri: se fijan las URIs antes de crear el cliente
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{USER}/db-delay-experiment")
ARTIFACTS = "/Volumes/workspace/ml/artifacts/delay_classifier"
os.makedirs(ARTIFACTS, exist_ok=True)

CAT = ["eva", "state_code", "train_category", "train_type_grp"]
NUM = ["hour", "dow", "month", "is_weekend", "is_holiday", "stop_seq",
       "eva_rate_7d", "eva_rate_28d", "eva_hour_rate_28d", "route_rate_7d", "route_rate_28d", "route_n_28d"]
FEATURES = CAT + NUM

# COMMAND ----------

# MAGIC %md ## 1. Split temporal

# COMMAND ----------

df = spark.table("workspace.gold.features").filter("service_date >= '2024-08-01'")
max_date = df.agg(F.max("service_date")).first()[0]
TEST_START = (pd.Timestamp(max_date).replace(day=1)).date()                    # último mes
VAL_START = (pd.Timestamp(TEST_START) - pd.DateOffset(months=1)).date()        # penúltimo mes

train = df.filter(F.col("service_date") < F.lit(VAL_START))
val = df.filter((F.col("service_date") >= F.lit(VAL_START)) & (F.col("service_date") < F.lit(TEST_START)))
test = df.filter(F.col("service_date") >= F.lit(TEST_START))

split_info = {}
for name, d in [("train", train), ("val", val), ("test", test)]:
    r = d.agg(F.count("*").alias("n"), F.avg("label").alias("pos"),
              F.min("service_date").alias("d0"), F.max("service_date").alias("d1")).first()
    split_info[name] = {"rows": r["n"], "pos_rate": round(r["pos"], 4), "from": str(r["d0"]), "to": str(r["d1"])}
    print(f"{name:5s} {r['n']:>12,} filas · {100 * r['pos']:.1f}% positivos · {r['d0']} → {r['d1']}")

# COMMAND ----------

def metrics(y, p, threshold=None):
    """AUC-PR, AUC-ROC y, si hay umbral, F1/precisión/recall."""
    out = {"auc_pr": average_precision_score(y, p), "auc_roc": roc_auc_score(y, p)}
    if threshold is not None:
        yhat = (p >= threshold).astype(int)
        out.update(f1=f1_score(y, yhat), precision=precision_score(y, yhat), recall=recall_score(y, yhat))
    return {k: round(float(v), 4) for k, v in out.items()}

def best_f1_threshold(y, p):
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec * rec / np.clip(prec + rec, 1e-9, None)
    i = int(np.nanargmax(f1[:-1]))
    return float(thr[i])

# Validación y prueba completas en pandas (≈ 2 M filas cada una)
cols = FEATURES + ["label", "service_date"]
val_pd = val.select(*cols).toPandas()
test_pd = test.select(*cols).toPandas()
results = {}

# COMMAND ----------

# MAGIC %md ## 2. Baseline: tasa histórica de la línea

# COMMAND ----------

thr_base = best_f1_threshold(val_pd["label"], val_pd["route_rate_28d"])
results["baseline_route_rate_28d"] = metrics(test_pd["label"], test_pd["route_rate_28d"], thr_base)
with mlflow.start_run(run_name="baseline_route_rate_28d"):
    mlflow.log_params({"model": "baseline", "score": "route_rate_28d", "threshold": round(thr_base, 4)})
    mlflow.log_metrics({f"test_{k}": v for k, v in results["baseline_route_rate_28d"].items()})
print("Baseline (test):", results["baseline_route_rate_28d"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regresión logística en Spark ML (distribuida)
# MAGIC
# MAGIC Las categóricas se codifican con *one-hot* (`StringIndexer` + `OneHotEncoder`). La hora y el día de la semana también
# MAGIC van como categorías, porque su efecto no es lineal (hay picos en hora punta). Las numéricas se **estandarizan**,
# MAGIC porque la regresión logística es sensible a la escala de las variables. Se entrena con una muestra del 25% del train.

# COMMAND ----------

lr_cat = CAT + ["hour_c", "dow_c"]
lr_num = ["month", "is_weekend", "is_holiday", "stop_seq", "eva_rate_7d", "eva_rate_28d",
          "eva_hour_rate_28d", "route_rate_7d", "route_rate_28d", "route_n_28d"]

def lr_prep(d):
    return (d.withColumn("hour_c", F.col("hour").cast("string")).withColumn("dow_c", F.col("dow").cast("string"))
             .withColumn("route_n_28d", F.log1p("route_n_28d")))

stages = [StringIndexer(inputCols=lr_cat, outputCols=[f"{c}_idx" for c in lr_cat], handleInvalid="keep"),
          OneHotEncoder(inputCols=[f"{c}_idx" for c in lr_cat], outputCols=[f"{c}_oh" for c in lr_cat],
                        handleInvalid="keep"),
          VectorAssembler(inputCols=lr_num, outputCol="num_vec"),
          StandardScaler(inputCol="num_vec", outputCol="num_scaled", withMean=True, withStd=True),
          VectorAssembler(inputCols=[f"{c}_oh" for c in lr_cat] + ["num_scaled"], outputCol="features"),
          LogisticRegression(labelCol="label", featuresCol="features", regParam=1e-4, maxIter=50)]

t0 = time.time()
lr_train = lr_prep(train).sample(fraction=0.25, seed=42)
lr_model = Pipeline(stages=stages).fit(lr_train)
lr_secs = time.time() - t0

def lr_scores(d):
    return (lr_model.transform(lr_prep(d)).select("label", vector_to_array("probability")[1].alias("p"))
            .toPandas())

lr_val, lr_test = lr_scores(val), lr_scores(test)
thr_lr = best_f1_threshold(lr_val["label"], lr_val["p"])
results["spark_logistic_regression"] = metrics(lr_test["label"], lr_test["p"], thr_lr)
with mlflow.start_run(run_name="spark_logistic_regression"):
    mlflow.log_params({"model": "LogisticRegression (Spark ML)", "regParam": 1e-4, "maxIter": 50,
                       "train_fraction": 0.25, "threshold": round(thr_lr, 4)})
    mlflow.log_metrics({f"test_{k}": v for k, v in results["spark_logistic_regression"].items()})
    mlflow.log_metric("train_seconds", lr_secs)
print(f"Regresión logística (test): {results['spark_logistic_regression']} · {lr_secs / 60:.1f} min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. LightGBM (modelo publicado)
# MAGIC
# MAGIC Árboles con *gradient boosting*. Manejan interacciones no lineales (por ejemplo, larga distancia × hora punta) y
# MAGIC categorías de forma nativa. Las categorías se codifican como enteros con un **vocabulario** que se guarda junto al
# MAGIC modelo: la API usa exactamente la misma codificación, y un valor desconocido se codifica como −1, que LightGBM
# MAGIC trata como faltante. Los árboles no necesitan estandarizar las variables numéricas.

# COMMAND ----------

TRAIN_ROWS = 4_000_000
frac = min(1.0, TRAIN_ROWS / split_info["train"]["rows"])
train_pd = train.select(*cols).sample(fraction=frac, seed=42).toPandas()
print(f"Muestra de entrenamiento: {len(train_pd):,} filas ({100 * frac:.1f}% del train)")

vocab = {c: sorted(train_pd[c].dropna().astype(str).unique().tolist()) for c in CAT}

def encode(pdf):
    X = pdf[FEATURES].copy()
    for c in CAT:
        X[c] = X[c].astype(str).map({v: i for i, v in enumerate(vocab[c])}).fillna(-1).astype(int)
    return X.astype({c: "float64" for c in NUM})

X_tr, y_tr = encode(train_pd), train_pd["label"].values
X_va, y_va = encode(val_pd), val_pd["label"].values
X_te, y_te = encode(test_pd), test_pd["label"].values

GRID = [{"num_leaves": 63, "min_child_samples": 200}, {"num_leaves": 255, "min_child_samples": 500}]
best = None
for g in GRID:
    params = dict(objective="binary", metric="average_precision", learning_rate=0.05, n_estimators=2000, subsample=0.8, subsample_freq=1,
                  colsample_bytree=0.8, reg_lambda=1.0, max_cat_to_onehot=8, verbose=-1, random_state=42, **g)
    t0 = time.time()
    clf = lgb.LGBMClassifier(**params)
    clf.fit(X_tr, y_tr, eval_set=[(X_va, y_va)], categorical_feature=CAT, callbacks=[lgb.early_stopping(50, verbose=False)])
    secs = time.time() - t0
    p_va = clf.predict_proba(X_va)[:, 1]
    m_va = metrics(y_va, p_va)
    with mlflow.start_run(run_name=f"lightgbm_leaves{g['num_leaves']}") as run:
        mlflow.log_params({**params, "best_iteration": clf.best_iteration_, "train_rows": len(X_tr)})
        mlflow.log_metrics({f"val_{k}": v for k, v in m_va.items()})
        mlflow.log_metric("train_seconds", secs)
    print(f"LightGBM {g}: val {m_va} · {clf.best_iteration_} árboles · {secs / 60:.1f} min")
    if best is None or m_va["auc_pr"] > best["val"]["auc_pr"]:
        best = {"clf": clf, "params": params, "val": m_va, "run_id": run.info.run_id, "p_va": p_va}

# COMMAND ----------

# MAGIC %md ## 5. Evaluación final en test (una sola vez, con el mejor modelo de validación)

# COMMAND ----------

clf = best["clf"]
thr = best_f1_threshold(y_va, best["p_va"])
p_te = clf.predict_proba(X_te)[:, 1]
results["lightgbm"] = metrics(y_te, p_te, thr)

print(f"{'modelo':28s} {'AUC-PR':>7s} {'AUC-ROC':>8s} {'F1':>6s} {'prec':>6s} {'recall':>7s}")
for name, m in results.items():
    print(f"{name:28s} {m['auc_pr']:>7.4f} {m['auc_roc']:>8.4f} {m['f1']:>6.3f} {m['precision']:>6.3f} {m['recall']:>7.3f}")
print(f"\nTasa base (AUC-PR de un modelo aleatorio) = {y_te.mean():.4f} · umbral F1 elegido en val = {thr:.3f}")

# Calibración: cuando el modelo dice 30%, ¿se retrasa ~30%?
cal = (pd.DataFrame({"p": p_te, "y": y_te}).assign(bucket=lambda d: (d.p * 10).clip(upper=9.999).astype(int) / 10)
       .groupby("bucket").agg(n=("y", "size"), prob_media=("p", "mean"), tasa_real=("y", "mean")).round(3))
print("\nCalibración (test):\n", cal.to_string())

imp = (pd.Series(clf.booster_.feature_importance("gain"), index=FEATURES).sort_values(ascending=False))
imp = (imp / imp.sum()).round(4)
print("\nImportancia de variables (gain):\n", imp.to_string())

by_cat = (pd.DataFrame({"cat": test_pd["train_category"], "y": y_te, "p": p_te}).groupby("cat")
          .apply(lambda d: pd.Series({"n": len(d), "tasa": d.y.mean(), "auc_pr": average_precision_score(d.y, d.p)}))
          .round(4))
print("\nAUC-PR por categoría (test):\n", by_cat.to_string())

# COMMAND ----------

# MAGIC %md ## 6. Registro en MLflow (Unity Catalog) y artefactos para la API

# COMMAND ----------

meta = {"features": FEATURES, "categorical": CAT, "numeric": NUM, "vocab": vocab,
        "threshold": round(thr, 4), "label": "delay_in_min > 5", "global_rate": float(y_tr.mean()),
        "split": split_info, "metrics_test": results, "best_iteration": int(clf.best_iteration_),
        "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "mlflow_run_id": best["run_id"]}

# Los Volumes no permiten 'seek' al escribir (LightGBM lo usa): se escribe en disco local y luego se copia
import shutil, tempfile
LOCAL = tempfile.mkdtemp()
clf.booster_.save_model(f"{LOCAL}/model.txt", num_iteration=clf.best_iteration_)
with open(f"{LOCAL}/model_meta.json", "w") as fh:
    json.dump(meta, fh, indent=1, default=str)
cal.reset_index().to_csv(f"{LOCAL}/calibration_test.csv", index=False)
imp.rename("gain").to_csv(f"{LOCAL}/feature_importance.csv")
for f in os.listdir(LOCAL):
    shutil.copyfile(f"{LOCAL}/{f}", f"{ARTIFACTS}/{f}")

with mlflow.start_run(run_id=best["run_id"]):
    mlflow.log_metrics({f"test_{k}": v for k, v in results["lightgbm"].items()})
    mlflow.log_metric("threshold", thr)
    for f in ["model_meta.json", "calibration_test.csv", "feature_importance.csv"]:
        mlflow.log_artifact(f"{LOCAL}/{f}")
    mlflow.lightgbm.log_model(clf.booster_, "model",
                              signature=infer_signature(X_te.head(100), p_te[:100]),
                              input_example=X_te.head(5),
                              registered_model_name="workspace.ml.delay_classifier")

print(f"Modelo guardado en {ARTIFACTS} y registrado como workspace.ml.delay_classifier (run {best['run_id']})")
print(f"Tamaño de model.txt: {os.path.getsize(f'{ARTIFACTS}/model.txt') / 1e6:.1f} MB")
