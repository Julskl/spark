# Распределённый Spark ML job: модель оценки времени доставки (ETA).
import os
import uuid
import clickhouse_connect

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

FEATURES = ["distance_km", "num_stops", "hour_of_day"]
LABEL = "delivery_time_min"
LIMIT = int(os.getenv("TRAIN_LIMIT", "50000"))


def read_from_clickhouse():
    """Читаем заказы драйвером в pandas через HTTP-интерфейс ClickHouse."""
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )
    query = f"""
        SELECT {', '.join(FEATURES)}, {LABEL}
        FROM delivery.orders
        ORDER BY created_at DESC
        LIMIT {LIMIT}
    """
    pdf = client.query_df(query)
    print(f"[eta-ml] прочитано заказов из ClickHouse: {len(pdf)}", flush=True)
    return pdf


def write_coefficients(run_id, coeffs, intercept, r2, n_samples):
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )
    rows = []
    for feature, coef in zip(FEATURES, coeffs):
        rows.append([run_id, "eta_linear_regression", feature,
                     float(coef), float(intercept), float(r2), int(n_samples)])
    rows.append([run_id, "eta_linear_regression", "__intercept__",
                 float(intercept), float(intercept), float(r2), int(n_samples)])

    client.insert(
        "delivery.eta_model",
        rows,
        column_names=["run_id", "model_name", "feature",
                      "coefficient", "intercept", "r2", "n_samples"],
    )
    print(f"[eta-ml] коэффициенты записаны в ClickHouse (run_id={run_id})", flush=True)


def main():
    run_id = str(uuid.uuid4())[:8]

    spark = (
        SparkSession.builder
        .appName(f"eta-linreg-{run_id}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    pdf = read_from_clickhouse()
    if len(pdf) < 10:
        raise RuntimeError("Слишком мало заказов для обучения — подождите генератор")

    # pandas -> Spark DataFrame, явно раскидываем на 3 партиции,
    # чтобы задачи реально разъехались по трём воркерам.
    sdf = spark.createDataFrame(pdf).repartition(3)
    n_samples = sdf.count()
    print(f"[eta-ml] партиций: {sdf.rdd.getNumPartitions()}, заказов: {n_samples}", flush=True)

    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    train_df = assembler.transform(sdf).select("features", LABEL)

    lr = LinearRegression(featuresCol="features", labelCol=LABEL,
                          maxIter=50, regParam=0.0)
    model = lr.fit(train_df)

    coeffs = list(model.coefficients)
    intercept = model.intercept
    r2 = model.summary.r2

    print("[eta-ml] ====== РЕЗУЛЬТАТ (ETA модель) ======", flush=True)
    for f, c in zip(FEATURES, coeffs):
        print(f"[eta-ml]   {f}: {c:.4f}", flush=True)
    print(f"[eta-ml]   intercept: {intercept:.4f}", flush=True)
    print(f"[eta-ml]   R2: {r2:.4f}", flush=True)
    print("[eta-ml] (истинные: distance_km=4, num_stops=6, hour_of_day=0.5, intercept=5)", flush=True)

    write_coefficients(run_id, coeffs, intercept, r2, n_samples)

    spark.stop()


if __name__ == "__main__":
    main()
