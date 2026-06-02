-- Схема инициализируется автоматически при первом старте контейнера ClickHouse
CREATE DATABASE IF NOT EXISTS delivery;

-- Модель времени доставки: delivery_time_min = 5 + 4.0*distance_km + 6.0*num_stops + 0.5*hour_of_day + шум
CREATE TABLE IF NOT EXISTS delivery.orders
(
    created_at        DateTime DEFAULT now(),
    order_id          UInt64,
    distance_km       Float64, -- расстояние маршрута, км
    num_stops         UInt16, -- число остановок на маршруте
    hour_of_day       UInt8, -- час оформления заказа
    delivery_time_min Float64 -- фактическое время доставки
)
ENGINE = MergeTree
ORDER BY (created_at, order_id);

CREATE TABLE IF NOT EXISTS delivery.eta_model
(
    created_at  DateTime DEFAULT now(),
    run_id      String,
    model_name  String,
    feature     String,
    coefficient Float64,
    intercept   Float64,
    r2          Float64,
    n_samples   UInt64
)
ENGINE = MergeTree
ORDER BY created_at;
