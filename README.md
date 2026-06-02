# Delivery - Airflow + Spark + ClickHouse + Grafana

Конвейер оценки времени доставки: непрерывная генерация заказов в ClickHouse,
оркестрация в Airflow (с обработкой сбоев через retry), распределённый расчёт
ML-модели в Spark на трёх узлах и визуализация с алертами в Grafana
