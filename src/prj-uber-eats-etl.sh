#!/bin/bash

ROOT_DIR="prj-uber-eats-etl"

mkdir -p "$ROOT_DIR"
cd "$ROOT_DIR"

touch readme.md
touch setup.py
touch requirements.txt
touch pyproject.toml
touch .gitignore
touch Makefile

mkdir -p config
touch config/base_config.json
touch config/dev_config.json
touch config/qa_config.json
touch config/prod_config.json
touch config/spark_config.json
touch config/auto_tuning_config.json
touch config/backpressure_config.json
touch config/resource_profiles.json

mkdir -p deployment/docker
mkdir -p deployment/kubernetes
touch deployment/docker/Dockerfile
touch deployment/docker/docker-compose.yml
touch deployment/kubernetes/spark-job.yaml
touch deployment/kubernetes/configmap.yaml

mkdir -p docs
touch docs/readme.md
touch docs/architecture.md
touch docs/usage.md

mkdir -p scripts
touch scripts/submit_job.sh
touch scripts/setup_env.sh

mkdir -p src/core/context
mkdir -p src/core/observability

mkdir -p src/extract/readers
mkdir -p src/extract/schemas
mkdir -p src/transform/cleansers
mkdir -p src/transform/enrichers/join_strategies
mkdir -p src/transform/aggregators
mkdir -p src/load/writers
mkdir -p src/execution

mkdir -p src/optimization
mkdir -p src/optimization/adaptive
mkdir -p src/state
mkdir -p src/schema
mkdir -p src/storage
mkdir -p src/orchestration
mkdir -p src/metrics/reporters
mkdir -p src/utils
mkdir -p src/pipelines/ubereats

mkdir -p src/recovery
mkdir -p src/resources
mkdir -p src/quality
mkdir -p src/monitoring

find src -type d | while read dir; do
    touch "$dir/__init__.py"
done

touch src/core/context/spark_session.py
touch src/core/observability/observer.py
touch src/core/observability/observable.py
touch src/core/observability/logging_observer.py

touch src/extract/readers/reader.py
touch src/extract/readers/s3_reader.py
touch src/extract/readers/kafka_reader.py
touch src/extract/readers/postgres_reader.py
touch src/extract/readers/factory.py
touch src/extract/schemas/order_schema.py
touch src/extract/schemas/payment_schema.py
touch src/extract/schemas/status_schema.py

touch src/transform/base_transformer.py
touch src/transform/cleansers/data_cleanser.py
touch src/transform/enrichers/order_enricher.py
touch src/transform/enrichers/join_strategies/join_strategy.py
touch src/transform/aggregators/metrics_calculator.py

touch src/load/writers/writer.py
touch src/load/writers/parquet_writer.py
touch src/load/writers/factory.py

touch src/execution/pipeline.py
touch src/execution/error_handling.py

touch src/optimization/repartitioning.py
touch src/optimization/join_optimizer.py
touch src/optimization/broadcast_manager.py
touch src/optimization/skew_handler.py
touch src/optimization/caching_strategy.py
touch src/optimization/memory_profiler.py
touch src/optimization/shuffle_tuning.py
touch src/optimization/stage_skew_detector.py
touch src/optimization/adaptive/dynamic_partition_adjustment.py
touch src/optimization/adaptive/speculation_manager.py

touch src/state/checkpoint_manager.py
touch src/state/watermark_tracker.py

touch src/schema/schema_registry.py
touch src/schema/schema_validator.py
touch src/schema/schema_evolution.py

touch src/storage/format_registry.py
touch src/storage/delta_lake_manager.py

touch src/orchestration/dependency_resolver.py
touch src/orchestration/job_tracker.py

touch src/metrics/performance_metrics.py
touch src/metrics/data_quality_metrics.py
touch src/metrics/reporters/console_reporter.py
touch src/metrics/reporters/prometheus_reporter.py

touch src/utils/config_loader.py
touch src/utils/environment.py
touch src/utils/spark_utils.py

touch src/pipelines/ubereats/order_pipeline.py

touch src/recovery/job_recovery_manager.py
touch src/recovery/checkpoint_strategies.py
touch src/recovery/idempotent_writer.py

touch src/resources/dynamic_allocation_manager.py
touch src/resources/executor_sizing.py
touch src/resources/cost_based_optimizer.py

touch src/quality/data_quality_checker.py
touch src/quality/schema_drift_detector.py
touch src/quality/anomaly_detector.py
touch src/quality/slas.py

touch src/monitoring/performance_profiler.py
touch src/monitoring/resource_tracker.py
touch src/monitoring/lineage_tracker.py
touch src/monitoring/alerting_system.py

mkdir -p tests/unit/core
mkdir -p tests/unit/extract
mkdir -p tests/unit/transform
mkdir -p tests/unit/load
mkdir -p tests/integration
mkdir -p tests/performance
mkdir -p tests/system
mkdir -p tests/resources
mkdir -p tests/scalability

find tests -type d | while read dir; do
    touch "$dir/__init__.py"
done

touch tests/unit/core/test_spark_session.py
touch tests/unit/extract/test_s3_reader.py
touch tests/unit/transform/test_order_enricher.py
touch tests/unit/load/test_parquet_writer.py
touch tests/integration/test_order_pipeline.py
touch tests/performance/test_large_dataset.py
touch tests/system/test_full_pipeline.py
touch tests/scalability/test_data_generator.py
touch tests/scalability/test_horizontal_scaling.py

touch tests/resources/orders_sample.json
touch tests/resources/status_sample.json
touch tests/resources/payments_sample.json
