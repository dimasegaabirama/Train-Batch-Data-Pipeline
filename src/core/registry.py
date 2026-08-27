import pytest
from typing_extensions import Literal

from src.etl.extract import IcebergExtract, MongoExtract
from src.etl.load import IcebergLoad
from src.etl.transform import (
    # BRONZE STAGE
    BronzeTransform,

    # SILVER STAGE
    PassengersTransform,
    RoutesTransform,
    StationsTransform,
    TicketsTransform,
    TrainsTransform,

    # GOLD STAGE
    CancellationSummary,
    RefundLoss,
    RevenueDaily,
    TrainPerformance
)

from src.utils.filter_utils import (
    build_iceberg_boolean_filter,
    build_iceberg_incremental_filter,
    build_mongo_incremental_filter,
)

Component = Literal["extract", "transform", "load", "filter"]

_FILTER_REGISTRY = {
    "incremental": {
        "mongo": build_mongo_incremental_filter,
        "iceberg": build_iceberg_incremental_filter
    },
    "boolean": {
        "iceberg": build_iceberg_boolean_filter
    }
}


_DATA_QUALITY_REGISTRY = {
    "bronze": {"default": "test_bronze.py"},
    "silver": {
        "passengers": "test_passengers.py",
        "trains": "test_trains.py",
        "stations": "test_stations.py",
        "routes": "test_routes.py",
        "tickets": "test_tickets.py"
    },
    "gold": {
        "train_performance": "test_train_performance.py",
        "refund_loss": "test_refund_loss.py",
        "revenue_daily": "test_revenue_daily.py",
        "cancellation_summary": "test_cancellation_summary.py"
    }
}


_EXTRACT_REGISTRY = {
    "bronze": {"default": MongoExtract},
    "silver": {"default": IcebergExtract},
    "gold": {"default": IcebergExtract}
}


_TRANSFORMER_REGISTRY = {
    "bronze": {"default": BronzeTransform},
    "silver": {
        "passengers": PassengersTransform,
        "trains": TrainsTransform,
        "stations": StationsTransform,
        "routes": RoutesTransform,
        "tickets": TicketsTransform
    },
    "gold": {
        "train_performance": TrainPerformance,
        "refund_loss": RefundLoss,
        "revenue_daily": RevenueDaily,
        "cancellation_summary": CancellationSummary
    }
}


_LOAD_REGISTRY = {
    "bronze": {"default": IcebergLoad},
    "silver": {"default": IcebergLoad},
    "gold": {"default": IcebergLoad},
}


_REGISTRY_MAP = {
    "extract": _EXTRACT_REGISTRY,
    "transform": _TRANSFORMER_REGISTRY,
    "load": _LOAD_REGISTRY,
    "filter": _FILTER_REGISTRY,
    "data_quality": _DATA_QUALITY_REGISTRY
}


def resolve_registry_class(
    key1: str,
    key2: str,
    component_name: Component,
    required: bool = True,
):
    registry = _REGISTRY_MAP.get(component_name)
    if registry is None:
        raise ValueError(
            f"Component '{component_name}' is not a registered component type"
        )

    sub_registry = registry.get(key1, {})
    component_cls = sub_registry.get(key2, sub_registry.get("default"))

    if component_cls is None and required:
        raise ValueError(
            f"{component_name} for '{key2}' under '{key1}' does not exist"
        )

    return component_cls
