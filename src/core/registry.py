from src.etl.extract import IcebergExtract, MongoExtract
from src.etl.transform.bronze import BronzeTransform
from src.etl.transform.silver import (
    PassengersTransform,
    RoutesTransform,
    StationsTransform,
    TicketsTransform,
    TrainsTransform,
)

from src.etl.load import IcebergLoad

from src.utils.filter_utils import (
    build_iceberg_incremental_filter,
    build_mongo_incremental_filter,
)

from src.data_quality.bronze import BronzeDQ

_FILTER_REGISTRY = {
    "bronze": {"default": build_mongo_incremental_filter},
    "silver": {"default": build_iceberg_incremental_filter},
}


_EXTRACT_REGISTRY = {
    "bronze": {"default": MongoExtract},
    "silver": {"default": IcebergExtract},
    "gold": {"default": IcebergExtract},
}


_TRANSFORMER_REGISTRY = {
    "bronze": {"default": BronzeTransform},
    "silver": {
        "passengers": PassengersTransform,
        "trains": TrainsTransform,
        "stations": StationsTransform,
        "routes": RoutesTransform,
        "tickets": TicketsTransform,
    },
    "gold": {},
}


_LOAD_REGISTRY = {
    "bronze": {"default": IcebergLoad},
    "silver": {"default": IcebergLoad},
    "gold": {"default": IcebergLoad},
}

_DATA_QUALITY_REGISTRY = {
    "bronze": {"default": BronzeDQ},
    "silver": {
        "passengers": PassengersDQ,
        "trains": TrainsDQ,
        "stations": StationsDQ,
        "routes": RoutesDQ,
        "tickets": TicketsDQ,
    }
}

def resolve_registry_class(
    registry: dict,
    stage: str,
    table_name: str,
    component_name: str,
    required: bool = True,
):

    stage_components = registry.get(stage)

    if not stage_components:
        raise ValueError(f"Stage '{stage}' is not registered for {component_name}")

    component_cls = stage_components.get(table_name) or stage_components.get("default")

    if not component_cls and required:
        raise ValueError(
            f"{component_name} for table "
            f"'{table_name}' in stage "
            f"'{stage}' does not exist"
        )

    return component_cls
