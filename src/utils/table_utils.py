def create_table_fullname(catalog_name: str, namespace_name: str, table_name: str):
    return f"{catalog_name}.{namespace_name}.{table_name}"

def create_table_view_name(table_name: str, global_temp: bool = False) -> str:
    prefix = "global_temp." if global_temp else ""
    return f"{prefix}{table_name}_view"

if __name__ == "__main__":
    view_name = create_table_view_name("passengers", False)
    print(view_name)  # Output: passengers_view