def print_flatten_schema(data, prefix=''):
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key
            print_flatten_schema(value, new_key)
    elif isinstance(data, list):
        print(f">>> {prefix}")
    else:
        print(f"{prefix} value is {data}")