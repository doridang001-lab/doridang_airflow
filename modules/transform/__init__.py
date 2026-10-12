# plugins/transform/__init__.py
def __getattr__(name):
    from importlib import import_module
    if name not in __all__:
        raise AttributeError(name)
    module = 'modules.transform.aggregate' if name == 'agg_df' else 'modules.transform.utility.io'
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value

__all__ = [
            'agg_df',
            'read_csv_glob',
            'load_data',
            'preprocess_df'
]
