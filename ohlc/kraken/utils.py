def transform_interval(interval):
    numeric = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]
    non_numeric = ['1Min', '5Min', '15Min', '30Min', '1H', '4H', '24H',  '7D', '15D']
    if interval in numeric:
        return non_numeric[numeric.index(interval)]
    return numeric[non_numeric.index(interval)]

def error_catching(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            return f"Method '{func.__name__}' Failed {str(e)}"
    return wrapper

