import configparser

def get_config(sec,key):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    value = config.get(sec,key)
    return value
