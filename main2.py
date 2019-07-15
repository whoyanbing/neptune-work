# Python 2/3 compatibility
from __future__  import print_function
# import gremlin driver  
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
# import pandas
import pandas as pd
# import sys
import sys

def nan_to_string(row_data):
    if type(row_data) == float:
        return ''
    return row_data

def parse_data(filename, sep=','):
    data_frame = pd.read_csv(filename, sep=sep, header=0, dtype=str, encoding='utf-8')
    return data_frame

def load_purch_history(filename, sep):
    purch_data_frame = parse_data(filename, sep=sep)

    for index, row in purch_data_frame.iterrows():
        pass
