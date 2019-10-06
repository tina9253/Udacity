from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.RawToPostgresOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.FactInsertQueries
    ]
