from __future__ import division,absolute_import,print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

#Defining the Plugin Class
class ImmigrationPlugin(AirflowPlugin):
    name = "immigration_plugin"
    operators = [
        operators.CreateTableOperator,
        operators.StageToRedshiftOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
