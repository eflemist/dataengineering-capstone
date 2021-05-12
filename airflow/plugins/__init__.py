from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
#import helpers

# Defining the plugin class
class GlobalEvntPlugin(AirflowPlugin):
    name = "gblevnt_plugin"
    operators = [
         operators.AERs3ToRedshiftOperator,
         operators.AERLoadDateDimOperator,
	 operators.AERLoadDimOperator,
	 operators.AERDataQualityOperator,
         operators.AERSqlInserts
    ]
    
#    helpers = [
#        helpers.SqlInserts
#    ]
    
