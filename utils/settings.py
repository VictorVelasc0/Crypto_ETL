"""
Author: Victor Velasco
Name: settings
Descritpion: This script is used for made the settings in datetime used in airflow and also for 
defining the airlfow environment is local or not
"""

import os
from pendulum import timezone
import pytzdata

pytzdata.set_directory("/usr/share/zoneinfo")

# Timezone for all dags
LOCAL_TZ_NAME = "America/Guatemala"
LOCAL_TZ = timezone(LOCAL_TZ_NAME)

# Environment Settings
IS_LOCAL = os.getenv("AIRFLOW_ENVIRONMENT") == "local"
