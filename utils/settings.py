""" Global project settings"""

import os
from pendulum import timezone
import pytzdata

pytzdata.set_directory("/usr/share/zoneinfo")

# Timezone for all dags
LOCAL_TZ_NAME = "America/Guatemala"
LOCAL_TZ = timezone(LOCAL_TZ_NAME)

# Environment Settings
IS_LOCAL = os.getenv("AIRFLOW_ENVIRONMENT") == "local"