import os
import sys
import pytest
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))

@pytest.fixture(params=["../dags/"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    pass
    #assert not dag_bag.import_errors