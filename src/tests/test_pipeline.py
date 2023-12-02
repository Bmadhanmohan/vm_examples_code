import os
import glob
import pytest
from pyspark.sql import SparkSession
from main.job.pipeline import PySparkJob, vehicles

REQUIRED_EFFICIENCY_PATH = "test_input/required.csv"
OBSERVED_EFFICIENCY_PATH = "test_input/observed.csv"
FAULTY_VEHICLES_PATH = "test_output/faulty_vehicles.csv"

job = PySparkJob()


@pytest.fixture(scope="module", autouse=True)
def setup():
    os.makedirs(os.path.dirname(REQUIRED_EFFICIENCY_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(FAULTY_VEHICLES_PATH), exist_ok=True)

    if not os.path.isfile(REQUIRED_EFFICIENCY_PATH):
        with open(REQUIRED_EFFICIENCY_PATH, 'w') as w:
            w.write("v1,25\n")
            w.write("v2,30\n")

    if not os.path.isfile(OBSERVED_EFFICIENCY_PATH):
        with open(OBSERVED_EFFICIENCY_PATH, 'w') as w:
            w.write("v1,23\n")
            w.write("v1,27\n")
            w.write("v1,28\n")
            w.write("v2,32\n")


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"


@pytest.mark.filterwarnings("ignore")
def test_read_csv():
    data = job.read_csv(OBSERVED_EFFICIENCY_PATH)
    observed = sorted(data.collect(), key=lambda x: x.vehicleId)

    assert (4 == len(observed))
    assert ("v1" == observed[0].vehicleId)
    assert ("v1" == observed[1].vehicleId)
    assert ("v2" == observed[3].vehicleId)


@pytest.mark.filterwarnings("ignore")
def test_calc_average_efficiency():
    observed = job.read_csv(OBSERVED_EFFICIENCY_PATH)
    avg_efficiency = job.calc_average_efficiency(observed)
    efficiency = sorted(avg_efficiency.collect(), key=lambda x: x.vehicleId)

    assert (2 == len(efficiency))
    assert ("v1" == efficiency[0].vehicleId)
    assert ("v2" == efficiency[1].vehicleId)
    assert (26.0 == efficiency[0].fuelEfficiency)
    assert (32.0 == efficiency[1].fuelEfficiency)
