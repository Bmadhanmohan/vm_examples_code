<<<<<<< HEAD
# vm_examples_code
=======
project
│
├── README.md
├── data
│   └── vehicle_required_efficiency.csv
│   └── vehicle_observed_efficiency.csv
│       └── result.csv
├── requirements.txt
└── src
    ├── app.py
    ├── main
    │   ├── __init__.py
    │   └── base
    │       └── __init__.py
    │   └── job
    │       ├── __init__.py
    │       └── pipeline.py
    └── tests
        ├── __init__.py
        └── test_pipeline.py

Objective:
In this challenge, your task is to create a Spark job that identifies malfunctioning vehicles in a transportation company. You will be provided with two input files: one containing the required fuel efficiency and another containing the observed fuel efficiency. Sample files can be found in the 'data' folder.

Data schema for both files:

less
Copy code
vehicles = StructType([
    StructField("vehicleId", StringType()),
    StructField("fuelEfficiency", DoubleType())
])
vehicles_required_efficiency.csv
Includes vehicleId and required fuelEfficiency
CSV format with one line per vehicleId
vehicles_observed_efficiency.csv
Contains vehicleId and observed fuelEfficiency (from sensor)
CSV format with multiple lines per vehicleId
Data Notes:
The file vehicles_required_efficiency.csv has one record for each vehicle, while vehicles_observed_efficiency.csv has multiple records per vehicle.
The average observed fuelEfficiency must not be greater or less than the required fuelEfficiency.
Implement the following 5 methods in the main/job/pipeline.py file:

init_spark_session(self) -> SparkSession:

Create a Spark session with master local and name Faulty Vehicles Detection
read_csv(self, input_path: str) -> DataFrame:

Read the data from the specified CSV file path
Skip the header line
Return a DataFrame with the pre-defined vehicles schema
calc_average_efficiency(self, observed: DataFrame) -> DataFrame:

Calculate the average observed fuel efficiency for each vehicle
Return a DataFrame of vehicles with vehicleId and average fuelEfficiency
find_faulty_vehicles(self, avg_observed: DataFrame, required: DataFrame) -> DataFrame:

Use the average observed fuel efficiency of vehicles from calc_average_efficiency
Join avg_observed vehicles with required vehicles using vehicleId
If the absolute difference abs(observed fuelEfficiency - required fuelEfficiency) >= 5, mark it as a faulty vehicle
Filter out non-faulty vehicles and return a DataFrame containing only vehicleId and average faulty fuelEfficiency
save_as(self, data: DataFrame, output_path: str) -> None:

Save the provided DataFrame to disk at the specified output_path.
The expected output is a folder containing part files without header and formatted as: <vehicleId><COMMA><fuelEfficiency>
Complete the job implementation so that the unit tests pass when run. Use the provided tests to check your progress as you work on the problem

## Commands
- run: 
```bash
python3 src/app.py data/vehicles_observed_efficiency.csv data/vehicles_required_efficiency.csv output
```
- install: 
```bash
pip3 install -r requirements.txt
```
- test: 
```bash
py.test -p no:warnings
```
>>>>>>> 7b8829c5bddcdebb6139065d9600a12ec65e19d6
