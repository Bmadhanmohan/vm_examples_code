import sys
from main.job.pipeline import PySparkJob


input_path = sys.argv[1]


def main():
    job = PySparkJob()

    # Load input data to DataFrame
    print("<<Reading>>")
    observed = job.read_csv(sys.argv[1])
    required = job.read_csv(sys.argv[2])

    # Compute average observed efficiency
    print("<<Avg Observed Efficiency>>")
    avg_observed_efficiency = job.calc_average_efficiency(observed)
    avg_observed_efficiency.show()

    # Get faulty vehicles
    print("<<Faulty Vehicles>>")
    faulty_vehicles = job.find_faulty_vehicles(avg_observed_efficiency, required)
    faulty_vehicles.show()

    # Store faulty vehicles to file
    print("<<Save Faulty Vehicles>>")
    job.save_as(faulty_vehicles, sys.argv[3])

    job.stop()


if __name__ == '__main__':
    main()
