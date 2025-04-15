import os
import re
import subprocess
import statistics

def main():
    trials = 5
    for strategy in [
        "SEQUENTIAL",
        "GLOBAL_LOCK",
        "TWO_PHASE_CENTRALIZED_MERGE",
        "SIMPLE_TWO_PHASE_RADIX",
        "SIMPLE_THREE_PHASE_RADIX",
        "IMPLICIT_REPARTITIONING",
        "DUCKDBISH_TWO_PHASE"
    ]:
        i, n = 1, os.cpu_count()

        while i <= n:
            result = subprocess.run(
                f"./build/main --num_threads {i} --strategy {strategy} --num_dryruns 3 --num_trials 5",
                shell=True, capture_output=True,
            )

            outputs = result.stdout.decode()
            matches = re.findall(r'aggregation_time=(\d+)ms', outputs)
            average = statistics.mean(map(float, matches[-trials:]))
            print(f"{strategy},aggregation,{i},{average}")

            match strategy:
                case "SEQUENTIAL":
                    break
                case "TWO_PHASE_CENTRALIZED_MERGE" | "SIMPLE_TWO_PHASE_RADIX" | "SIMPLE_THREE_PHASE_RADIX" | "DUCKDBISH_TWO_PHASE":
                    matches = re.findall(r'phase_1=(\d+)ms', outputs)
                    average = statistics.mean(map(float, matches[-trials:]))
                    print(f"{strategy},phase_1,{i},{average}")
                    matches = re.findall(r'phase_2=(\d+)ms', outputs)
                    average = statistics.mean(map(float, matches[-trials:]))
                    print(f"{strategy},phase_2,{i},{average}")
                    if strategy == "SIMPLE_THREE_PHASE_RADIX":
                        matches = re.findall(r'phase_3=(\d+)ms', outputs)
                        average = statistics.mean(map(float, matches[-trials:]))
                        print(f"{strategy},phase_3,{i},{average}")
            i *= 2

if __name__ == '__main__':
    main()
