from datetime import datetime

# From the JSON
fertilization_time_str = "2024.01.13 13:20:00"
t2_timestamp_str = "2024.01.14 18:44:05"

# Parse datetimes
fertilization_time = datetime.strptime(fertilization_time_str, "%Y.%m.%d %H:%M:%S")
t2_timestamp = datetime.strptime(t2_timestamp_str, "%Y.%m.%d %H:%M:%S")

# Calculate difference in hours
time_diff_seconds = (t2_timestamp - fertilization_time).total_seconds()
time_diff_hours = time_diff_seconds / 3600

print(f"Fertilization Time: {fertilization_time}")
print(f"t2 Timestamp: {t2_timestamp}")
print(f"\nTime difference: {time_diff_seconds} seconds")
print(f"Time difference: {time_diff_hours} hours")
print(f"\nAPI provided Time: 29.4 hours")
print(f"Expected Time: 29.40145917 hours")
print(f"Calculated Time: {time_diff_hours} hours")
print(f"\nCalculated matches expected: {abs(time_diff_hours - 29.40145917) < 0.001}")

# Calculate what the timestamp should be for 29.40145917 hours
from datetime import timedelta
expected_timestamp = fertilization_time + timedelta(hours=29.40145917)
print(f"\nExpected timestamp for 29.40145917 hours: {expected_timestamp}")
print(f"Actual timestamp: {t2_timestamp}")
print(f"Difference: {(expected_timestamp - t2_timestamp).total_seconds()} seconds")
