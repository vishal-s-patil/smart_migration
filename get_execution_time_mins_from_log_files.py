import os
import sys
from datetime import datetime

def get_timestamp(line):
    try:
        ts_str = " ".join(line.strip().split()[:2])
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 get_execution_time_mins_from_log_files.py <source_folder> <output_file>")
        sys.exit(1)

    source_folder = sys.argv[1]
    output_file = sys.argv[2]

    with open(output_file, "w") as out_file:
        for fname in os.listdir(source_folder):
            if not fname.startswith("debug-") or not fname.endswith(".log"):
                continue

            file_path = os.path.join(source_folder, fname)
            with open(file_path, "r") as f:
                lines = f.readlines()

            if not lines:
                continue

            first_ts = get_timestamp(lines[0])
            last_ts = get_timestamp(lines[-1])

            name_part = fname.replace("debug-", "").replace(".log", "")

            if first_ts and last_ts:
                diff_minutes = int((last_ts - first_ts).total_seconds() / 60)
                out_file.write(f"{name_part},{diff_minutes},{first_ts},{last_ts}\n")
            else:
                out_file.write(f"{name_part},ERROR,,\n")

if __name__ == "__main__":
    main()
