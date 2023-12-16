import csv
import os


def get_cpu_count() -> int:
    return os.cpu_count()


def read_file(file_path: str) -> list[str]:
    with open(file_path) as f:
        f.readline()  # skip header
        lines = f.readlines()

    return lines


def write_csv(file_path: str, data: list[tuple]):
    with open(file_path, "w") as f:
        writer = csv.writer(f, delimiter=",")
        for row in data:
            writer.writerow(row)


def parse_csv_line(line: str) -> list[str]:
    reader = csv.reader([line], delimiter=",")
    return next(reader)


def split_list(lst: list, n: int) -> list[list]:
    return [lst[i * n : (i + 1) * n] for i in range((len(lst) + n - 1) // n)]


def round_number(number: float) -> float:
    return float(f"{number:.2f}")
