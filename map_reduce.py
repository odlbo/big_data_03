import typing as t

from collections import defaultdict
from dataclasses import dataclass
from multiprocessing import Pool

from helpers import read_file, write_csv, parse_csv_line, split_list, get_cpu_count


_INPUT_DATA_FILE = r".\input_data.csv"
_OUTPUT_DATA_FILE = r".\output_data.csv"
_PROCESS_POOL_SIZE = get_cpu_count() - 1


@dataclass
class CarModelInfoBase:
    make: str
    model: str

    @property
    def id(self):
        return hash((self.make, self.model))


@dataclass
class CarModelInfo(CarModelInfoBase):
    price: float


@dataclass
class CarModelInfoAggretated(CarModelInfoBase):
    price_total: float = 0.0
    count: int = 0


def _parse_data(line: str) -> CarModelInfo:
    data_raw = parse_csv_line(line)

    return CarModelInfo(
        make=data_raw[0],
        model=data_raw[1],
        price=float(data_raw[15]),
    )


def mapper(lines: list[str]) -> dict[tuple[str, str], CarModelInfoAggretated]:
    result = {}

    for line in lines:
        line = line.strip()
        if not line:
            continue

        car_model_info = _parse_data(line)

        if car_model_info.id not in result:
            car_model_info_agg = CarModelInfoAggretated(
                make=car_model_info.make,
                model=car_model_info.model,
            )
            result[car_model_info.id] = car_model_info_agg

        car_model_info_agg.price_total += car_model_info.price
        car_model_info_agg.count += 1

    return result


def reducer(
    prepared_data: list[dict[tuple[str, str]], CarModelInfoAggretated]
) -> dict[tuple[str, str], CarModelInfoAggretated]:
    result = defaultdict(CarModelInfoAggretated)

    for car_models in prepared_data:
        for car_model_agg in car_models.values():
            if car_model_agg.id not in result:
                result[car_model_agg.id] = car_model_agg
                continue

            result[car_model_agg.id].price_total += car_model_agg.price_total
            result[car_model_agg.id].count += car_model_agg.count

    return result


def main():
    # read input data
    lines = read_file(file_path=_INPUT_DATA_FILE)
    lines_per_process = split_list(lst=lines, n=_PROCESS_POOL_SIZE)

    # map-reduce
    mapper_results = []
    with Pool(_PROCESS_POOL_SIZE) as pool:
        mapper_results = pool.map(mapper, lines_per_process)

    reducer_result = reducer(mapper_results)

    # calculate average price
    result = []
    for car_model_agg in reducer_result.values():
        result.append(
            (
                car_model_agg.make,
                car_model_agg.model,
                car_model_agg.price_total / car_model_agg.count,
            )
        )

    # write result
    write_csv(
        file_path=_OUTPUT_DATA_FILE,
        header=("make", "model", "avg_price"),
        data=result,
    )


if __name__ == "__main__":
    main()
