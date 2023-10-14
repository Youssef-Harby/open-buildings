import asyncio
import duckdb
import logging
import time
import json
from typing import List, Dict
from get_scoop import FileFinder
from geoparquet_metadata_duckdb import ParquetProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def main():
    async def parquet_finder():
        parquet_finder = FileFinder(extension="parquet")
        await parquet_finder.find_files(
            "https://data.source.coop/cholmes/overture/",
            output_file_path="results/parquet_result.json",
        )

    await parquet_finder()

    def read_json_file(file_path: str) -> List[str]:
        with open(file_path, "r") as f:
            data = json.load(f)

        def collect_urls(d: Dict) -> List[str]:
            urls = []
            for key, value in d.items():
                if key == "files":
                    urls.extend([item["url"] for item in value])
                else:
                    urls.extend(collect_urls(value))
            return urls

        return collect_urls(data)

    def process_parquet_files(file_paths: List[str], limit: int = 3):
        def update_metadata(d: Dict, file_paths: List[str]) -> Dict:
            nonlocal processed_count  # Declare the counter as nonlocal to modify it within the nested function
            for key, value in d.items():
                if key == "files":
                    for item in value:
                        if item["url"] in file_paths:
                            if (
                                processed_count < limit
                            ):  # Check if the limit has been reached
                                with duckdb.connect() as con:
                                    processor = ParquetProcessor(con)
                                    logging.info(f"Processing {item['url']}")
                                    start_time = time.time()

                                    item["columns"] = processor.get_column_info(
                                        item["url"]
                                    )
                                    item["bbox"] = processor.get_bbox(item["url"])
                                    item["geometry_type"] = processor.get_geometry_type(
                                        item["url"]
                                    )

                                    end_time = time.time()
                                    logging.info(
                                        f"Completed processing {item['url']} in {end_time - start_time} seconds."
                                    )

                                # Increment the counter
                                processed_count += 1

                                # Write updated data to file after processing each URL
                                with open("results/parquet_result.json", "w") as f:
                                    json.dump(data, f, indent=4)

                            else:
                                return  # Exit the function if the limit is reached
                else:
                    update_metadata(
                        value, file_paths
                    )  # Recursive call for nested dictionaries

        processed_count = 0  # Initialize a counter for processed URLs
        with open("results/parquet_result.json", "r") as f:
            data = json.load(f)
        update_metadata(data, file_paths)

    parquet_file_paths = read_json_file("results/parquet_result.json")
    process_parquet_files(parquet_file_paths)


if __name__ == "__main__":
    asyncio.run(main())
