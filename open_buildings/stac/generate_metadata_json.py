import asyncio
import duckdb
import logging
import time
import json
from pathlib import Path
from get_scoop import FileFinder
from geoparquet_metadata_duckdb import ParquetProcessor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def main():
    output_file_path = Path("results/parquet_result.json")

    async def parquet_finder():
        if not output_file_path.exists():
            parquet_finder = FileFinder(extension="parquet")
            await parquet_finder.find_files(
                "https://data.source.coop/cholmes/overture/",
                output_file_path=output_file_path,
            )

    await parquet_finder()

    def read_json_file(file_path: str) -> list:
        with open(file_path, "r") as f:
            data = json.load(f)

        def collect_urls(d: dict) -> list:
            urls = []
            for key, value in d.items():
                if key == "files":
                    urls.extend([item["url"] for item in value])
                else:
                    urls.extend(collect_urls(value))
            return urls

        return collect_urls(data)

    def needs_processing(item: dict) -> bool:
        # Check 'columns'
        if not item.get("columns"):
            return True

        # Check 'bbox'
        bbox = item.get("bbox", [[]])
        if not bbox or len(bbox[0][0]) != 8:
            return True

        # Check 'geometry_type'
        if not item.get("geometry_type"):
            return True

        return False

    def process_parquet_files(file_paths: list):
        def update_metadata(d: dict, file_paths: list) -> None:
            for key, value in d.items():
                if key == "files":
                    for item in value:
                        if item["url"] in file_paths:
                            if needs_processing(item):
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

                                    # If after processing, the conditions are still true, we will skip the item.
                                    if needs_processing(item):
                                        logging.warning(
                                            f"Skipping {item['url']} due to empty or incorrect fields."
                                        )

                                # Write updated data to file after processing each URL
                                with open(output_file_path, "w") as f:
                                    json.dump(data, f, indent=4)

                else:
                    update_metadata(value, file_paths)

        with open(output_file_path, "r") as f:
            data = json.load(f)
        update_metadata(data, file_paths)

    parquet_file_paths = read_json_file(output_file_path)
    process_parquet_files(parquet_file_paths)


if __name__ == "__main__":
    asyncio.run(main())
