import httpx
import json
import asyncio
import logging
import time
from pathlib import Path


class FileFinder:
    def __init__(self, extension="parquet"):
        self.extension = extension
        self.client = httpx.AsyncClient(timeout=10.0)
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    async def find_files(self, json_url, output_file_path="result.json"):
        start_time = time.time()
        result_dict = await self._find_files(json_url)
        elapsed_time = time.time() - start_time
        logging.info(f"Total time taken: {elapsed_time:.2f} seconds")

        # Ensure the directory exists before trying to save the file
        output_file_path = Path(output_file_path)
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Save result to a JSON file
        with output_file_path.open("w") as f:
            json.dump(result_dict, f, indent=4)

        await self.client.aclose()  # Close the HTTP client
        return result_dict

    async def _find_files(self, json_url):
        files_dict = {}
        try:
            response = await self.client.get(json_url)
            response.raise_for_status()
            json_data = response.json()
        except (httpx.RequestError, json.JSONDecodeError) as e:
            logging.error(
                f"Failed to retrieve or decode JSON data from {json_url}: {e}"
            )
            return files_dict

        # Process objects and prefixes concurrently
        tasks = []

        if "objects" in json_data:
            files_dict["files"] = [
                {"url": obj["url"], "size": obj["size"]}
                for obj in json_data["objects"]
                if obj["name"].endswith(f".{self.extension}")
            ]

        if "prefixes" in json_data:
            for prefix in json_data["prefixes"]:
                new_url = f"{json_url.rstrip('/')}/{prefix}"
                tasks.append(self._find_files(new_url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(
                    f"Error processing prefix {json_data['prefixes'][i]}: {result}"
                )
            else:
                files_dict[json_data["prefixes"][i]] = result

        return files_dict


# Example usage
async def main():
    parquet_finder = FileFinder(extension="parquet")
    await parquet_finder.find_files(
        "https://data.source.coop/cholmes/overture/",
        output_file_path="results/parquet_result.json",
    )

    pmtiles_finder = FileFinder(extension="pmtiles")
    await pmtiles_finder.find_files(
        "https://data.source.coop/cholmes/overture/",
        output_file_path="results/pmtiles_result.json",
    )


if __name__ == "__main__":
    asyncio.run(main())
