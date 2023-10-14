import duckdb
from typing import List, Dict
import logging
import time


class ParquetProcessor:
    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        self.con = db_connection
        self._setup_spatial_extension()
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def _setup_spatial_extension(self):
        logging.info("Installing and loading spatial and httpfs extensions.")
        try:
            self.con.load_extension("spatial")
            self.con.load_extension("httpfs")
        except duckdb.DuckDBError:
            logging.info("Spatial and httpfs extensions not found. Installing now.")
            self.con.install_extension("spatial")
            self.con.load_extension("spatial")
            self.con.install_extension("httpfs")
            self.con.load_extension("httpfs")

    def get_column_info(self, file_path: str) -> Dict[str, str]:
        logging.info(f"Getting column info for {file_path}")
        start_time = time.time()
        query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
        try:
            column_info_rows = self.con.execute(query).fetchall()
        except duckdb.DuckDBError as e:
            logging.error(f"Database error: {e}")
            return {}
        column_info = {row[0]: row[1] for row in column_info_rows}
        end_time = time.time()
        logging.info(
            f"Completed getting column info in {end_time - start_time} seconds."
        )
        return column_info

    def get_bbox(self, file_path: str) -> List[float]:
        logging.info(f"Getting bounding box for {file_path}")
        start_time = time.time()
        query = f"SELECT ST_AsText(ST_Envelope_Agg(ST_GeomFromWKB(geometry))) as bbox FROM read_parquet('{file_path}')"
        try:
            metadata_row = self.con.execute(query).fetchone()
        except duckdb.DuckDBError as e:
            logging.error(f"Database error: {e}")
            return []
        bbox_str = str(metadata_row[0])  # Convert bytes to string
        coords = bbox_str.replace("POLYGON ((", "").replace("))", "").split(", ")
        bbox_list = [float(coord) for pair in coords for coord in pair.split()]
        end_time = time.time()
        logging.info(
            f"Completed getting bounding box in {end_time - start_time} seconds."
        )
        return bbox_list

    def get_geometry_type(self, file_path: str) -> str:
        logging.info(f"Getting geometry type for {file_path}")
        start_time = time.time()
        query = f"SELECT ST_GeometryType(ST_GeomFromWKB(geometry)) FROM read_parquet('{file_path}')"
        try:
            geometry_type = self.con.execute(query).fetchone()[0]
        except duckdb.DuckDBError as e:
            logging.error(f"Database error: {e}")
            return ""
        camel_case_type = "".join(
            word.capitalize() for word in geometry_type.split("_")
        )
        end_time = time.time()
        logging.info(
            f"Completed getting geometry type in {end_time - start_time} seconds."
        )
        return camel_case_type


def process_parquet_files(file_paths: List[str]):
    with duckdb.connect() as con:
        processor = ParquetProcessor(con)
        for file_path in file_paths:
            logging.info(f"Processing {file_path}")
            start_time = time.time()

            column_info = processor.get_column_info(file_path)
            bbox = processor.get_bbox(file_path)
            geometry_type = processor.get_geometry_type(file_path)

            end_time = time.time()
            logging.info(
                f"Completed processing {file_path} in {end_time - start_time} seconds."
            )

            print(f"File: {file_path}")
            print(f"Column Information: {column_info}")
            print(f"Bounding Box: {bbox}")
            print(f"Geometry Type: {geometry_type}")


if __name__ == "__main__":
    parquet_files = [
        "https://data.source.coop/cholmes/overture/geoparquet-country-quad-2/AD.parquet",
        "https://data.source.coop/cholmes/overture/geoparquet-country-quad-2/AI.parquet",
        "https://data.source.coop/cholmes/overture/geoparquet-country-quad-2/AE.parquet",
    ]
    process_parquet_files(parquet_files)
