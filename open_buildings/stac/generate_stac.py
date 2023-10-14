import pystac
from pystac import Provider
import json
from datetime import datetime
from pathlib import Path

file_path = "./results/parquet_result.json"

# Load the provided JSON data
with open(file_path, "r") as f:
    data = json.load(f)

# Create a new STAC Catalog
catalog = pystac.Catalog(
    id="overture-maps",
    title="Overture Maps Data",
    description="This dataset is a copy of the [Overture Maps Data](https://overturemaps.org/download/) offering the data in more GIS-friendly",
)


def handle_hive_partition(directory):
    """Handle the hive partitioned directories."""
    parts = str(directory).split("=")
    if len(parts) > 1:
        return Path(parts[0]) / parts[1]
    else:
        return Path(directory)


def process_files(directory, content_files):
    # Create a new STAC Collection
    collection = pystac.Collection(
        id=str(directory).strip("/"),
        description=f"{directory} collection",
        extent=pystac.Extent.from_dict(
            {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2023-07-26T00:00:00Z", None]]},
            }
        ),
        keywords=["overture", "maps", "data", "geoparquet"],
        providers=[
            Provider(
                name="Overture Maps",
                url="https://overturemaps.org",
                roles=["producer", "licensor"],
            ),
            Provider(
                description="Processed from raw parquet in  to GeoParquet and PMTiles",
                name="Chris Holmes",
                url="https://twitter.com/opencholmes",
                roles=["processor"],
            ),
            Provider(
                description="Generate Compliant STAC Catalogs",
                name="Youssef Harby",
                url="https://github.com/Youssef-Harby/",
                roles=["processor"],
            ),
            Provider(
                description="Provided cloud storage",
                name="Source Cooperative",
                url="https://source.coop/",
                roles=["host"],
            ),
        ],
        license="CDLA-Permissive-2.0",
    )

    # Add the STAC Collection to the STAC Catalog
    catalog.add_child(collection)

    for file_info in content_files:
        # Extract country code from the URL to use as ID
        country_code = file_info["url"].split("/")[-1].replace(".parquet", "")
        # Create a STAC Item
        item = pystac.Item(
            id=country_code,
            geometry={
                "type": file_info["geometry_type"],
                "coordinates": file_info["bbox"][0],
            },
            bbox=[
                min(coord[0] for coord in file_info["bbox"][0][0]),
                min(coord[1] for coord in file_info["bbox"][0][0]),
                max(coord[0] for coord in file_info["bbox"][0][0]),
                max(coord[1] for coord in file_info["bbox"][0][0]),
            ],
            datetime=datetime.strptime("2023-07-26T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            properties={},
        )

        # Create a custom Asset and add it to the STAC Item
        asset = pystac.Asset(
            href=file_info["url"],
            title=country_code,
            media_type="application/parquet",
        )
        item.add_asset("data", asset)

        # Add the STAC Item to the STAC Collection
        collection.add_item(item)


def process_directory(directory, content):
    final_directory = handle_hive_partition(directory)

    if isinstance(content, list):  # check if content is a list of files
        process_files(final_directory, content)
    elif isinstance(
        content, dict
    ):  # check if content is a dictionary of subdirectories
        if "files" in content:  # check for files key in the dictionary
            process_files(final_directory, content["files"])
        for subdirectory, subcontent in content.items():
            if isinstance(
                subcontent, list
            ):  # again, check if subcontent is a list of files
                process_files(subdirectory, subcontent)
            elif isinstance(subcontent, dict):
                process_directory(final_directory / subdirectory, subcontent)


# Iterate through the provided data to create STAC Collections and add them to the catalog
for directory, content in data.items():
    if isinstance(content, list):  # check if content is a list of files
        process_files(directory, content)
    elif isinstance(
        content, dict
    ):  # check if content is a dictionary of subdirectories
        if "files" in content:  # check for files key in the dictionary
            process_files(directory, content["files"])
        for subdirectory, subcontent in content.items():
            if isinstance(
                subcontent, list
            ):  # again, check if subcontent is a list of files
                process_files(subdirectory, subcontent)

# Set the catalog's root HREF
catalog.normalize_hrefs("./results/cholmes/overture")
# Write the STAC Catalog to disk
catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
