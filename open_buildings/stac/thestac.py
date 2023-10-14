import pystac
import json
from datetime import datetime

file_path = "./results/parquet_result.json"

# Load the provided JSON data
with open(file_path, "r") as f:
    data = json.load(f)

# Create a new STAC Catalog
catalog = pystac.Catalog(id="my-catalog", description="A description of my catalog")

# Iterate through the provided data to create STAC Collections and add them to the catalog
for directory, content in data.items():
    if directory != "files" and content.get("files"):
        # Create a new STAC Collection
        collection = pystac.Collection(
            id=directory,
            description=f"A description of {directory} collection",
            extent=pystac.Extent.from_dict(
                {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [["2023-07-26T00:00:00Z", None]]},
                }
            ),
        )

        # Add the STAC Collection to the STAC Catalog
        catalog.add_child(collection)

        for file_info in content["files"]:
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
                datetime=datetime.strptime(
                    "2023-07-26T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
                ),
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

# Set the catalog's root HREF
catalog.normalize_hrefs("./results/cholmes/overture")

# Write the STAC Catalog to disk
catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
