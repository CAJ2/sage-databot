# requirements: project

import wmill
from sqlalchemy import select
from sqlalchemy.orm import Session
from urllib.request import urlretrieve
import tempfile
import os

from f.db.databot.model import OFFProduct
from f.db.sage.model import VariantSources, Source, ExternalSource
from f.graphql.api_client.client import CreateSourceInput
from f.graphql.api_client.enums import SourceType
from f.graphql.api_client.input_types import SourceInput, UpdateVariantInput
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.s3 import S3Client


def barcode_to_path(barcode: str) -> str:
    """
    Convert a barcode to OpenFoodFacts directory path.
    Example: "4012359114303" -> "/401/235/911/4303"
    """
    # Remove any prefix
    barcode = barcode.replace("off_", "")

    # Pad with zeros if needed to ensure proper length
    if len(barcode) < 13:
        barcode = barcode.zfill(13)

    # Split into groups: 3, 3, 3, 4
    parts = [
        barcode[0:3],
        barcode[3:6],
        barcode[6:9],
        barcode[9:13],
    ]

    return "/" + "/".join(parts)


def off_image_url(barcode: str, image_id: str, size: str = "full") -> str:
    path = barcode_to_path(barcode)

    # Determine filename based on size
    if size == "full":
        filename = f"{image_id}.jpg"
    else:
        filename = f"{image_id}.{size}.jpg"

    url = (
        f"https://openfoodfacts-images.s3.eu-west-3.amazonaws.com/data{path}/{filename}"
    )
    return url


def download_off_image(url: str) -> tuple[str, str] | None:
    """
    Download an image from OpenFoodFacts S3 bucket.
    Returns (temp_file_path, extension) or None if download fails.
    """
    print(f"Downloading image from: {url}")

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    temp_file.close()

    try:
        urlretrieve(url, temp_file.name)
        print(f"Successfully downloaded to: {temp_file.name}")
        return (temp_file.name, ".jpg")
    except Exception as e:
        print(f"Failed to download image from {url}: {e}")
        raise e


def main(
    variant_id: str,
    image_sizes: list[str] = ["full"],
):
    """
    Download OpenFoodFacts product images and upload them to S3.

    Args:
        variant_id: The variant ID to process
        image_sizes: List of image sizes to download (e.g., ["full", "400"])
    """

    crdb = create_sql_engine()
    client, user = api_connect()

    # Initialize S3 client for sources bucket
    s3_client = S3Client(resource_id="f/s3_config/s3_sources")

    # Step 1: Validate variant exists in CRDB
    with Session(crdb) as session:
        stmt = (
            select(VariantSources)
            .join(Source, Source.id == VariantSources.source_id)
            .where(VariantSources.variant_id == variant_id)
        )
        variants = session.scalars(stmt).unique().all()
        if not variants:
            raise ValueError(f"Variant {variant_id} not found in CRDB")

    print(f"Variant {variant_id} found")

    # Step 2: Get OFF external source connection
    with Session(crdb) as session:
        stmt = select(ExternalSource.source_id).where(
            ExternalSource.source == "OFF", ExternalSource.variant_id == variant_id
        )
        external_source = session.scalars(stmt).first()
        if not external_source:
            raise ValueError(
                f"No OpenFoodFacts connection found for variant {variant_id} in external_sources"
            )

        off_barcode = external_source

    print(f"Found OFF barcode: {off_barcode}")

    # Step 3: Get OFF product data
    off_product_id = f"off_{off_barcode}"
    print(f"Fetching OFF product data for {off_product_id}...")

    with Session(crdb) as session:
        stmt = select(OFFProduct).where(OFFProduct.id == off_product_id)
        product = session.scalars(stmt).first()

    if not product:
        raise ValueError(f"No OFF product found with id {off_product_id}")

    if not product.images or not product.images.images:
        raise ValueError(f"No images found for product {off_product_id}")

    print(f"Found product with images: {product.images.images}")

    # Step 4: Extract image IDs from images field
    images_data = product.images.images
    if isinstance(images_data, list):
        image_ids: dict[str, list[str]] = {}
        # Only get the primary images, not cropped sections
        # Some images have an imgid referring to the larger image it is cropped from
        for image in images_data:
            if "imgid" not in image or image["imgid"] is None:
                image_ids[image["key"]] = [
                    k for k in image["sizes"].keys() if k in image_sizes
                ]
    else:
        raise ValueError(f"Unexpected images data format: {type(images_data)}")

    if not image_ids:
        raise ValueError(f"No valid image IDs found in product {off_product_id}")

    print(f"Found {len(image_ids)} images: {image_ids}")

    # Step 5: Process each image and size combination
    created_sources = []

    for image_id, sizes in image_ids.items():
        for size in sizes:
            print(f"\nProcessing image {image_id}, size: {size}")

            url = off_image_url(off_barcode, image_id, size)
            exists = False
            for v in variants:
                if v.source.type == SourceType.IMAGE and v.source.content_url == url:
                    print(
                        f"Source already exists for image {image_id} size {size}, skipping"
                    )
                    exists = True
                    break

            if exists:
                continue

            # Download from OFF
            download_result = download_off_image(url)
            if not download_result:
                print(f"Skipping image {image_id} size {size} - download failed")
                continue

            temp_path, ext = download_result

            try:
                # Upload to sage-leaf-sources S3
                size_suffix = "" if size == "full" else f".{size}"
                s3_key = f"off/{off_barcode}/{image_id}{size_suffix}{ext}"
                s3_url = f"s3://{s3_client.bucket}/{s3_key}"

                print(f"Uploading to S3: {s3_url}")

                # Upload file
                with open(temp_path, "rb") as f:
                    s3_client.s3_upload(s3_url, f)

                # Generate public URL
                # For DigitalOcean Spaces with CDN
                public_url = f"https://{s3_client.bucket}.fra1.cdn.digitaloceanspaces.com/{s3_key}"

                print(f"Public URL: {public_url}")

                # Create source via GraphQL
                source_input = CreateSourceInput(
                    type=SourceType.IMAGE,
                    location=public_url,
                    contentURL=url,
                    metadata={
                        "parent_source": "g6OJVnSzQkE0mHtYS31O9",  # OFF source ID in CRDB
                        "key": image_id,
                        "size": size,
                    },
                )

                print("Creating source record...")
                op = client.add_source(source_input)

                if not op.create_source or not op.create_source.source:
                    print(f"Failed to create source for image {image_id} size {size}")
                    continue

                source_id = op.create_source.source.id
                print(f"Created source: {source_id}")

                print("Linking source to variant...")
                variant_input = UpdateVariantInput(id=variant_id)
                variant_input.add_sources = [SourceInput(id=source_id)]
                op = client.update_variant(variant_input)

                if not op.update_variant or not op.update_variant.variant:
                    print(
                        f"Failed to link source to variant for image {image_id} size {size}"
                    )
                    continue

                created_sources.append(
                    {
                        "variant_id": variant_id,
                        "off_barcode": off_barcode,
                        "source_id": source_id,
                        "image_id": image_id,
                        "size": size,
                        "location": public_url,
                    }
                )

            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    print(f"Warning: Failed to delete temp file {temp_path}: {e}")

    print(f"Successfully created {len(created_sources)} source(s)")

    return created_sources
