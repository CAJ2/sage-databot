from prefect import flow


@flow
def update_db_tags():
    """
    Flow to update the tags table with predefined tags.
    """
    # Placeholder for the actual implementation
    pass


@flow
def osm_tags_flow():
    """
    Flow to assign Sage database place tags based on OSM tags.
    """
