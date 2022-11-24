import pytest
from conftest import spark
import chispa

from src.transform import deduplicate_by_event_id


def test_transform_deduplicate(spark):
    input_data = [
        {"id": "05551f56-2d63-477f-91cb-e286b1df16fc", "datetime": "2021-01-23 10:22:28", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}},
        {"id": "097f20f6-afb9-488b-a24b-0a5a76272dd8", "datetime": "2021-01-23 10:50:05", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}},
        {"id": "097f20f6-afb9-488b-a24b-0a5a76272dd8", "datetime": "2021-01-23 10:50:05", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}}
    ]

    sc = spark.sparkContext
    df = spark.read.json(sc.parallelize([input_data]))
    df = deduplicate_by_event_id(df)

    assert df.select("id").distinct().count() == 2


@pytest.fixture
def test_transform(spark):
    pass
