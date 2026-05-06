import os
import json
import logging
import requests

logger = logging.getLogger(__name__)


def handle(url):
    """Fetch the delta index file and write the sorted list of filenames to XCom."""
    logger.info(f"Fetching delta index from {url}...")

    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})

    response = session.get(url, timeout=30)
    response.raise_for_status()

    filenames = sorted(
        line.strip()
        for line in response.text.splitlines()
        if line.strip()
    )

    logger.info(f"Found {len(filenames)} delta files.")

    xcom_dir = "/airflow/xcom"
    try:
        os.makedirs(xcom_dir, exist_ok=True)
        with open(f"{xcom_dir}/return.json", "w") as f:
            json.dump(filenames, f)
        logger.info("Delta file list written to XCom.")
    except OSError:
        logger.info(f"DELTA_FILES={filenames}")
