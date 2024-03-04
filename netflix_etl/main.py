from netflix_etl import NetflixETL
from logger import logger
import time


if __name__ == "__main__":
    start = time.time()

    netflix_etl = NetflixETL()
    netflix_etl.run_etl()

    logger.info(f"Completed successfully: {time.time()-start:.4f} seconds")
