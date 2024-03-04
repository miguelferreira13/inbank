import pandas as pd
import sqlite3
import yaml
from logger import logger


def db_connection(func):
    """
    Decorator to open and close the connection to the database
    """

    def wrapper(self, *args, **kwargs):
        logger.debug("Opening db connection")
        with sqlite3.connect(self.db_name) as conn:
            func(self, conn, *args, **kwargs)
            logger.debug("Closing db connection")
    return wrapper


class NetflixETL:
    """
    This class is responsible for extracting, transforming and loading the Netflix dataset
    """
    def __init__(self):
        self.config = self.config_init()
        self.db_name: str = self.config["ETL"]["DB"]
        self.netflix_csv: str = self.config["ETL"]["DATASET"]
        self.sql_schema_file: str = self.config["ETL"]["SQL_SCHEMA_FILE"]
        self.netflix_df: pd.DataFrame = None

    def config_init(self):
        """
        Read the config file and set the class attributes
        """
        try:
            with open("config.yaml", "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            logger.exception(f"Config file not found: {e}")

    def extract_data(self):
        """
        Read the netflix csv file and return a dataframe
        """
        logger.info("Extracting data")
        # read the netflix csv file and return a dataframe
        self.netflix_df: pd.DataFrame = pd.read_csv(self.netflix_csv, delimiter=";")

    def transform_data(self) -> pd.DataFrame:
        """
        Process the dataframe
        """
        logger.info("Transforming data")

        # rename the columns
        self.netflix_df.columns = [x.strip().lower().replace(" ", "_") for x in self.netflix_df.columns.to_list()]

        # process dates
        self.netflix_df["join_date"] = pd.to_datetime(
            self.netflix_df["join_date"], dayfirst=True).dt.date
        self.netflix_df["last_payment_date"] = pd.to_datetime(
            self.netflix_df["last_payment_date"], dayfirst=True).dt.date

        # factorize subscription by type, country, plan_duration and montly rev
        sub_ids, _ = pd.factorize(
            self.netflix_df.subscription_type + self.netflix_df.country +
            self.netflix_df.plan_duration + self.netflix_df.monthly_revenue.astype(str)
            )

        # add subscription id to df
        self.netflix_df["subscription_id"] = sub_ids

    @db_connection
    def load_data(self, conn: sqlite3.Connection):
        """
        Load the data into a sqlite database
        """
        logger.info("Loading data")
        self.db_init()

        for row in self.netflix_df.iterrows():

            subscription_id: int = self.insert_subscription(conn, row[1])
            user_id: int = self.insert_user(conn, row[1], subscription_id)
            activity_id: int = self.insert_activity(conn, row[1], user_id, subscription_id)

            logger.debug(f"Inserted user: {user_id}, "
                         f"Subscription: {subscription_id}, "
                         f"Activity: {activity_id}")
        conn.commit()

    def insert_user(self, conn: sqlite3.Connection, row: pd.Series, subscription_id: int) -> int:

        row["subscription_id"] = subscription_id
        cols = ["subscription_id", "join_date",
                "last_payment_date", "age", "gender", "device",
                "active_profiles", "household_profile_ind"]

        user_id = conn.execute(
            f"INSERT INTO users ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))}) RETURNING id",
            row[cols].to_list()
        ).fetchone()[0]

        return user_id

    def insert_subscription(self, conn: sqlite3.Connection, row: pd.Series) -> int:

        cols = ["subscription_type", "country",
                "plan_duration", "monthly_revenue"]

        subscription_id = conn.execute(
            f"""INSERT OR IGNORE INTO subscriptions ({', '.join(cols)})
            VALUES ({', '.join(['?']*len(cols))}) RETURNING id""",
            row[cols].to_list()
        ).fetchone()

        if subscription_id is None:
            logger.debug(f"Subscription already exists: {row[cols].to_list()}")
            subscription_id = conn.execute(
                f"""SELECT id FROM subscriptions WHERE {'AND '.join([f"{col} = ?" for col in cols])}""",
                row[cols].to_list()
            ).fetchone()

        return subscription_id[0]

    def insert_activity(self, conn: sqlite3.Connection, row: pd.Series, user_id: int, subscription_id: int) -> int:

        row["user_id"] = user_id
        row["subscription_id"] = subscription_id
        cols = ["user_id", "subscription_id", "movies_watched", "series_watched"]

        activity_id = conn.execute(
            f"INSERT INTO activity ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))}) RETURNING id",
            row[cols].to_list()
        ).fetchone()[0]

        return activity_id

    @db_connection
    def db_init(self, conn: sqlite3.Connection):
        try:
            with open(self.sql_schema_file, "r") as file:
                conn.executescript(file.read())
        except FileNotFoundError as e:
            logger.exception(f"Schema file not found: {e}")

    def run_etl(self):
        """ Run the ETL process """
        # extract
        self.extract_data()
        # transform
        self.transform_data()
        # load
        self.load_data()
