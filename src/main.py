import pandas as pd
import sqlite3
import time

def extract_data() -> pd.DataFrame:
    # read the netflix csv file and return a dataframe
    df: pd.DataFrame = pd.read_csv('Netflix_dataset.csv', delimiter=';')
    # rename the columns
    df.columns = [x.strip().lower().replace(' ', '_') for x in df.columns.to_list()]
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # process dates
    df['join_date'] = pd.to_datetime(df['join_date'], dayfirst=True)
    df['last_payment_date'] = pd.to_datetime(df['last_payment_date'], dayfirst=True)

    # factorize subscription by type, country, plan_duration and montly rev
    sub_ids, _ = pd.factorize(df.subscription_type + df.country + df.plan_duration + df.monthly_revenue.astype(str))
    
    # add subscription id to df
    df['subscription_id'] = sub_ids
    return df


def load_data(df: pd.DataFrame):
    with sqlite3.connect('netflix.db') as conn:

        # users table
        users_table_data: pd.DataFrame = df[['user_id', 'subscription_id', 'age', 'gender', 'device', 'join_date', 'last_payment_date']]
        users_table_data.rename(columns = {'user_id':'id'}, inplace = True)
        users_table_data.to_sql('users', conn, if_exists='replace', index=False)

        # subscriptions table
        subscriptions_table_data: pd.DataFrame = df[['subscription_id', 'subscription_type', 'country', 'plan_duration', 'monthly_revenue']].drop_duplicates()
        subscriptions_table_data.rename(columns = {'subscription_id':'id'}, inplace = True)
        subscriptions_table_data.to_sql('subscriptions', conn, if_exists='replace', index=False)

        # activity table
        activity_table_data: pd.DataFrame = df[['user_id', 'active_profiles', 'household_profile_ind', 'movies_watched', 'series_watched']]
        activity_table_data.to_sql('activity', conn, if_exists='replace', index=False)

if __name__ == "__main__":
    start = time.time()

    # extract
    netflix_df: pd.DataFrame = extract_data()
    # transform
    process_df: pd.DataFrame = transform_data(netflix_df)
    # load
    load_data(process_df)

    print(f"Completed successfully: {time.time()-start:.4f} seconds")



