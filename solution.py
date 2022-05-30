import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
raw = ROOT / 'example_data_2019_v1.csv'


def clean_df(df):
    df = df.drop(columns=['Category'])
    df.Action_time = pd.DataFrame.astype(df.Action_time, "datetime64[ns]")
    return df


def divede_df_by_user(df):
    users = df.User_ID.unique().tolist()
    return [df[df.User_ID == user] for user in users]


def clean_user_df(df):
    df = df.sort_values(by='Action_time')
    df['delta'] = df.Action_time.diff().astype('timedelta64[s]')
    df['new_session'] = df.delta.apply(lambda x: True if x > 300 else False)
    df['one_session'] = df.delta.apply(lambda x: True if x > 60 else False)
    df.iloc[0, -1] = True
    return df

def divide_df_by_session(df):
    session_dfs = []
    sessions = df[df.new_session == True].index.tolist()
    i = 0
    for session in sessions:
        session_dfs.append(df[(df.index >= i) & (df.index < session)])
        i = session
    return session_dfs


def get_session_lengths(session_df):
    return session_df.delta.sum()


def get_result(all_df):
    final = []
    i = 0
    for df in all_df:
        df = clean_user_df(df)
        session_dfs = divide_df_by_session(df)
        for session_df in session_dfs:
            session_df['session_duration'] = session_df.delta.sum()
            session_df['session_duration'] = session_df.delta.sum()
            session_df['count_total_url'] = session_df.URL.nunique()
            session_df['count_unique_url'] = session_df.URL.unique().size
            session_df['session_number'] = i
            i += 1
            session_df.drop(columns=['delta', 'Action_time', 'URL', 'new_session', 'one_session'], inplace=True)
            final.append(session_df)
    return final

def main():
    df = pd.read_csv(raw)
    df = clean_df(df)
    all_df = divede_df_by_user(df)
    result = get_result(all_df)
    result = pd.concat(result)
    result.to_csv(ROOT / 'result.csv')

if __name__ == '__main__':
    main()

    
