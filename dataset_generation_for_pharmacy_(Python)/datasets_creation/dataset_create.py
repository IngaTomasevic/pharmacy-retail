import openai
from openai import OpenAI
import pandas as pd
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = api_key
client = OpenAI()


def call_gpt(request, role="user", mode='gpt-3.5-turbo') -> str:
    """
    Function calling GPT to get  the answer in the text form.
    WARNING: call as precise as possible, to get only words(w/o any description).
    E.g. good request='Generate 100 english full names without any description'.
    Returns: str
    """
    completion = client.chat.completions.create(
        model=mode, messages=[{"role": role, "content": request}]
    )
    return completion.choices[0].message.content


def generate_products_hierarchy(ctg_label_singular: str, sub_ctg_label_plural: str,
                                ctg_name: str, times_from, times_to):
    """
    Function calling GPT to get number of child subcategory label names for the given category label name.
    Useful samples:
    - Find {5} {subcategories} for {antibiotic} {category},
    - Find {10} {products} for {sedative} {subcategory}.
    Returns: list
    """
    n = random.randint(times_from, times_to)
    subcategories = sub_ctg_label_plural
    category = ctg_label_singular
    category_name = ctg_name
    request = (f'Generate {n} {subcategories} for {category_name} {category} without any description. /'
               f'Generate concise {subcategories} without any additional description. /'
               f'Provide the result as enumerated list of values /'
               f'each starting with a new line. Please do not provide empty values.')
    subcategories_str = call_gpt(request)
    subcategories_list = [w.strip('0123456789. ') for w in subcategories_str.split('\n')]
    return subcategories_list


def join_csv_files_one_unique(path1: str, path2: str, times_from=1, times_to=1, unit=False):
    """ Rand_num: random number from the given range (times_from, times_to)
    Each row from path1 will be duplicated random times and
    joined by axis=1 to the unique rows from path 2.
    If unit: adds unit_num column between 2 joined parts
    Useful sample 1: one sale contains several positions (product units).
    Useful sample 2: same medication can be presented by several different brands.
    Returns: DataFrame"""
    path1_df = pd.read_csv(path1)
    path2_df = pd.read_csv(path2)

    dfs_list = []
    for idx in range(len(path1_df)):
        rand_num = random.randint(times_from, times_to)
        if rand_num == 0:
            continue

        path1_single_row = pd.DataFrame(path1_df.loc[[idx]])
        path1_duplicated_df = pd.concat([path1_single_row] * rand_num, ignore_index=True)

        path2_df_rand_idx = random.sample(range(len(path2_df)), rand_num)
        path2_random_df = pd.DataFrame(path2_df.loc[path2_df_rand_idx])
        path2_random_df.index = range(len(path1_duplicated_df))

        if unit:
            unit_dict = {'unit_num': [x for x in range(1, rand_num + 1)]}
            unit_df = pd.DataFrame(unit_dict)
            joined_df = pd.concat([path1_duplicated_df, unit_df, path2_random_df], axis=1)
        else:
            joined_df = pd.concat([path1_duplicated_df, path2_random_df], axis=1)

        dfs_list.append(joined_df)

    if dfs_list:
        return pd.concat(dfs_list)


def join_data_frames_one_unique(df1: pd.DataFrame, df2: pd.DataFrame, times_from=1, time_to=1, unit=False):
    """ Does the same as join_csv_files_one_unique but with Date frames instead of csv files.
    Returns: DataFrame"""
    dfs_list = []
    for idx in range(len(df1)):
        rand_num = random.randint(times_from, time_to)

        if rand_num == 0:
            continue

        try:
            single_row_series = df1.loc[[idx]]
            df1_single_row = pd.DataFrame(single_row_series)
            df1_duplicated = pd.concat([df1_single_row] * rand_num)
            df2_rand_idx = random.sample(range(len(df2)), rand_num)
            df2_random = pd.DataFrame(df2.loc[df2_rand_idx])
            df1_duplicated.index = range(len(df1_duplicated))
            df2_random.index = range(len(df1_duplicated))

            if unit:
                unit_dict = {'unit_num': [x for x in range(1, rand_num + 1)]}
                unit_df = pd.DataFrame(unit_dict)
                joined_df = pd.concat([df1_duplicated, unit_df, df2_random], axis=1)
            else:
                joined_df = pd.concat([df1_duplicated, df2_random], axis=1)

            dfs_list.append(joined_df)
        except KeyError:
            continue

    if dfs_list:
        return pd.concat(dfs_list)


def duplicate_rows(path: str, times_from=1, time_to=1):
    """Duplicates each row random times.
    Useful sample: prepare from n to m sales rows for each day.
    Returns DataFrame"""
    path_df = pd.read_csv(path)
    new_dfs = []
    for idx in range(len(path_df)):
        random_num = random.randint(times_from, time_to)
        single_row_df = pd.DataFrame(path_df.loc[[idx]])
        duplicated_df = pd.concat([single_row_df] * random_num, ignore_index=True)
        new_dfs.append(duplicated_df)

    if new_dfs:
        return pd.concat(new_dfs)


def subsequent_days(year_from, year_to):
    """Generated dataframe of all subsequent days for given range of years.
    Returns: DataFrame"""
    start_day = datetime(year_from, 1, 1)
    end_day = datetime(year_to + 1, 1, 1)

    days_dict = {'day': []}

    current_date = start_day
    while current_date < end_day:
        days_dict['day'].append(current_date)
        current_date += timedelta(days=1)

    df_days = pd.DataFrame(days_dict)
    return df_days


def subsequent_times(time_from=0, time_to=23):
    """Generates dataframe of all times (precision: seconds) in the specified range of hours.
    Returns: DataFrame"""
    time_dict = {'time': []}
    time_start = datetime(100, 1, 1, time_from, 0, 0)
    time_end = datetime(100, 1, 1, time_to - 1, 59, 59)

    current_time = time_start
    while current_time <= time_end:
        time_dict['time'].append(current_time.time())
        td = timedelta(0, seconds=1)
        current_time += td

    df_times = pd.DataFrame(time_dict)
    return df_times


def group_by_count(file_path: str, *attributes: str):
    """Checks rows number in csv file by given attribute ("Group by" counting).
    Useful when it's needed to check if random generated values are more or less natural
    (not equally spread). If not-> use delete_attribute_rows function.
    Returns: DataFrame"""
    df = pd.read_csv(file_path)
    grouped_series = df.groupby(list(*attributes)).size()
    grouped_df = pd.DataFrame(grouped_series)
    return grouped_df


def delete_attribute_rows(path: str, attribute: str, range_from, range_to):
    """For each given attribute value drops random number of rows in the specified range.
    The more difference in rows number you want to achieve - the more range from/to specify.
    Goal: make dataset more 'natural', when it looks too artificial.
    Returns: DataFrame"""
    local_path_df = pd.read_csv(path)
    unique_attr_values = set(local_path_df[attribute].values)

    for value in unique_attr_values:
        rand_num = random.randint(range_from, range_to)
        locked_df = local_path_df.loc[local_path_df[attribute] == value]
        grouped_df_idx_list = list(locked_df.index.values)
        try:
            random_idx_list = random.sample(grouped_df_idx_list, rand_num)
        except ValueError:
            continue
        local_path_df.drop(random_idx_list, inplace=True)

    return local_path_df
