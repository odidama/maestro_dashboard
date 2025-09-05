from functools import cache
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


@cache
def get_static_files(f_type):
    curr_dir = Path(__file__).parent
    folder_path = curr_dir / 'data'
    file_list = os.listdir(folder_path)
    for filename in file_list:
        file_path = os.path.join(folder_path, filename)
        extension = os.path.splitext(file_path)[1]
        if f_type == 'xls' and extension.lower() == '.xls':
            xls_df = pd.read_excel(file_path)
            return xls_df
        elif f_type == 'csv' and extension.lower() == '.csv':
            csv_df = pd.read_csv(file_path, encoding='latin-1')
            return csv_df
        else:
            pass


def transform_df():
    df = get_static_files('csv')

    rename_columns = {'CENSUS_YEAR': 'CENSUS_YEAR', 'GEO_NAME': 'REGION_NAME', 'GEO_LEVEL': 'REGION_TYPE',
                      'CHARACTERISTIC_NAME': 'DIMENSIONS', 'C1_COUNT_TOTAL': 'TOTAL_COUNT',
                      'C2_COUNT_MEN+': 'TOTAL_COUNT_MEN', 'C3_COUNT_WOMEN+': 'TOTAL_COUNT_WOMEN'}

    # req_columns = [0, 2, 3, 4, 9, 11, 13, 15]

    req_columns = ['CENSUS_YEAR',
                   'REGION_NAME',
                   'REGION_TYPE',
                   'DIMENSIONS',
                   'TOTAL_COUNT',
                   'TOTAL_COUNT_MEN',
                   'TOTAL_COUNT_WOMEN']

    df.rename(columns=rename_columns, inplace=True)
    df = df[req_columns]

    can_df = df.loc[df['REGION_NAME'] == 'Canada'][req_columns]
    can_df_agGrp = df.loc[df['REGION_NAME'] == 'Canada'].iloc[8:33]
    can_df_summary = df.loc[df['REGION_NAME'] == 'Canada'].iloc[0:7]

    nfl_df = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador'][req_columns]
    nfl_df_agGrp = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador'].iloc[8:33]
    nfl_df_summary = df.loc[df['REGION_NAME'] == 'Newfoundland and Labrador'].iloc[0:7]

    pei_df = df.loc[df['REGION_NAME'] == 'Prince Edward Island'][req_columns]
    pei_df_agGrp = df.loc[df['REGION_NAME'] == 'Prince Edward Island'].iloc[8:33]
    pei_df_summary = df.loc[df['REGION_NAME'] == 'Prince Edward Island'].iloc[0:7]

    ns_df = df.loc[df['REGION_NAME'] == 'Nova Scotia'][req_columns]
    ns_df_agGrp = df.loc[df['REGION_NAME'] == 'Nova Scotia'].iloc[8:33]
    ns_df_summary = df.loc[df['REGION_NAME'] == 'Nova Scotia'].iloc[0:7]

    nb_df = df.loc[df['REGION_NAME'] == 'New Brunswick'][req_columns]
    nb_df_agGrp = df.loc[df['REGION_NAME'] == 'New Brunswick'].iloc[8:33]
    nb_df_summary = df.loc[df['REGION_NAME'] == 'New Brunswick'].iloc[0:7]

    qu_df = df.loc[df['REGION_NAME'] == 'Quebec'][req_columns]
    qu_df_agGrp = df.loc[df['REGION_NAME'] == 'Quebec'].iloc[8:33]
    qu_df_summary = df.loc[df['REGION_NAME'] == 'Quebec'].iloc[0:7]

    on_df = df.loc[df['REGION_NAME'] == 'Ontario'][req_columns]
    on_df_agGrp = df.loc[df['REGION_NAME'] == 'Ontario'].iloc[8:33]
    on_df_summary = df.loc[df['REGION_NAME'] == 'Ontario'].iloc[0:7]

    mb_df = df.loc[df['REGION_NAME'] == 'Manitoba'][req_columns]
    mb_df_agGrp = df.loc[df['REGION_NAME'] == 'Manitoba'].iloc[8:33]
    mb_df_summary = df.loc[df['REGION_NAME'] == 'Manitoba'].iloc[0:7]

    sk_df = df.loc[df['REGION_NAME'] == 'Saskatchewan'][req_columns]
    sk_df_agGrp = df.loc[df['REGION_NAME'] == 'Saskatchewan'].iloc[8:33]
    sk_df_summary = df.loc[df['REGION_NAME'] == 'Saskatchewan'].iloc[0:7]

    ab_df = df.loc[df['REGION_NAME'] == 'Alberta'][req_columns]
    ab_df_AgGrp = df.loc[df['REGION_NAME'] == 'Alberta'].iloc[8:33]
    ab_df_summary = df.loc[df['REGION_NAME'] == 'Alberta'].iloc[0:7]

    bc_df = df.loc[df['REGION_NAME'] == 'British Columbia'][req_columns]
    bc_df_agGrp = df.loc[df['REGION_NAME'] == 'British Columbia'].iloc[8:33]
    bc_df_summary = df.loc[df['REGION_NAME'] == 'British Columbia'].iloc[0:7]

    yk_df = df.loc[df['REGION_NAME'] == 'Yukon'][req_columns]
    yk_df_agGrp = df.loc[df['REGION_NAME'] == 'Yukon'].iloc[8:33]
    yk_df_summary = df.loc[df['REGION_NAME'] == 'Yukon'].iloc[0:7]

    nwt_df = df.loc[df['REGION_NAME'] == 'Northwest Territories'][req_columns]
    nwt_df_agGrp = df.loc[df['REGION_NAME'] == 'Northwest Territories'].iloc[8:33]
    nwt_df_summary = df.loc[df['REGION_NAME'] == 'Northwest Territories'].iloc[0:7]

    nu_df = df.loc[df['REGION_NAME'] == 'Nunavut'][req_columns]
    nu_df_agGrp = df.loc[df['REGION_NAME'] == 'Nunavut'].iloc[8:33]
    nu_df_summary = df.loc[df['REGION_NAME'] == 'Nunavut'].iloc[0:7]

    all_AgeGrp_Data_combined = pd.concat([can_df_agGrp, nfl_df_agGrp, pei_df_agGrp, ns_df_agGrp, nb_df_agGrp,
                                          qu_df_agGrp, on_df_agGrp, mb_df_agGrp, nb_df_agGrp, sk_df_agGrp, ab_df_AgGrp,
                                          bc_df_agGrp, yk_df_agGrp, nwt_df_agGrp, nu_df_agGrp], axis=0)

    all_Df_Summary_Combined = pd.concat([can_df_summary, nfl_df_summary, pei_df_summary, ns_df_summary, nb_df_summary,
                                         qu_df_summary, on_df_summary, mb_df_summary, nb_df_summary, sk_df_summary,
                                         ab_df_summary, bc_df_summary, yk_df_summary, nwt_df_summary,
                                         nu_df_summary], axis=0)

    return all_AgeGrp_Data_combined, all_Df_Summary_Combined
