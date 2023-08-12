import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from whizzbox import amazon_sites, db_connector, toolkit, config, s3_connector as s3c, gsheet_connector as gc
from whizzbox import custom_errors
from dotenv import load_dotenv
import os
import time
import numpy as np

PROJECT_NAME = 'whiz-amz-cortex-live'
SAMPLE = True  # if it's a sample, no. of sites are limited
SEND_FAIL_EMAIL = True
UPDATE_GSHEET = False
UPLOAD_TO_S3 = True


# noinspection SpellCheckingInspection
def get_service_area_id():
    url = "https://logistics.amazon.in/flex/api/getOperationalRegions"

    payload = ""
    headers = {"cookie": "session-id=260-6428466-7223316; session-id-time=2082787201l; "
                         "session-token=GbElPtRlx37WtbcOToTC3cRl2schxCQbGfw9PermyrbyTyonOdmMyDvN9xAn3Y1u"
                         "urDFHnNpykoR0H9l8R%2Bw0pqKOD14Zwtk4QClUVQpQsIItI6ETeBDN0M5SpFCUku0BGvpR2xSOFJN"
                         "nMUMDfYiflLntxjcLhKYtat1HekaJ2LZEwTlJ3dL6%2Fs%2B9U37Zu8JIQq%2FK7qPG5HFr4SwvqwIoQ"
                         "pyvIMoFI7ZbtIDa1gNSij8nXYZmqmACRQ0FUVbIFHYlcHjlwmkXzg%3D"}

    response = requests.request("GET", url, data=payload, headers=headers)

    data = json.loads(response.text)
    req_data = data
    api_df_raw = pd.DataFrame(req_data)
    api_df = api_df_raw.explode('basicServiceAreas')
    api_df_inter = pd.concat([api_df.drop(['basicServiceAreas'], axis=1),
                              api_df['basicServiceAreas'].apply(pd.Series)], axis=1)

    df_final = pd.concat([api_df_inter.drop(['pickUpLocationAddress', 'pickUpLocation'], axis=1),
                          api_df_inter['pickUpLocationAddress'].apply(pd.Series),
                          api_df_inter['pickUpLocation'].apply(pd.Series)], axis=1)

    final_df = df_final[['defaultStationCode', 'serviceAreaID', 'regionID', 'regionName',
                         'state', 'postalCode', 'longitude', 'latitude', 'active']]
    final_df.columns = ['site_code', 'service_area_id', 'region_id', 'region_name',
                        'state', 'postal_code', 'longitude', 'latitude', 'active']
    return final_df


# noinspection SpellCheckingInspection
def get_drivers_data(yyyy_mm_dd, service_area_id):
    url = "https://logistics.amazon.in/operations/execution/api/summaries"

    querystring = {"localDate": f"{yyyy_mm_dd}", "serviceAreaId": f"{service_area_id}"}

    payload = ""
    headers = {
        "cookie": 'session-id=258-5953668-1245245; ubid-acbin=261-0863908-3910552; '
                  'lc-acbin=en_IN; x-acbin="57E9eKz@7A@To6Vddbaqer5jgdCIqlKfP2T8kM6wKCICy?gDv7vle?RKDpKPgjZK";'
                  ' at-acbin=Atza|IwEBIJk3W45PoKDQRfBKht6R7k4X7GddLPM54_esWjs8dHrH6815nPGyW5DMuqexf1w0bDDz10cOG'
                  'JJEgdOwPR3Us-wmX2ChldzceR7pz7ciQRrZdKYRDV5A-L0kRQRtlbXvJvefFPPYLQUohVZ59kgsrTlSnAMO2MUVSxI8q'
                  'sBOXW2fPqGjf3o3IIx_f3T8ASPkGXUQcS2C9yTpnncFHoyCgAcCfs0dyQBBJzFYedq3XUQLVg; sess-at-acbin'
                  '="ckW6tTbMs1yigrh3KsKaTVeibCPC2YHIFdC5PnL4F+Q="; x-amz-log-portal-locale=en-IN; session'
                  '-id-time=2082787201l; session-token=F8JjOaEew4Id8WCibAXIjaVT8YkIKwPtKpnJQf0jBIrqTbaLb4Y2Kv47'
                  'nJXwvdCTKygovIhu+XF4jOcoVSZ7ImRYrmDOYEUTdlQ6uYK2r9jmnmHdDBLkl/HGcgDFFKrrJidRt61RI4MJ1PGgzhW'
                  'OIWRMV7OAW+QVoHlg45HkGmhxM9AGo2+x2Ef52i5bwyjCBkSruiVfXAjKD+lMC/jz+1xLh2P9+ZIGbPtnFScrRdjrq'
                  'FJEehq9CQ',
        "authority": "logistics.amazon.in",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "referer": f"https://logistics.amazon.in/operations/execution/itineraries?selectedDay={yyyy_mm_dd}"
                   f"&serviceAreaId={service_area_id}",
        "sec-ch-ua": '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/109.0.0.0 Safari/537.36"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    data = json.loads(response.text)
    driver_df = pd.DataFrame(data['transporters'])
    if not driver_df.empty:
        driver_ids = list(driver_df['transporterId'].unique())
        itinerary_df = pd.DataFrame(data['itinerarySummaries'])
        packages_df = pd.DataFrame(data['transporterPackageSummaries']) if data['transporterPackageSummaries'] \
            else pd.DataFrame()
        loaction_df = get_drivers_location(yyyy_mm_dd, service_area_id, driver_ids)
        merged_df = pd.merge(itinerary_df, pd.merge(driver_df, loaction_df, on='transporterId', how='left'),
                             on='transporterId', how='left')
        merged_df['driver_name'] = merged_df['firstName'].str.title() + ' ' + merged_df['lastName'].str.title()
        if not packages_df.empty:
            merged_df_final = pd.merge(merged_df, packages_df[['transporterId', 'packageStatus']],
                                       on='transporterId', how='left')
        else:
            merged_df_final = merged_df.copy()
        df_final = merged_df_final.copy()
    else:
        df_final = pd.DataFrame()
    return df_final


# noinspection SpellCheckingInspection

def get_drivers_location(yyyy_mm_dd, service_area_id, transporter_ids: list):
    url = "https://logistics.amazon.in/operations/execution/api/transporters/locationUpdate"
    payload = {
        "transporterIds": transporter_ids}
    headers = {
        "authority": "logistics.amazon.in",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/json;charset=UTF-8",
        "cookie": 'session-id=258-5953668-1245245; ubid-acbin=261-0863908-3910552; '
                  'lc-acbin=en_IN; x-acbin="57E9eKz@7A@To6Vddbaqer5jgdCIqlKfP2T8kM6wKCICy?gDv7vle?RKDpKPgjZK";'
                  ' at-acbin=Atza|IwEBIJk3W45PoKDQRfBKht6R7k4X7GddLPM54_esWjs8dHrH6815nPGyW5DMuqexf1w0bDDz10cOG'
                  'JJEgdOwPR3Us-wmX2ChldzceR7pz7ciQRrZdKYRDV5A-L0kRQRtlbXvJvefFPPYLQUohVZ59kgsrTlSnAMO2MUVSxI8q'
                  'sBOXW2fPqGjf3o3IIx_f3T8ASPkGXUQcS2C9yTpnncFHoyCgAcCfs0dyQBBJzFYedq3XUQLVg; sess-at-acbin'
                  '="ckW6tTbMs1yigrh3KsKaTVeibCPC2YHIFdC5PnL4F+Q="; x-amz-log-portal-locale=en-IN; session'
                  '-id-time=2082787201l; session-token=F8JjOaEew4Id8WCibAXIjaVT8YkIKwPtKpnJQf0jBIrqTbaLb4Y2Kv47'
                  'nJXwvdCTKygovIhu+XF4jOcoVSZ7ImRYrmDOYEUTdlQ6uYK2r9jmnmHdDBLkl/HGcgDFFKrrJidRt61RI4MJ1PGgzhW'
                  'OIWRMV7OAW+QVoHlg45HkGmhxM9AGo2+x2Ef52i5bwyjCBkSruiVfXAjKD+lMC/jz+1xLh2P9+ZIGbPtnFScrRdjrq'
                  'FJEehq9CQ', "origin": "https://logistics.amazon.in",
        "referer": f"https://logistics.amazon.in/operations/execution/itineraries?selectedDay={yyyy_mm_dd}"
                   f"&serviceAreaId={service_area_id}",
        "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "'Windows'",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/110.0.0.0 Safari/537.36"
    }

    response = requests.request("POST", url, json=payload, headers=headers)
    data = json.loads(response.text)
    return pd.DataFrame(data['transportersLocation'])


def manipulate_the_data(input_df):
    df_raw = input_df.copy()
    df_inter = df_raw.astype(object)
    df_inter.fillna('No Data', inplace=True)
    df = pd.concat([df_inter.drop(['stopProgress', 'packageStatus'], axis=1),
                    df_inter['stopProgress'].apply(pd.Series), df_inter['packageStatus'].apply(pd.Series)], axis=1)
    df['amazon_login_time'] = df['itineraryStartTime']. \
        apply(lambda x: datetime.fromtimestamp(int(str(x)[:10])) if x != 'No Data' else None)
    df['location_updated_time'] = df['epochTimestamp']. \
        apply(lambda x: datetime.fromtimestamp(int(str(x)[:10])) if x != 'No Data' else None)
    df['lastDriverEventTime'] = df['lastDriverEventTime']. \
        apply(lambda x: datetime.fromtimestamp(int(str(x)[:10])) if x != 'No Data' else None)
    df['inactive_since'] = np.where(df['location_updated_time'] >= df['lastDriverEventTime'],
                                    df['location_updated_time'],
                                    df['lastDriverEventTime'])
    df['inactive_since'].fillna(df['location_updated_time'], inplace=True)
    df['inactive_hours'] = df['inactive_since']. \
        apply(lambda x: round((datetime.now() - x).total_seconds() / 3600, 2) if x != 'No Data' else x)
    # df['inactive_hours'] = np.where((df['executionStatus'] == 'COMPLETE') | (df['driverSessionEnded'] is True),
    #                                 0, df['inactive_hours'])
    df['planned_end_time'] = df['timeRemainingSecs']. \
        apply(lambda x: datetime.now() + timedelta(seconds=x + 3) if x != 'No Data' else x)
    df['stops_assigned'] = df['total']
    df['stops_completed'] = np.where(df['actionedTimeWindowed'] == 0, df['completed'],
                                     df['total'] - df['notStarted'] - df['inProgress'])
    df['stops_at_risk'] = df['stopsAndPackagesByTaskAssessment']. \
        apply(lambda x: x['AT_RISK']['stopsImpacted'] if x else 'No Data')
    df['pkgs_at_risk'] = df['stopsAndPackagesByTaskAssessment']. \
        apply(lambda x: x['AT_RISK']['packagesImpacted'] if x else 'No Data')
    df['stops_ahead'] = df['stopsAndPackagesByTaskAssessment']. \
        apply(lambda x: x['AHEAD']['stopsImpacted'] if x else 'No Data')
    df['pkgs_ahead'] = df['stopsAndPackagesByTaskAssessment']. \
        apply(lambda x: x['AHEAD']['packagesImpacted'] if x else 'No Data')

    df['shift_hours_remaining'] = df['timeRemainingSecs']. \
        apply(lambda x: round(x / 3600, 2) if x != 'No Data' else x)

    req_cols = ['updated_timestamp', 'station_code', 'transporterId', 'driver_name', 'amazon_login_time',
                'planned_end_time', 'shift_hours_remaining', 'inactive_since', 'location_updated_time',
                'lastDriverEventTime', 'inactive_hours', 'executionStatus', 'driverSessionEnded',
                'stops_assigned', 'stops_completed', 'progressStatus', 'stops_at_risk', 'pkgs_at_risk',
                'stops_ahead', 'pkgs_ahead', 'totalPackages', 'UNASSIGNED', 'DELIVERED', 'REMAINING',
                'REATTEMPTABLE', 'UNDELIVERABLE', 'RETURNED']
    req_cols.remove('location_updated_time')
    req_cols.remove('lastDriverEventTime')

    for col in req_cols:
        if col not in df.columns:
            df[col] = 0

    final_df = df[req_cols]

    final_df = final_df.rename(columns={'driverSessionEnded': 'driver_session_ended',
                                        'transporterId': 'transporter_id',
                                        'executionStatus': 'trip_status',
                                        'lastDriverEventTime': 'last_driver_event_time',
                                        'progressStatus': 'progress_status',
                                        'totalPackages': 'pkgs_assigned',
                                        'UNASSIGNED': 'pkgs_unassigned',
                                        'DELIVERED': 'pkgs_delivered',
                                        'REMAINING': 'pkgs_remaining',
                                        'REATTEMPTABLE': 'pkgs_reattemptable',
                                        'UNDELIVERABLE': 'pkgs_undeliverable',
                                        'RETURNED': 'pkgs_rts_done'})

    return final_df


def split_the_df(input_df):
    df = input_df.copy()
    date_cols = ['amazon_login_time', 'inactive_since', 'planned_end_time']

    for col in date_cols:
        df[col] = df[col].apply(lambda x: x.strftime("%d/%m/%Y %H:%M") if x != 'No Data' else x)

    df['progress_status'] = df['progress_status'].str.replace('_', ' ').str.title()

    inactive_df_cols = ['station_code', 'updated_timestamp', 'transporter_id', 'driver_name',
                        'progress_status', 'amazon_login_time', 'inactive_since', 'shift_hours_remaining',
                        'stops_assigned', 'stops_completed', 'stops_at_risk', 'pkgs_at_risk',
                        'om_name', 'rm_name', 'client']
    inactive_drivers = df[(df.inactive_hours >= 1) & (df.trip_status == 'DEPARTED')][inactive_df_cols].\
        reset_index(drop=True)

    not_departed_df_cols = ['station_code', 'updated_timestamp', 'transporter_id', 'driver_name', 'amazon_login_time',
                            'stops_assigned', 'stops_completed', 'om_name', 'rm_name', 'client']
    not_departed_drivers = df[df.trip_status == 'NOT_DEPARTED'][not_departed_df_cols].reset_index(drop=True)
    behinders_cols = ['station_code', 'updated_timestamp', 'transporter_id', 'driver_name', 'amazon_login_time',
                      'planned_end_time', 'shift_hours_remaining', 'inactive_hours', 'stops_assigned',
                      'stops_completed', 'stops_at_risk', 'om_name', 'rm_name', 'client']
    behinders = df[df.stops_at_risk != 'No Data']
    behinders = behinders[behinders.stops_at_risk >= 20][behinders_cols].reset_index(drop=True)

    deliveries_cols = ['station_code', 'updated_timestamp', 'transporter_id', 'driver_name', 'amazon_login_time',
                       'pkgs_delivered', 'pkgs_rts_done', 'pkgs_reattemptable', 'stops_assigned',
                       'stops_completed', 'om_name', 'rm_name', 'client']

    deliveries_df = df[deliveries_cols]
    deliveries_df = deliveries_df.replace('', np.nan).fillna(0)

    return {'inactive_drivers_df': inactive_drivers,
            'not_departed_drivers_df': not_departed_drivers,
            'behinders_df': behinders,
            'deliveries_df': deliveries_df}

def get_current_drivers_data(sample):
    current_datetime = datetime.now(config.tz)
    date_str = current_datetime.strftime("%Y-%m-%d")
    amazon_sites_df_raw = amazon_sites.create_amazon_sites_df(db=db_connector.connect_to_db(db_name='whizzard'))
    amz_sites_fpath = f'/home/ubuntu/atom/{PROJECT_NAME}/amazon_sites.xlsx' \
        if config.ON_SERVER else f'../{PROJECT_NAME}/amazon_sites.xlsx'
    amazon_sites_df = toolkit.save_or_retrieve_df_excel(input_df=amazon_sites_df_raw, fpath=amz_sites_fpath)
    service_area_id_df = get_service_area_id()
    sites_service_id_df = pd.merge(toolkit.snake_case_the_cols(amazon_sites_df),
                                   service_area_id_df[['site_code', 'service_area_id']],
                                   on='site_code', how='left')

    stn_code_id_list = sites_service_id_df[['site_code', 'service_area_id']].to_numpy().tolist()
    print(f'Total Number of Sites found : {len(stn_code_id_list)}')
    stn_code_id_list = [row for row in stn_code_id_list if row[0] in ['HYDC', 'HYBH']] if sample else stn_code_id_list
    print(f'Sample Size (Number of Sites) : {len(stn_code_id_list)}') if sample else print(end='')
    drivers_df_list = []
    num = 0
    for stn, s_area_id in stn_code_id_list:
        num += 1
        try:
            drivers_df = get_drivers_data(yyyy_mm_dd=date_str, service_area_id=s_area_id)
            if not drivers_df.empty:
                drivers_df.insert(loc=0, column='date', value=date_str)
                drivers_df.insert(loc=1, column='updated_timestamp', value=current_datetime.strftime("%d/%m/%Y %H:00"))
                drivers_df.insert(loc=2, column='station_code', value=stn)
                drivers_df_list.append(drivers_df)
                print(f'{num}. {stn} - success', end=' ')
            else:
                print(f'{num}. {stn} - no_data', end=' ')
        except Exception as err:
            error_name = type(err).__name__
            print(f'{num}. {stn} - {error_name}', end=' ')
        if num % 8 == 0:
            print()
    print()

    if drivers_df_list:
        defaulters_df_raw = pd.concat(drivers_df_list)
    else:
        raise custom_errors.NoDataError

    defaulters_df = manipulate_the_data(input_df=defaulters_df_raw)

    site_details_df_raw = amazon_sites_df.drop(columns='Client Site Code')
    site_details_df = site_details_df_raw.rename(columns={'Site Code': 'station_code'})
    site_details_df = toolkit.snake_case_the_cols(input_df=site_details_df)
    defaulters_df_final = pd.merge(defaulters_df, site_details_df, on='station_code', how='left')
    splitted_dfs = split_the_df(input_df=defaulters_df_final)
    # splitted_dfs['inactive_drivers_df'].columns = ['Site', 'Updated Time', 'Driver ID', 'Name', 'Status',
    #                                                'Amz Login Time', 'Inactive From', 'Shift Hours Left',
    #                                                'Total Stops', 'Completed Stops', 'Stops at Risk',
    #                                                'Packages at Risk', 'OM', 'RM', 'Client']
    # splitted_dfs['not_departed_drivers_df'].columns = ['Site', 'Updated Time', 'Driver ID', 'Name', 'Amz Login Time',
    #                                                    'Total Stops', 'Completed Stops', 'OM', 'RM', 'Client']
    # splitted_dfs['behinders_df'].columns = ['Site', 'Updated Time', 'Driver ID', 'Name', 'Amz Login Time',
    #                                         'Shift Ends at', 'Shift Hours Left', 'Inactive Hours', 'Total Stops',
    #                                         'Completed Stops', 'Stops at Risk', 'OM', 'RM', 'Client']
    # splitted_dfs['deliveries_df'].columns = ['Site', 'Updated Time', 'Driver ID', 'Name', 'Amz Login Time',
    #                                          'Delivered', 'RTS Done', 'Reattemptable', 'Total Stops',
    #                                          'Completed Stops', 'OM', 'RM', 'Client']
    return {'defaulters_df_final': defaulters_df_final,
            'defaulters_df_raw': defaulters_df_raw,
            'inactive_drivers_df': splitted_dfs['inactive_drivers_df'],
            'not_departed_drivers_df': splitted_dfs['not_departed_drivers_df'],
            'behinders_df': splitted_dfs['behinders_df'],
            'deliveries_df': splitted_dfs['deliveries_df']}


if __name__ == '__main__':
    load_dotenv()
    data_folderpath = toolkit.create_folder(projectname=PROJECT_NAME, foldername='C:\\Users\\Admin\\Desktop\\whizzard\\whiz-amz-cortex-live')
    final_output_fname = f'{datetime.now(config.tz).strftime("%Y-%m-%d_AmazonCortexLive_%HHrs")}.xlsx'
    final_output_fpath = data_folderpath + '/' + final_output_fname
    t1 = time.time()  # execution start time for the python script

    google_creds_fpath = f'/home/ubuntu/atom/{PROJECT_NAME}/google_account_credentials.json' if config.ON_SERVER \
        else f'/Users/Admin/PycharmProjects/{PROJECT_NAME}/google_account_credentials.json'

    try:
        print('\n--------------------***--------------------\n')
        print(f'Execution Started at: {datetime.now(config.tz).strftime("%Y-%m-%d %H:%M:%S")}')
        final_dfs = get_current_drivers_data(sample=SAMPLE)
        with pd.ExcelWriter(final_output_fpath, engine=None) as writer:
            # final_dfs['defaulters_df_raw'].to_excel(writer, sheet_name='Raw', index=False)
            # final_dfs['defaulters_df_final'].to_excel(writer, sheet_name='Data', index=False)
            final_dfs['inactive_drivers_df'].to_excel(writer, sheet_name='Inactive', index=False)
            final_dfs['not_departed_drivers_df'].to_excel(writer, sheet_name='Not Departed', index=False)
            final_dfs['behinders_df'].to_excel(writer, sheet_name='Behinders', index=False)
            final_dfs['deliveries_df'].to_excel(writer, sheet_name='Packages', index=False)
            # df_raw.to_excel(writer, sheet_name='Raw-Data', index=False)

        s3_foldername = 'whiz-amz-cortex-live'
        s3_storage = s3c.connect_to_s3_storage(os.getenv('AWS_ACCESS_KEY_ID'),
                                               os.getenv('AWS_SECRET_ACCESS_KEY'))
        atom_bucket = s3_storage.Bucket('atom-s3')  # selecting a bucket from the s3 storage

        if not SAMPLE and UPLOAD_TO_S3:
            s3c.upload_to_s3(atom_bucket, s3_foldername, final_output_fpath, final_output_fname)

        req_excel_files = sorted(s3c.get_all_excels(connected_bucket=atom_bucket, folder_name=s3_foldername),
                                 reverse=True)[:40]
        history_pkgs_df = s3c.concat_excel_sheets_to_df(connected_bucket=atom_bucket,
                                                        excel_files=req_excel_files,
                                                        sheet_num='Packages')

        if UPDATE_GSHEET:
            gc.upload_df_to_gsheets(creds_fpath=google_creds_fpath,
                                    dataframe=final_dfs['inactive_drivers_df'],
                                    file_name="amazon-cortex-live-data",
                                    sheet_name="Inactive", formatting=False)
            gc.upload_df_to_gsheets(creds_fpath=google_creds_fpath,
                                    dataframe=final_dfs['not_departed_drivers_df'],
                                    file_name="amazon-cortex-live-data",
                                    sheet_name="Not Departed", formatting=False)
            gc.upload_df_to_gsheets(creds_fpath=google_creds_fpath,
                                    dataframe=final_dfs['behinders_df'],
                                    file_name="amazon-cortex-live-data",
                                    sheet_name="Behinders", formatting=False)
            gc.upload_df_to_gsheets(creds_fpath=google_creds_fpath,
                                    dataframe=final_dfs['deliveries_df'],
                                    file_name="amazon-cortex-live-data",
                                    sheet_name="Packages", formatting=False)
            gc.upload_df_to_gsheets(creds_fpath=google_creds_fpath,
                                    dataframe=history_pkgs_df,
                                    file_name="amazon-cortex-live-data",
                                    sheet_name="History - Packages", formatting=False)

        print(f'Total Time for the Complete Execution : {(time.time() - t1) / 60:.3f} minutes')
        print(f'Execution Completed at: {datetime.now(config.tz).strftime("%Y-%m-%d %H:%M:%S")}')

    except Exception as e:
        message = f'Error occured while generating the report!\nError:{type(e).__name__, e}'
        print(message)
        subj = f'Failed: {final_output_fname.replace(".csv", "")}'
        toolkit.send_failure_email(send=SEND_FAIL_EMAIL, from_email=os.getenv('EMAIL_ID'),
                                   pwd=os.getenv('EMAIL_PASSWORD'),
                                   receiver_email='jitesh.jadhao@whizzard.in',
                                   email_subject=subj, email_message=message)
