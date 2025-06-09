import xml.etree.ElementTree as ET
import pandas as pd


def parse_apple_health(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    records = []
    workouts = []
    activity_summaries = []
    heart_rates = []
    resting_heart_rates = []
    sleep_analysis = []

    for record in root.findall(".//Record"):
        attributes = record.attrib
        records.append({
            'type': attributes.get('type', ''),
            'sourceName': attributes.get('sourceName', ''),
            'unit': attributes.get('unit', ''),
            'creationDate': attributes.get('creationDate', ''),
            'startDate': attributes.get('startDate', ''),
            'endDate': attributes.get('endDate', ''),
            'value': attributes.get('value', '')
        })

        if attributes.get('type') == 'HKQuantityTypeIdentifierHeartRate':
            heart_rates.append({
                'sourceName': attributes.get('sourceName', ''),
                'creationDate': attributes.get('creationDate', ''),
                'startDate': attributes.get('startDate', ''),
                'endDate': attributes.get('endDate', ''),
                'value': attributes.get('value', ''),
                'unit': attributes.get('unit', '')
            })

        if attributes.get('type') == 'HKQuantityTypeIdentifierRestingHeartRate':
            resting_heart_rates.append({
                'sourceName': attributes.get('sourceName', ''),
                'creationDate': attributes.get('creationDate', ''),
                'startDate': attributes.get('startDate', ''),
                'endDate': attributes.get('endDate', ''),
                'value': attributes.get('value', ''),
                'unit': attributes.get('unit', '')
            })

        if attributes.get('type') == 'HKCategoryTypeIdentifierSleepAnalysis':
            sleep_analysis.append({
                'sourceName': attributes.get('sourceName', ''),
                'creationDate': attributes.get('creationDate', ''),
                'startDate': attributes.get('startDate', ''),
                'endDate': attributes.get('endDate', ''),
                'value': attributes.get('value', '')
            })

    for workout in root.findall(".//Workout"):
        attributes = workout.attrib
        workouts.append({
            'workoutActivityType': attributes.get('workoutActivityType', ''),
            'duration': attributes.get('duration', ''),
            'durationUnit': attributes.get('durationUnit', ''),
            'totalEnergyBurned': attributes.get('totalEnergyBurned', ''),
            'totalEnergyBurnedUnit': attributes.get('totalEnergyBurnedUnit', ''),
            'creationDate': attributes.get('creationDate', ''),
            'startDate': attributes.get('startDate', ''),
            'endDate': attributes.get('endDate', '')
        })

    for summary in root.findall(".//ActivitySummary"):
        attributes = summary.attrib
        activity_summaries.append({
            'activeEnergyBurned': attributes.get('activeEnergyBurned', ''),
            'activeEnergyBurnedGoal': attributes.get('activeEnergyBurnedGoal', ''),
            'appleExerciseTime': attributes.get('appleExerciseTime', ''),
            'appleExerciseTimeGoal': attributes.get('appleExerciseTimeGoal', ''),
            'appleStandHours': attributes.get('appleStandHours', ''),
            'appleStandHoursGoal': attributes.get('appleStandHoursGoal', ''),
            'dateComponents': attributes.get('dateComponents', '')
        })

    df_records = pd.DataFrame(records)
    df_workouts = pd.DataFrame(workouts)
    df_activity_summaries = pd.DataFrame(activity_summaries)
    df_heart_rates = pd.DataFrame(heart_rates)
    df_resting_heart_rate = pd.DataFrame(resting_heart_rates)
    df_sleep_analysis = pd.DataFrame(sleep_analysis)

    return df_records, df_workouts, df_activity_summaries, df_heart_rates, df_resting_heart_rate, df_sleep_analysis


def sleep_analysis(df_sleep_analysis: pd.DataFrame) -> pd.DataFrame:
    df_sleep_analysis["sourceName"] = df_sleep_analysis["sourceName"].str.replace("Apple\xa0Watch di Valerio", "Apple Watch")

    df_sleep_analysis_filt = df_sleep_analysis[df_sleep_analysis["sourceName"] == "Apple Watch"]
    df_sleep_analysis_filt["startDate"] = pd.to_datetime(df_sleep_analysis_filt["startDate"])
    df_sleep_analysis_filt["endDate"] = pd.to_datetime(df_sleep_analysis_filt["endDate"])
    df_sleep_analysis_filt["duration"] = (df_sleep_analysis_filt["endDate"] - df_sleep_analysis_filt["startDate"]).dt.total_seconds() / 60

    df_sleep_analysis_filt['startDate'] = df_sleep_analysis_filt['startDate'].dt.tz_convert('Europe/Rome')
    df_sleep_analysis_filt['endDate'] = df_sleep_analysis_filt['endDate'].dt.tz_convert('Europe/Rome')

    def calculate_reference_date(date):
        return date.date() if date.hour < 13 else (date + pd.Timedelta(days=1)).date()

    df_sleep_analysis_filt['referenceStartDate'] = df_sleep_analysis_filt['startDate'].apply(calculate_reference_date)

    # Expand the 'value' column into multiple columns, assigning duration values to them
    df_expanded = df_sleep_analysis_filt.assign(
        **{val: df_sleep_analysis_filt['duration'].where(df_sleep_analysis_filt['value'] == val, 0)
           for val in df_sleep_analysis_filt['value'].unique()}
    )

    # Drop unnecessary columns
    df_expanded = df_expanded.drop(columns=['value', 'duration', 'sourceName'])

    # Perform a single groupby on referenceStartDate to:
    # - Get the minimum startDate
    # - Get the maximum endDate
    # - Sum the expanded sleep stage columns
    agg_dict = {col: ('sum') for col in df_expanded.columns if col not in ['referenceStartDate', 'startDate', 'endDate']}

    final_df = df_expanded.groupby('referenceStartDate', as_index=False).agg(
        min_startDate=('startDate', 'min'),
        max_endDate=('endDate', 'max'),
        **{col: (col, 'sum') for col in df_expanded.columns if col not in ['referenceStartDate', 'startDate', 'endDate', 'sourceName']}
    )

    final_df = final_df.rename(columns={
        'HKCategoryValueSleepAnalysisAsleepCore': 'asleepCore',
        'HKCategoryValueSleepAnalysisAsleepDeep': 'asleepDeep',
        'HKCategoryValueSleepAnalysisInBed': 'inBed',
        'HKCategoryValueSleepAnalysisAwake': 'awake',
        'HKCategoryValueSleepAnalysisAsleepREM': 'asleepREM',
        'HKCategoryValueSleepAnalysisAsleepUnspecified': 'asleepUnspecified'
    })

    final_df["totalSleep"] = final_df["asleepCore"] + final_df["asleepDeep"] + final_df["asleepREM"] + final_df["asleepUnspecified"]

    # Convert total sleep in format like 4h 30m
    final_df["duration"] = final_df["totalSleep"].apply(lambda x: f"{int(x // 60)}h {int(x % 60)}m")

    # add start and end sleep with following format: 23.39 1.34 9.35
    final_df["startStr"] = final_df["min_startDate"].dt.strftime("%H.%M")
    final_df["endStr"] = final_df["max_endDate"].dt.strftime("%H.%M")

    return final_df


def split_sleep(df_sleep):
    df_sleep["min_startDate"] = pd.to_datetime(df_sleep["min_startDate"])
    df_sleep["max_endDate"] = pd.to_datetime(df_sleep["max_endDate"])

    # List to save the split rows
    rows = []

    for idx, row in df_sleep.iterrows():
        start = row["min_startDate"]
        end = row["max_endDate"]

        # If start and end have timezone info, preserve it
        tzinfo = start.tzinfo

        if start.date() == end.date():
            # if it starts and ends on the same day, no split needed
            rows.append({"date": start.date(), "start": start, "end": end})
        else:
            # Split: from start until 23:59:59
            end_of_day = pd.Timestamp(year=start.year, month=start.month, day=start.day, hour=23, minute=59, second=59, tz=tzinfo)
            rows.append({"date": start.date(), "start": start, "end": end_of_day})

            # Split: from 00:00:00 of next day until end
            start_of_next_day = pd.Timestamp(year=end.year, month=end.month, day=end.day, hour=0, minute=0, second=0, tz=tzinfo)
            rows.append({"date": end.date(), "start": start_of_next_day, "end": end})

    df_sleep_split = pd.DataFrame(rows)
    df_sleep_split["start"] = pd.to_datetime(df_sleep_split["start"])
    df_sleep_split["end"] = pd.to_datetime(df_sleep_split["end"])
    df_sleep_split = df_sleep_split.sort_values(by=["date", "start"])

    return df_sleep_split


def heart_rate_analysis(df_heart_rates: pd.DataFrame, df_sleep_split: pd.DataFrame) -> pd.DataFrame:
    df_heart_rates["sourceName"] = df_heart_rates["sourceName"].str.replace("Apple\xa0Watch di Valerio", "Apple Watch")

    df_heart_rates_filt = df_heart_rates[df_heart_rates["sourceName"] == "Apple Watch"]
    df_heart_rates_filt["startDate"] = pd.to_datetime(df_heart_rates_filt["startDate"])
    df_heart_rates_filt["endDate"] = pd.to_datetime(df_heart_rates_filt["endDate"])
    df_heart_rates_filt["value"] = pd.to_numeric(df_heart_rates_filt["value"], errors="coerce")

    # Extract the date from the startDate column
    df_heart_rates_filt['date'] = df_heart_rates_filt['startDate'].dt.date

    # Extract day, month and year
    df_heart_rates_filt['day'] = df_heart_rates_filt['startDate'].dt.day
    df_heart_rates_filt['month'] = df_heart_rates_filt['startDate'].dt.month
    df_heart_rates_filt['year'] = df_heart_rates_filt['startDate'].dt.year
    df_heart_rates_filt['hour'] = df_heart_rates_filt['startDate'].dt.hour
    df_heart_rates_filt['minute'] = df_heart_rates_filt['startDate'].dt.minute
    df_heart_rates_filt['second'] = df_heart_rates_filt['startDate'].dt.second

    # Extract year month and day from the sleep analysis dataframe
    df_sleep_split['year'] = df_sleep_split['start'].dt.year
    df_sleep_split['month'] = df_sleep_split['start'].dt.month
    df_sleep_split['day'] = df_sleep_split['start'].dt.day

    # Merge the heart rates with the sleep analysis dataframe
    df_heart_rates_filt_sleep = df_heart_rates_filt.merge(
        df_sleep_split[['year', 'month', 'day', 'start', 'end']],
        on=['year', 'month', 'day'],
        how='left'
    )

    # Filter the heart rates to only include those that are within the sleep analysis time range
    df_heart_rates_filt_sleep = df_heart_rates_filt_sleep[
        (df_heart_rates_filt_sleep['startDate'] >= df_heart_rates_filt_sleep['start']) &
        (df_heart_rates_filt_sleep['startDate'] <= df_heart_rates_filt_sleep['end'])
    ]

    # delete the startDate and endDate columns
    df_heart_rates_filt_sleep = df_heart_rates_filt_sleep.drop(columns=['endDate', 'creationDate'])

    # Groupby year month day, keep minimum and maximum value
    df_heart_rates_grouped = df_heart_rates_filt_sleep.groupby(['year', 'month', 'day']).agg(
        minHeartRate=('value', 'min'),
        maxHeartRate=('value', 'max'),
        avgHeartRate=('value', 'mean'),
        date=('startDate', 'first')
    ).reset_index()
    df_heart_rates_grouped['date'] = pd.to_datetime(df_heart_rates_grouped['date'])
    df_heart_rates_grouped['date'] = df_heart_rates_grouped['date'].dt.date

    # left join with the original dataframe to get the time
    df_heart_rates_grouped = df_heart_rates_grouped.merge(
        df_heart_rates_filt_sleep[['year', 'month', 'day', 'hour', 'minute', 'second', 'value']].rename(columns={'value': 'minHeartRate'}),
        on=['year', 'month', 'day', 'minHeartRate'],
        how='left'
    )

    # drop duplicates for year month and day, keep first
    df_heart_rates_grouped = df_heart_rates_grouped.drop_duplicates(subset=['year', 'month', 'day'])

    return df_heart_rates_grouped


def resting_heart_rate_analysis(df_resting_heart_rate: pd.DataFrame) -> pd.DataFrame:
    df_resting_heart_rate["sourceName"] = df_resting_heart_rate["sourceName"].str.replace("Apple\xa0Watch di Valerio", "Apple Watch")

    df_resting_heart_rate_filt = df_resting_heart_rate[df_resting_heart_rate["sourceName"] == "Apple Watch"]
    df_resting_heart_rate_filt["startDate"] = pd.to_datetime(df_resting_heart_rate_filt["startDate"])
    df_resting_heart_rate_filt["endDate"] = pd.to_datetime(df_resting_heart_rate_filt["endDate"])
    df_resting_heart_rate_filt["value"] = pd.to_numeric(df_resting_heart_rate_filt["value"], errors="coerce")

    # Extract the date from the startDate column
    df_resting_heart_rate_filt['date'] = df_resting_heart_rate_filt['startDate'].dt.date

    # Extract day, month and year
    df_resting_heart_rate_filt['day'] = df_resting_heart_rate_filt['startDate'].dt.day
    df_resting_heart_rate_filt['month'] = df_resting_heart_rate_filt['startDate'].dt.month
    df_resting_heart_rate_filt['year'] = df_resting_heart_rate_filt['startDate'].dt.year

    df_resting_heart_rate_filt = df_resting_heart_rate_filt.sort_values(by=['date'])

    return df_resting_heart_rate_filt

if __name__ == "__main__":
    from datetime import datetime
    # Get today's date
    today = pd.Timestamp(datetime.today().date())

    # Create a date range from 1 Jan 2025 to today
    date_range = pd.date_range(start="2025-01-01", end=today)

    # Create the DataFrame with the 'date' column
    df = pd.DataFrame()
    df['date'] = pd.to_datetime(date_range)
    df['date'] = df['date'].dt.date

    # parse xml file
    xml_file = r"C:\Users\Vale\PycharmProjects\Jarvis2.0\data\health\dati esportati_2\apple_health_export\dati esportati.xml"
    df_records, df_workouts, df_activity_summaries, df_heart_rates, df_resting_heart_rate, df_sleep_analysis = parse_apple_health(xml_file)

    # Sleep Analysis
    df_sleep_analysis = sleep_analysis(df_sleep_analysis)
    df_sleep_split = split_sleep(df_sleep_analysis)

    # Heart Rate Analysis
    df_heart_rates_parsed = heart_rate_analysis(df_heart_rates, df_sleep_split)
    df_heart_rates_parsed_filled = df.merge(df_heart_rates_parsed, on='date', how='left')

    # merge heart rates with sleep analysis
    df_sleep_analysis["referenceStartDate"] = pd.to_datetime(df_sleep_analysis["referenceStartDate"])
    df_sleep_analysis["year"] = df_sleep_analysis["referenceStartDate"].dt.year
    df_sleep_analysis["month"] = df_sleep_analysis["referenceStartDate"].dt.month
    df_sleep_analysis["day"] = df_sleep_analysis["referenceStartDate"].dt.day

    df_merged = df_heart_rates_parsed_filled.merge(df_sleep_analysis, on=['year', 'month', 'day'], how='left')

    # analyze resting heart rates
    df_resting_heart_rate = resting_heart_rate_analysis(df_resting_heart_rate)

    # get first date across all dataframes
    first_date = min(
        pd.to_datetime(df_heart_rates['startDate']).dt.tz_localize(None).min(),
        pd.to_datetime(df_sleep_analysis['referenceStartDate']).dt.tz_localize(None).min(),
        pd.to_datetime(df_resting_heart_rate['startDate']).dt.tz_localize(None).min()
    )

    # merge resting heart rates with df_merged
    df_merged = df_merged.merge(
        df_resting_heart_rate[["year", "month", "day", "value"]]
        .rename(columns={"value": "restingHeartRate"}),
        on=['year', 'month', 'day'], how='left'
    )

    (df_merged[['year', 'month', 'day', 'startStr', 'endStr', 'duration',
                'minHeartRate', 'restingHeartRate']]
     .to_csv("merged_sleep_heart_rate.csv", index=False))

    # df_records.to_csv("apple_health_records.csv", index=False)
    # df_workouts.to_csv("apple_health_workouts.csv", index=False)
    # df_activity_summaries.to_csv("apple_health_activity_summaries.csv", index=False)
    # df_heart_rates.to_csv("apple_health_heart_rate.csv", index=False)
    # df_sleep_analysis.to_csv("apple_health_sleep_analysis.csv", index=False)

    print('end')