import csv
import datetime

# current_date_string is a string in the format YYYY-MM-DD
# returns x days before the current_date_string as a string in the format YYYY-MM-DD
def x_days_before(current_date_string, x=0):
    format = "%Y-%m-%d"
    current_date = datetime.datetime.strptime(current_date_string, format)
    diff = datetime.timedelta(days=x)
    result = current_date - diff
    return result.strftime(format)

def find_row_with_date(fields, rows, state_date_to_row_index_map, date_string, state):
    result = None
    row_index = state_date_to_row_index_map.get((state, date_string), -1)

    if row_index == -1:
        return None

    result = rows[row_index]
    return result

def append_x_day_data(fields, rows, state_date_to_row_index_map, current_row_index, x=0):
    current_row = rows[current_row_index]
    date_index = fields.index('date')
    current_date_string = current_row[date_index]
    state_index = fields.index('state')
    current_state = current_row[state_index]

    x_days_before_date = x_days_before(current_date_string, x=x)
    row_for_x_days_before = find_row_with_date(fields, rows, state_date_to_row_index_map, x_days_before_date, current_state)

    cases_index = fields.index('cases')
    deaths_index = fields.index('deaths')
    cases_in_current_day = int(current_row[cases_index])
    deaths_in_current_day = int(current_row[deaths_index])

    if row_for_x_days_before is None:
        # find daily and weekly for rows that do not have a prev or week before date
        rows[current_row_index].extend([str(cases_in_current_day), str(deaths_in_current_day)])
        return


    cases_in_x_days_before = int(row_for_x_days_before[cases_index])
    case_difference = cases_in_current_day - cases_in_x_days_before
    rows[current_row_index].append(str(case_difference))

    deaths_in_x_days_before = int(row_for_x_days_before[deaths_index])
    death_difference = deaths_in_current_day - deaths_in_x_days_before
    rows[current_row_index].append(str(death_difference))

def append_daily_data(fields, rows, state_date_to_row_index_map, current_row_index):
    append_x_day_data(fields, rows, state_date_to_row_index_map, current_row_index, x=1)

def append_weekly_data(fields, rows, state_date_to_row_index_map, current_row_index):
    append_x_day_data(fields, rows, state_date_to_row_index_map, current_row_index, x=7)

def append_new_data(fields, rows, state_date_to_row_index_map, current_row_index):
    append_daily_data(fields, rows, state_date_to_row_index_map, current_row_index)
    append_weekly_data(fields, rows, state_date_to_row_index_map, current_row_index)

def preprocess_csv(filename, new_filename, new_fields):
    fields = []
    rows = []

    with open(filename) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",")

        fields = next(csvreader)

        for row in csvreader:
            rows.append(row)

    state_date_to_row_index_map = {}
    state_index = fields.index('state')
    date_index = fields.index('date')

    for index, row in enumerate(rows):
        state = row[state_index]
        date = row[date_index]
        state_date_to_row_index_map[(state, date)] = index

    for index, row in enumerate(rows):
        append_new_data(fields, rows, state_date_to_row_index_map, index)

    with open(new_filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(new_fields)
        csvwriter.writerows(rows)

def preprocess_us_states():
    filename = "data/us-states.csv"
    new_filename = "data/us-states-extended.csv"
    new_fields = ["date", "state", "fips", "total_cases", "total_deaths", "cases_in_past_day", "deaths_in_past_day", "cases_in_past_seven_days", "deaths_in_past_seven_days"]
    preprocess_csv(filename, new_filename, new_fields)

def preprocess():
    preprocess_us_states()

if __name__ == "__main__":
    preprocess()
