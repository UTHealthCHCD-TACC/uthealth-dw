import csv
def csv_row_counter(filename):
    row_count = 0
    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        while True:
            try:
                if next(csv_reader):
                    row_count += 1
            except Exception as e:
                break
    return row_count

print(csv_row_counter('Z:/DrWu/HIV/hiv_cohort_tokenized_brand_name_ids.csv'))