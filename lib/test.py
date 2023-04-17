import pandas as pd
import random
from datetime import datetime, timedelta
from SCRAPER import run_scraper
from UPDATER import filtered_result_data


def mix_price_date():
    global index
    df = pd.read_csv('RESULT.csv')
    df.columns = ["Product ID", "Runtime DateTime", "Product Title", "Price",
                  "EAN", "Stock Availability"]

    def random_date(start, end):
        timestamp = random.uniform(start.timestamp(), end.timestamp())
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')

    def random_price():
        return '£' + str(round(random.uniform(1, 100), 2))

    for index, row in df.iterrows():
        new_timestamp = random_date(datetime.now() - timedelta(days=10), datetime.now())
        new_price = random_price()
        df.at[index, 'Runtime DateTime'] = str(new_timestamp)
        if int(index) % 3 == 0:
            df.at[index, 'Price'] = new_price
    df.to_csv('RESULT_RASTGELE.csv', index=False, header=False)
    print("Fiyat ve Tarih alanları rastgele değerlerle değiştirildi..")
    return df

def print_filtered_resul_data(df):
    filtered_df = df.groupby('EAN').agg({'Product Title': lambda x: x.str.slice(0, 20).iloc[0],
                                         'Runtime DateTime': list,
                                         'Price': list}).reset_index()

    renamed_columns = {'EAN': 'EAN',
                       'Product Title': 'Product Title'}
    filtered_df = filtered_df.rename(columns=renamed_columns)

    max_num_dates = filtered_df['Runtime DateTime'].apply(len).max()
    for i in range(max_num_dates):
        filtered_df[f'Date {i + 1}'] = ""
        filtered_df[f'Price {i + 1}'] = ""

    for i in range(len(filtered_df)):
        num_dates = len(filtered_df['Runtime DateTime'][i])
        for j in range(num_dates):
            date_str = str(filtered_df['Runtime DateTime'][i][j])[:10]
            filtered_df.loc[i, f'Date {j + 1}'] = date_str
            filtered_df.loc[i, f'Price {j + 1}'] = filtered_df['Price'][i][j]

    filtered_df = filtered_df.drop(['Runtime DateTime', 'Price'], axis=1)

    print(f"Distinct EANs: {len(filtered_df)}\n")

    print(filtered_df.to_string(index=False))

if __name__ == "__main__":
    run_scraper()
    # mix_price_date()
    #df = filtered_result_data()
    #print_filtered_resul_data(df)
