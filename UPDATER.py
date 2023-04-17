import datetime

import pandas as pd
from lib.helper import configs, setup_logger

logger = setup_logger(' UPDATER:PY ', 'lib/log.xml', level="INFO")


def filtered_result_data():
    try:
        df = pd.read_csv(configs.output_file)
        df['Runtime DateTime'] = pd.to_datetime(df['Runtime DateTime'])
        mask = df.isna() | (df == '')
        df = df.drop(df[mask.any(axis=1)].index)

        df['Price'] = df['Price'].replace({'£': ''}, regex=True).astype(float)
        # TODO Feature : diğer parabirimleri için kayıt tutarak eski haline getir
        unique_products = df['EAN'].unique()
        for product_ean in unique_products:
            try:

                product_data = df[df["EAN"] == product_ean]

                # En eski kayıt
                oldest_record = product_data.loc[product_data["Runtime DateTime"].idxmin()]
                # En güncel kayıt
                latest_record = product_data.loc[product_data["Runtime DateTime"].idxmax()]
                # En yüksek fiyatlı en eski kayıt
                highest_price_oldest_record = \
                    product_data.sort_values(by=["Price", "Runtime DateTime"], ascending=[False, True]).iloc[0]
                # En yüksek fiyatlı en güncel kaıt
                highest_price_latest_record = \
                    product_data.sort_values(by=["Price", "Runtime DateTime"], ascending=[False, False]).iloc[0]
                # En düşük fiyatlı en eski kayıt
                lowest_price_oldest_record = \
                    product_data.sort_values(by=["Price", "Runtime DateTime"], ascending=[True, True]).iloc[0]
                # En düşük fiyatlı en güncel kayıt
                lowest_price_latest_record = \
                    product_data.sort_values(by=["Price", "Runtime DateTime"], ascending=[True, False]).iloc[0]

                drop_indexes = product_data.index[~product_data["Runtime DateTime"].isin(
                    [oldest_record["Runtime DateTime"], latest_record["Runtime DateTime"],
                     highest_price_oldest_record["Runtime DateTime"], highest_price_latest_record["Runtime DateTime"],
                     lowest_price_oldest_record["Runtime DateTime"], lowest_price_latest_record["Runtime DateTime"], ])]
                drop_indexes_df = df.index[df["Runtime DateTime"].isin(
                    [oldest_record["Runtime DateTime"], latest_record["Runtime DateTime"],
                     highest_price_oldest_record["Runtime DateTime"], highest_price_latest_record["Runtime DateTime"],
                     lowest_price_oldest_record["Runtime DateTime"], lowest_price_latest_record["Runtime DateTime"], ])]

                df = df.drop(drop_indexes)
            except Exception as err:
                logger.error(f" EAN :{product_ean} ERR : {err}")

        df['Price'] = df['Price'].apply(lambda x: '£' + str(x))
        df.to_csv(configs.output_file, header=True, mode='w', index=False)
        return df
    except FileNotFoundError as ferr:
        logger.error(f" Dosya Okuma Hatası :{configs.output_file} ERR: {ferr}")
    except ValueError as verr:
        logger.error(f" Değer hatası  ERR: {verr}")
    except KeyError as kerr:
        logger.error(f" Sütun adı hatalı  ERR: {kerr}")
    except Exception as err:
        logger.error(f" ERR: {err}")


if __name__ == "__main__":
    df = filtered_result_data()
