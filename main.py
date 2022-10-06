import pandas as pd
import numpy as np


def get_order_details():

    df = pd.read_csv("interview_df.csv")
    # convert to datetime column (needed if the datetime field is not datetime)
    df['created_date'] = pd.to_datetime(df['created_date'])
    # creating copies of original dataframe to process individual problem
    # so that it wont corrupt or change index of the source dataframe
    df2 = df.copy()
    df3 = df.copy()

    # orders_on_customer_id_7D

    df1 = df.groupby('customer_id').rolling('7D', on='created_date').count().reset_index()
    # rename column name to desired one
    df1.rename(columns={'order_amount': 'orders_on_customer_id_7D'}, inplace=True)
    final_df = pd.merge(df, df1[['created_date', 'customer_id', 'orders_on_customer_id_7D']], how='inner',
                        left_on=['created_date', 'customer_id'],
                        right_on=['created_date', 'customer_id'])

    # customer_id_has_paid

    df2['paid_status'] = np.where(df2['order_status'] == 'paid', True,
                                  np.where(df2['order_status'] == 'unpaid', False, False))

    df2['customer_id_has_paid'] = df2.groupby('customer_id')['paid_status'].apply(lambda x: x.shift(fill_value=0)
                                                                                  .expanding().sum())

    df2['customer_id_has_paid'] = df2['customer_id_has_paid'].astype('bool')
    final_df = pd.merge(final_df, df2[['created_date', 'customer_id', 'customer_id_has_paid']], how='inner',
                        left_on=['created_date', 'customer_id'],
                        right_on=['created_date', 'customer_id'])

    # shop_id_count_paid_orders_90D

    df3['paid_status'] = np.where(df3['order_status'] == 'paid', 1,
                                  np.where(df3['order_status'] == 'unpaid', 0, 0))
    # creating index with datecolumn
    # sorting the dataframe with date column
    df3.index = pd.DatetimeIndex(df["created_date"])
    df3 = df3.sort_index()

    df3 = df3.groupby('shop_id').rolling('90D', on='created_date')['paid_status'].sum().reset_index()
    # get rid of the index name (same index name and column name will cause trouble in merge)
    df3.index.name = None
    # rename column name to desired one
    df3.rename(columns={'paid_status': 'shop_id_count_paid_orders_90D'}, inplace=True)

    final_df = pd.merge(final_df, df3[['created_date', 'shop_id', 'shop_id_count_paid_orders_90D']], how='inner',
                        left_on=['created_date', 'shop_id'],
                        right_on=['created_date', 'shop_id'])

    final_df.to_csv('output.csv', header=True, index=False)

    return True


if __name__ == '__main__':
    get_order_details()


