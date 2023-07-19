import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery


# check whether the input wallet address is a valid Bitcoin address
def check_valid(input_address) -> bool:
    if (34 >= len(input_address) >= 27) and (re.match(r"^[a-zA-Z0-9]+$", input_address)):
        return True
    else:
        return False


def main():
    # create a bigquery client with a specified project
    client = bigquery.Client(project="interviewees-bigquery")

    # get two wall addresses from user inputs
    user_input_first = input("Enter the first wallet address:")
    user_input_second = input("Enter the second wallet address:")

    # handle invalid addresses from user, and ask users to re-enter inputs when given invalid ones
    while (not check_valid(user_input_first)) or (not check_valid(user_input_second)):
        user_input_first = input("Please enter the first Bitcoin address of valid format:")
        user_input_second = input("Please enter the second Bitcoin address of valid format:")

    # the following query retrieve transaction data between two wallet addresses
    query = """
    SELECT input_address, output_address,IP.value AS value, 
        IP.block_number AS block_number, IP.type AS type,
        IP.block_timestamp AS timestamp, IP.required_signatures AS required_signatures
        FROM bigquery-public-data.crypto_bitcoin.inputs AS IP, 
        bigquery-public-data.crypto_bitcoin.outputs AS OP,
        UNNEST (IP.addresses) as input_address, UNNEST(OP.addresses) as output_address
        WHERE IP.spent_transaction_hash = OP.transaction_hash 
            AND IP.spent_output_index = OP.index 
            AND ((input_address = @user_input_first
            AND output_address = @user_input_second) OR (input_address = @user_input_second 
            AND output_address = @user_input_first));
    """

    # add two wall addresses as query parameters before executing the query
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_input_first", "STRING", user_input_first),
            bigquery.ScalarQueryParameter("user_input_second", "STRING", user_input_second),
        ]
    )

    # collect the result returned from query and transform it to a dataframe
    result = client.query(query, job_config=job_config).to_dataframe()

    # show the dataframe scheme and sample data sorted by timestamp
    result.sort_values(by=['timestamp'], ascending=True).head(10)

    # Data Analysis

    # find the number of total transactions
    count_row = result.shape[0]
    print("total number of transactions:", count_row)

    # find total value being transferred
    sum_of_value = result['value'].sum()
    print("total value being transferred:", sum_of_value)

    # find average transaction value
    average_of_value = result['value'].mean()
    print("average value being transferred:", average_of_value)

    # find the median transaction value
    median_of_value = result['value'].median()
    print("median value being transferred:", median_of_value)

    # find the max transaction value
    max_of_value = result['value'].max()
    print("max value being transferred:", max_of_value)

    # find the min transaction value
    min_of_value = result['value'].min()
    print("min value being transferred:", min_of_value)

    # find the start and latest dates of transactions
    first_timestamp = result['timestamp'].min()
    latest_timestamp = result['timestamp'].max()
    print("start date of transaction:", first_timestamp)
    print("latest date of transaction:", latest_timestamp)

    # sort number transactions of different date decreasingly to observe patterns
    date_tran = result[['timestamp']].copy()
    date_tran['timestamp'] = date_tran['timestamp'].dt.date
    date_tran.rename(columns={'timestamp': "date"}, inplace=True)
    date_tran = date_tran.groupby(['date']).agg(
        number_of_transactions=pd.NamedAgg(column="date", aggfunc="count"))
    date_tran = date_tran.sort_values(by=['number_of_transactions'], ascending=False)

    print(date_tran.head(20))

    # Data Visualization

    # create a line plot which describes the relationship between date and number of transactions happened
    fig, ax = plt.subplots(figsize=(15, 7))
    sns.lineplot(ax=ax, x='date', y='number_of_transactions', data=date_tran)
    plt.tight_layout()
    plt.title("Monthly Number of Transactions")
    plt.xlabel("Months")
    plt.ylabel("Number of Transactions" )
    plt.show()

    # create a pie chart which illustrates the ratio of transactions from opposite directions
    tran_first_to_second = result['input_address'].value_counts()['1PPkPubRnK2ry9PPVW7HJiukqbSnWzXkbi']
    tran_second_to_first = result['input_address'].value_counts()['19Kz98riwoFdTPnKe6s2Fg2xAEoa39rGg6']
    tran_compare_lst = [tran_first_to_second, tran_second_to_first]
    labels = ['Transactions from First to Second', 'Transactions from Second to First']
    colors = sns.color_palette('pastel')[0:2]
    plt.pie(tran_compare_lst, labels=labels, colors=colors, autopct='%.0f%%')
    plt.title("Ratio of Transactions between Two Addresses")
    plt.show()


if __name__ == "__main__":
    main()


# Summary of Analysis and Derived Insights

"""
During the analysis phase, we calculate various metrics (average,mean,median etc.) about the transactions and realize
the average is close to the min value. After inspection, we realize most transactions have a fixed value of 1000 
except four transactions with a value of 5757. We also notice the duration of interaction between two addresses ranges  
from 2016-08-30 to 2020-12-06. In addition, we find out most transactions happened between the end of 2018 and the 
beginning of 2019. In the visualization phase, the line plot proves that most transactions happened in a short period 
of time with significant fluctuations. Finally, the pie chart suggests an interesting fact that transactions from both
directions exactly equal, which means two wallets may belong to the same individual or organization.
"""