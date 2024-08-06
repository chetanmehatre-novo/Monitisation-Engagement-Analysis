#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 14:20:21 2024

@author: chetan
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import config2

from config2 import SQLQuery

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from snowflake.sqlalchemy import URL


q = SQLQuery('snowflake')




df = q("""WITH RECURSIVE date_series AS (
              SELECT DATEADD(month, -96, Date_trunc(month,to_date(GETDATE()))) AS month
              UNION ALL
              SELECT DATEADD('month', 1, month)
              FROM date_series
              WHERE DATEADD('month', 1, month) <= Date_trunc(month,to_date(GETDATE()))
            ),
            
            date_series_link AS (
            SELECT *
            ,1 as tag 
            from date_series
            ),
            
            businesses_distinct AS (
            SELECT business_id,
            date_trunc(month,to_date(account_close_date)) as Close_month,
            1 as tag 
            FROM data.businesses
            ),
            
            biz_month_series_all AS (
            Select business_id,
            month,
            FROM businesses_distinct bd 
            LEFT JOIN date_series_link ds ON bd.tag = ds.tag
            ),
            
            PD as (
            SELECT *, sum(amount) over (partition by business_id order by transaction_date asc rows between unbounded preceding and current row) as cum_deposits
            FROM prod_db.data.transactions
            WHERE 1=1
                and type='credit'
                and status <>'rejected'
            )
            , first_deposit_above as (
            SELECT *, row_number() over (partition by business_id order by transaction_date asc) as row_num
            FROM PD
            WHERE cum_deposits >=1000
            ),
            
            cumulative_deposit_flag_1k as (SELECT abc.business_id
                    , transaction_date as deposit_amt_reached_date
                    , running_balance
                    , account_funded_at
            FROM first_deposit_above abc
            LEFT JOIN prod_db.data.businesses biz on abc.business_id=biz.business_id
            WHERE row_num = 1
            order by transaction_date desc),
            
            
            
            Temp as (SELECT txn.business_id,
            Date_trunc(month, to_date(txn.transaction_date)) as Txn_month, 
            Case when Type = 'debit' then 1 else 0 end as Debit_Flag,
            Case when Type = 'credit' then 1 else 0 end as Credit_Flag,
            Case when txn_month = Date_trunc(month,to_date(Deposit_AMT_Reached_Date)) then 1 else 0 end as Activation_1k_Month,
            Sum(abs(amount)) as Aggregate_Amount
            FROM data.transactions txn
            left join cumulative_deposit_flag_1k temp on txn.business_id=temp.business_id
            where Txn_month >= Date_trunc(month,to_date(Deposit_AMT_Reached_Date)) and status <> 'rejected'
            group by 1,2,3,4,5
            order by 1,2,3,4,5)
            
            ,Temp2 as (
            SELECT business_id,
            Date_trunc(month, to_date(transaction_date)) as Txn_month, 
            count(transaction_id) as txn_count
            from data.transactions
            group by 1,2),
            
            engagement_monthly_flag_subquery as (Select 
            a.business_id,
            a.txn_month,
            sum (a.Debit_Flag) AS Debit_Flag,
            Sum(a.Credit_Flag) AS Credit_Flag,
            Case when sum(a.Activation_1k_Month)>0 then 1 else 0 end as Activation_1k_Month,
            sum(a.Aggregate_Amount) as amount,
            sum(b.txn_count) as Txn_count
            from Temp a
            left join temp2 b on a.business_id=b.business_id and a.txn_month=b.txn_month
            group by 1,2
            ORDER BY 1,2),
            
            avg_daily_balance as (Select date_trunc(month, to_date(date)) as month_bal
                , business_id as bid
                , avg(day_end_balance) as avg_balance
            FROM prod_db.data.balances_daily
            Group by 1,2
            Order by 1 desc),
            
            Base1 AS (
            Select a.*,
            b.debit_flag,
            b.credit_flag,
            b.Activation_1k_Month,
            b.amount,
            b.txn_count
            FROM biz_month_series_all a
            left join engagement_monthly_flag_subquery b on a.business_id=b.business_id and a.month=b.txn_month
            ),
            
            -- Input New Engagement Flag Criteria below  inside the ( )
            --      Debit_Flag = 1 means at least one debit txn in the month
            --      Credit Flag = 1 means at least one credit txn in the month
            --      Amount > x means GTV in the month must be greater than x
            --      Txn Count > x means # of txns in the month must be greater than x
            Engagement_Details as (SELECT month,
            business_id,
            -- CASE WHEN Activation_1k_Month = 1 or (debit_flag=1 and amount > 100) then 1 else 0 end as Engagement_Flag,
            --CASE WHEN Txn_count > 0 then 1 else 0 end as Engagement_Flag,
            --CASE WHEN Txn_count >= 10 then 1 else 0 end as Engagement_Flag,
            --CASE WHEN Txn_count >= 5 and a.avg_balance>= 5000 then 1 else 0 end as Engagement_Flag,
            --CASE WHEN Txn_count >= 5 and Amount >= 500 then 1 else 0 end as Engagement_Flag,
            --CASE WHEN Txn_count >= 5 then 1 else 0 end as Engagement_Flag,
            CASE WHEN Txn_count >= 5 and Amount >= 10000 then 1 else 0 end as Engagement_Flag,
            from base1 b
            left join avg_daily_balance a on b.business_id = a.bid and b.month = a.month_bal),
            
            First_Engagement_Month AS (
              SELECT business_id,
                     MIN(month) AS first_engagement_month
              FROM Engagement_Details
              WHERE engagement_flag = 1
              GROUP BY business_id),
             
            Engagement_Lags AS (
              SELECT a.month,
                     a.business_id,
                     a.Engagement_Flag,
                     LAG(a.Engagement_Flag) OVER (PARTITION BY a.business_id ORDER BY a.month) AS lag1,
                     LAG(a.Engagement_Flag, 2) OVER (PARTITION BY a.business_id ORDER BY a.month) AS lag2,
                     LAG(a.Engagement_Flag, 3) OVER (PARTITION BY a.business_id ORDER BY a.month) AS lag3,
                     Case when b.first_engagement_month = a.month then 1 else 0 end as first_engagement_month,
                     Case when b.first_engagement_month = a.month then b.first_engagement_month else NULL end as first_engagement_date,
                     Case when b.first_engagement_month <= a.month then 1 else 0 end as Engagement_All_time
              FROM Engagement_Details a
              left join first_engagement_month b on a.business_id=b.business_id
            
            )
            
            
            SELECT Engagement_Lags.business_id, month, date(b.account_create_date) as account_create_date, date(b.account_funded_at) as account_funded_at,
                  SUM(CASE WHEN Engagement_Flag = 1 THEN 1 ELSE NULL END) AS Engaged_This_Month,
                  SUM(CASE WHEN Engagement_Flag = 1 AND lag1 = 1 THEN 1 ELSE NULL END) AS Engaged_This_Month_and_last_month,
                  SUM(CASE WHEN Engagement_Flag = 1 AND lag1 = 0 AND lag2 = 1 THEN 1 ELSE NULL END) AS Engaged_This_Month_and_2_months_ago,
                  SUM(CASE WHEN Engagement_Flag = 1 AND lag1 = 0 AND lag2 = 0 and lag3 = 1 THEN 1 ELSE NULL END) AS Engaged_This_Month_and_3_months_ago,
                  SUM(CASE WHEN Engagement_Flag = 1 AND lag1 = 0 AND lag2 = 0 and lag3 = 0 and Engagement_All_time = 1 and first_engagement_month = 0 THEN 1 ELSE NULL END) AS Churned_Engaged_This_Month,
                  SUM(CASE WHEN Engagement_Flag = 0 AND lag1 = 1 THEN 1 ELSE NULL END) AS Engaged_Last_Month_Not_This_Month,
                  SUM(CASE WHEN Engagement_Flag = 0 AND lag1 = 0 AND lag2 = 1 THEN 1 ELSE NULL END) AS Engaged_Two_Months_Ago_Not_Last_Or_This_Month,
                  SUM(CASE WHEN Engagement_Flag = 0 AND lag1 = 0 AND lag2 = 0 and lag3 = 1 THEN 1 ELSE NULL END) AS Engaged_three_Months_Ago_Not_Last_2_or_This_Month,
                  SUM(first_engagement_month) as first_Time_MAB,
                  max(first_engagement_date) as first_engagement_date
            FROM Engagement_Lags
            left join data.businesses b on Engagement_Lags.business_id = b.business_id
            where 
            --Engagement_Lags.business_id in ('0e542967-39bf-472c-ab72-a3a070f00bee', 'e5f38861-dabe-4f59-a286-82c9a913e1dc', '111475c8-ad22-42d7-8821-e1875e8b4e13', 'dc073983-457a-4662-95e8-bccfcb299e06', '55d8d579-cfa7-4f38-8786-765d7a297561')
            --date(account_create_date) >= date('2020-10-01')
            --and 
            date(month) >= date(b.account_create_date)
            GROUP BY 1,2,3,4 order by 1,2,3,4
            
            """)


df['account_create_date'] = pd.to_datetime(df['account_create_date'])

df['month'] = pd.to_datetime(df['month'])


####
monthly_engagement = df[['business_id', 'month', 'account_create_date', 'account_funded_at',
       'engaged_this_month']].copy()

monthly_engagement.fillna(0, inplace=True)

#


df = monthly_engagement.copy()

# Convert 'month' to datetime for proper sorting
df['month'] = pd.to_datetime(df['month'])

# Sort by 'business_id' and 'month'
df = df.sort_values(by=['business_id', 'month'])

# Initialize the new columns
df['dormant_3months_not_engaged'] = 0
df['first_time_dormant'] = 0
df['first_time_resurrected'] = 0

# Function to identify dormancy
def identify_dormancy(group):
    # Calculate rolling sum of 'engaged_flag' over three months
    group['engaged_3_months'] = group['engaged_this_month'].rolling(window=3, min_periods=3).sum()

    # Identify the first occurrence of three consecutive months with no engagement
    group['dormant_3months_not_engaged'] = (group['engaged_3_months'] == 0).astype(int)

    # Mark the first occurrence of dormancy for each business_id
    if group['dormant_3months_not_engaged'].sum() > 0:
        first_dormant_idx = group[group['dormant_3months_not_engaged'] == 1].index[0]
        group.at[first_dormant_idx, 'first_time_dormant'] = 1

    return group

# Function to identify resurrection
def identify_resurrection(group):
    # Identify the first time resurrected after dormancy
    if group['first_time_dormant'].sum() > 0:
        dormant_idx = group[group['first_time_dormant'] == 1].index[0]
        post_dormant = group.loc[dormant_idx+1:]
        first_resurrected_idx = post_dormant[post_dormant['engaged_this_month'] == 1].index[0] if (post_dormant['engaged_this_month'] == 1).any() else None
        if first_resurrected_idx is not None:
            group.at[first_resurrected_idx, 'first_time_resurrected'] = 1

    return group

# Apply the dormancy function to each group of 'business_id'
df = df.groupby('business_id').apply(identify_dormancy)

# Apply the resurrection function to each group of 'business_id'
df = df.groupby('business_id').apply(identify_resurrection)

# Drop the temporary 'engaged_3_months' column
df.drop(columns=['engaged_3_months'], inplace=True)

# Display the updated DataFrame
print(df)

# # Function to identify dormancy and resurrection
# def check_dormant_and_resurrected(group):
#     group = group.reset_index(drop=True)
#     first_dormant_found = False
#     dormant_index = None

#     # Check for dormancy over three consecutive months
#     for i in range(len(group) - 2):
#         if group.iloc[i:i+3]['engaged_this_month'].sum() == 0:
#             group.loc[group.index[i+2], 'dormant_3months_not_engaged'] = 1
#             if not first_dormant_found:
#                 group.loc[group.index[i+2], 'first_time_dormant'] = 1
#                 first_dormant_found = True
#                 dormant_index = group.index[i+2]

#     # Check for resurrection after the first dormancy
#     if first_dormant_found and dormant_index is not None:
#         for j in range(dormant_index + 1, len(group)):
#             if group.loc[group.index[j], 'engaged_this_month'] == 1:
#                 group.loc[group.index[j], 'first_time_resurrected'] = 1
#                 break

#     return group

# # Apply the function to each group of 'business_id'
# df = df.groupby('business_id').apply(check_dormant_and_resurrected)

# # Reset index if necessary
# df.reset_index(drop=True, inplace=True)

# # Display the dataframe
# print(df)

# # Group by 'business_id'
# grouped = df.groupby('business_id')

# # Identify dormancy and resurrection using vectorized operations
# def identify_dormancy_and_resurrection(group):
#     # Calculate rolling sum of 'engaged_flag' over three months
#     group['engaged_3_months'] = group['engaged_this_month'].rolling(window=3, min_periods=3).sum()

#     # Identify the first occurrence of three consecutive months with no engagement
#     group['dormant_3months_not_engaged'] = (group['engaged_3_months'] == 0).astype(int)

#     # Mark the first occurrence of dormancy for each business_id
#     if group['dormant_3months_not_engaged'].sum() > 0:
#         first_dormant_idx = group[group['dormant_3months_not_engaged'] == 1].index[0]
#         group.at[first_dormant_idx, 'first_time_dormant'] = 1

#     # Identify the first time resurrected after dormancy
#     if group['first_time_dormant'].sum() > 0:
#         dormant_idx = group[group['first_time_dormant'] == 1].index[0]
#         post_dormant = group.loc[dormant_idx+1:]
#         first_resurrected_idx = post_dormant[post_dormant['engaged_this_month'] == 1].index[0] if (post_dormant['engaged_this_month'] == 1).any() else None
#         if first_resurrected_idx is not None:
#             group.at[first_resurrected_idx, 'first_time_resurrected'] = 1

#     return group

# # Apply the vectorized function to each group
# df = grouped.apply(identify_dormancy_and_resurrection)

# # Drop the temporary 'engaged_3_months' column
# df.drop(columns=['engaged_3_months'], inplace=True)

# # Display the updated DataFrame
# print(df)


# # Calculate 'months_after_dormant'
# def calculate_months_after_dormant(row, first_dormant_month):
#     if pd.isna(first_dormant_month):
#         return None
#     # Calculate difference in months
#     delta_years = row['month'].year - first_dormant_month.year
#     delta_months = row['month'].month - first_dormant_month.month
#     total_months = delta_years * 12 + delta_months
#     return total_months

# # Apply calculation for each row
# df['months_after_dormant'] = df.apply(
#     lambda row: calculate_months_after_dormant(row, df[(df['business_id'] == row['business_id']) & (df['first_time_dormant'] == 1)]['month'].min()),
#     axis=1
# )


# Sort by 'business_id' and 'month'
df = df.sort_values(by=['business_id', 'month'])

# Calculate the first dormant month for each business_id
df['first_time_dormant_month'] = df[df['first_time_dormant'] == 1].groupby('business_id')['month'].transform('min')


# Forward fill the 'first_time_dormant_month' for each business_id
df['first_time_dormant_month'] = df.groupby('business_id')['first_time_dormant_month'].transform('ffill')


# Calculate 'months_after_dormant'
df['months_after_dormant'] = (df['month'].dt.year - df['first_time_dormant_month'].dt.year) * 12 + \
                             (df['month'].dt.month - df['first_time_dormant_month'].dt.month)


# # Create the 'first_time_dormant_month' column and propagate the values
# df['first_time_dormant_month'] = df.groupby('business_id')['month'].transform(
#     lambda x: x[df['first_time_dormant'] == 1].min()
# )


# Filter to keep only rows with non-negative 'months_after_dormant' values
df2 = df[df['months_after_dormant'] >= 0]

# Drop rows where 'months_after_dormant' is NaN
df2 = df2.dropna(subset=['months_after_dormant'])


# Create pivot table
pivot_table = df2.pivot_table(
    index='first_time_dormant_month',
    columns='months_after_dormant',
    values='first_time_resurrected',
    aggfunc='sum',
    fill_value=0
)

# Display the pivot table
print(pivot_table)

# Get the number of first-time dormant customers per month
first_time_dormant_per_month_sample = df[df['first_time_dormant'] == 1].groupby('month').size()



# Create pivot table
pivot_table2 = df2.pivot_table(
    index='first_time_dormant_month',
    columns='months_after_dormant',
    values='first_time_resurrected',
    aggfunc='sum',
    fill_value=0
)

# Display the pivot table
print(pivot_table2)

# Get the number of first-time dormant customers per month
first_time_dormant_per_month = df[df['first_time_dormant'] == 1].groupby('month').size()


# # Mark periods of resurrection
# monthly_engagement['prev_is_dormant'] = monthly_engagement.groupby('business_id')['dormant_3months_not_engaged'].shift(1).fillna(False)
# monthly_engagement['is_resurrected'] = (monthly_engagement['engaged_this_month'] == 1) & (monthly_engagement['prev_is_dormant'] == True)

# # Create the pivot table with 'became_dormant' month as index and 'months_since_dormant' as columns
# pivot_table = monthly_engagement[monthly_engagement['is_resurrected']].pivot_table(
#     index='first_dormant_month',
#     columns='months_since_dormant',
#     values='business_id',
#     aggfunc='count',
#     fill_value=0
# )

# # Display the pivot table
# print("Pivot Table of Customers Who Became Dormant:")
# print(pivot_table)







# standard finmab

# Create pivot table
pivot_table3 = df2.pivot_table(
    index='first_time_dormant_month',
    columns='months_after_dormant',
    values='first_time_resurrected',
    aggfunc='sum',
    fill_value=0
)

# Display the pivot table
print(pivot_table3)



# Get the number of first-time dormant customers per month
first_time_dormant_per_month3 = df[df['first_time_dormant'] == 1].groupby('month').size()


# # Mark periods of resurrection
# df2['prev_is_dormant'] = df2.groupby('business_id')['dormant_3months_not_engaged'].shift(1).fillna(False)
# df2['is_resurrected'] = (df2['engaged_this_month'] == 1) & (df2['prev_is_dormant'] == True)

# # Create the pivot table with 'became_dormant' month as index and 'months_since_dormant' as columns
# pivot_table3_1 = df2[df2['is_resurrected']].pivot_table(
#     index='first_time_dormant_month',
#     columns='months_after_dormant',
#     values='business_id',
#     aggfunc='count',
#     fill_value=0
# )

# # Display the pivot table
# print("Pivot Table of Customers Who Became Dormant:")
# print(pivot_table3_1)



# 5+ finmab

# Create pivot table
pivot_table4 = df2.pivot_table(
    index='first_time_dormant_month',
    columns='months_after_dormant',
    values='first_time_resurrected',
    aggfunc='sum',
    fill_value=0
)

# Display the pivot table
print(pivot_table4)



# Get the number of first-time dormant customers per month
first_time_dormant_per_month4 = df[df['first_time_dormant'] == 1].groupby('month').size()



# 5+ finmab and gtv >= 10000

# Create pivot table
pivot_table5 = df2.pivot_table(
    index='first_time_dormant_month',
    columns='months_after_dormant',
    values='first_time_resurrected',
    aggfunc='sum',
    fill_value=0
)

# Display the pivot table
print(pivot_table5)



# Get the number of first-time dormant customers per month
first_time_dormant_per_month5 = df[df['first_time_dormant'] == 1].groupby('month').size()






































# # Identify dormant periods (3 consecutive months of is_engaged == 0)
# monthly_engagement['dormant'] = (monthly_engagement['engaged_this_month'] == 0).astype(int)

# # Ensure the DataFrame is sorted by customer_id and month in ascending order
# monthly_engagement = monthly_engagement.sort_values(by=['business_id', 'month'])

# # Create month column
# # monthly_engagement['month2'] = monthly_engagement.index.get_level_values('month').to_period('M')


# # Calculate rolling dormant periods within each customer
# monthly_engagement['dormant_period'] = (
#     monthly_engagement.groupby('business_id')['dormant']
#     .rolling(window=3, min_periods=3)
#     .sum()
#     .reset_index(level=0, drop=True)
# )

# # Calculate rolling dormant periods within each month of each customer
# # monthly_engagement['dormant_period'] = (monthly_engagement.groupby(['business_id', 'month2'])['dormant'].rolling(window=3, min_periods=3, axis=0).sum().reset_index(level=[0, 1], drop=True))
# # monthly_engagement['dormant_period'] = monthly_engagement.groupby('business_id')['dormant'].rolling(window=3, min_periods=3).sum().reset_index(level=0, drop=True)
# monthly_engagement['is_dormant'] = monthly_engagement['dormant_period'] == 3

# # Mark periods of resurrection
# monthly_engagement['prev_is_dormant'] = monthly_engagement.groupby('business_id')['is_dormant'].shift(1).fillna(False)
# monthly_engagement['is_resurrected'] = (monthly_engagement['engaged_this_month'] == 1) & (monthly_engagement['prev_is_dormant'] == True)

# # Reset index to bring back transaction_date
# monthly_engagement = monthly_engagement.reset_index()

# # Create a pivot table with the count of resurrected customers
# pivot_table_resurrected = monthly_engagement[monthly_engagement['is_resurrected']].pivot_table(
#     index=monthly_engagement['month'],
#     columns='mob',
#     values='business_id',
#     aggfunc='count'
# ).fillna(0).astype(int)

# # Display the pivot table for resurrected customers
# print("Pivot Table with Resurrected Customers Count by MOB:")
# print(pivot_table_resurrected)


# #
# # monthly_engagement['dormant_flag'] = np.where(monthly_engagement['dormant_period'] == 3, 1, 0)

# # Mark the first month of dormant period
# monthly_engagement['was_dormant'] = monthly_engagement.groupby('business_id')['is_dormant'].shift(1).fillna(False)
# monthly_engagement['became_dormant'] = np.where(monthly_engagement['is_dormant'] & ~monthly_engagement['was_dormant'], 1, 0)

# monthly_engagement['month'] = pd.to_datetime(monthly_engagement['month']).dt.date

# d = monthly_engagement.groupby('month')['became_dormant'].sum()

# # Mark the first month of dormant period
# monthly_engagement['was_dormant'] = monthly_engagement.groupby('business_id')['is_dormant'].shift(1).fillna(False)
# monthly_engagement['became_dormant'] = monthly_engagement['is_dormant'] & ~monthly_engagement['was_dormant']


# # Calculate the first dormant month per customer
# first_dormant_month = monthly_engagement[monthly_engagement['became_dormant']].groupby('business_id')['month'].min().reset_index()
# first_dormant_month = first_dormant_month.rename(columns={'month': 'first_dormant_month'})
# monthly_engagement = monthly_engagement.merge(first_dormant_month, on='business_id', how='left')

# # Convert Period to datetime for calculations
# monthly_engagement['first_dormant_month'] = pd.to_datetime(monthly_engagement['first_dormant_month'])
# monthly_engagement['month_date'] = pd.to_datetime(monthly_engagement['month'])

# # Calculate Months on Book (MOB) after the dormant month
# # monthly_engagement['months_since_dormant'] = (
# #     (monthly_engagement['month_date'] - monthly_engagement['first_dormant_month']) / np.timedelta64(1, 'M').astype(int)
# # )


# monthly_engagement['months_since_dormant'] = (monthly_engagement['month_date'].dt.year - monthly_engagement['first_dormant_month'].dt.year) * 12 + (monthly_engagement['month_date'].dt.month - monthly_engagement['first_dormant_month'].dt.month)


# # # Calculate Months on Book (MOB) after the dormant month
# # def calculate_mob(row):
# #     if pd.isna(row['first_dormant_month']) or pd.isna(row['month_date']):
# #         return np.nan
# #     # Compute difference in months
# #     diff = (row['month_date'].year - row['first_dormant_month'].year) * 12 + (row['month_date'].month - row['first_dormant_month'].month)
# #     return diff

# # monthly_engagement['months_since_dormant'] = monthly_engagement.apply(calculate_mob, axis=1)


# # Create the DataFrame for the pivot table
# pivot_data = monthly_engagement[monthly_engagement['became_dormant']]


# # Create the pivot table with 'became_dormant' month as index and 'months_since_dormant' as columns
# pivot_table = monthly_engagement[monthly_engagement['is_resurrected']].pivot_table(
#     index='first_dormant_month',
#     columns='months_since_dormant',
#     values='business_id',
#     aggfunc='count',
#     fill_value=0
# )

# # Display the pivot table
# print("Pivot Table of Customers Who Became Dormant:")
# print(pivot_table)

# #
# monthly_engagement['became_dormant2'] = np.where(monthly_engagement['is_dormant'] & ~monthly_engagement['was_dormant'], 1, 0)

# d2 = monthly_engagement.groupby('first_dormant_month')['became_dormant2'].sum()


# #temp
# # Create the pivot table with 'became_dormant' month as index and 'months_since_dormant' as columns
# pivot_table5 = monthly_engagement.pivot_table(
#     index='first_dormant_month',
#     columns='months_since_dormant',
#     values='business_id',
#     aggfunc='count',
#     fill_value=0
# )

# # Display the pivot table
# print("Pivot Table of Customers Who Became Dormant:")
# print(pivot_table5)
# # temp end


# #
# monthly_engagement['became_dormant2'] = np.where(monthly_engagement['is_dormant'] & ~monthly_engagement['was_dormant'], 1, 0)

# d2 = monthly_engagement.groupby('first_dormant_month')['became_dormant2'].sum()




# ####

# # Calculate the first dormant month per customer
# first_resurrected_month = monthly_engagement[monthly_engagement['is_resurrected']].groupby('business_id')['month'].min().reset_index()
# first_resurrected_month = first_resurrected_month.rename(columns={'month': 'first_resurrected_month'})
# monthly_engagement = monthly_engagement.merge(first_resurrected_month, on='business_id', how='left')


# #
# monthly_engagement['is_resurrected_first_time'] = np.where(monthly_engagement['month']==monthly_engagement['first_resurrected_month'], 1, 0)

# # Create the pivot table with 'became_dormant' month as index and 'months_since_dormant' as columns
# pivot_table3 = monthly_engagement[monthly_engagement['is_resurrected_first_time']].pivot_table(
#     index='first_dormant_month',
#     columns='months_since_dormant',
#     values='business_id',
#     aggfunc='count',
#     fill_value=0
# )

# # Display the pivot table
# print("Pivot Table of Customers Who Became Dormant:")
# print(pivot_table3)


# # Get the number of first-time dormant customers per month
# first_time_dormant_per_month = df[df['first_time_dormant'] == 1].groupby('month').size()

