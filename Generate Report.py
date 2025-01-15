# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, day, current_date,date_sub
from datetime import date,timedelta,datetime

dbutils.widgets.dropdown("time_period","weekly",["monthly","weekly","today"])
time_period = dbutils.widgets.get("time_period")
today = date.today()


if time_period == "monthly":
    first_day_of_month = today.replace(day=1)
    month_end = first_day_of_month -timedelta(days=1)
    month_start = first_day_of_month -timedelta(days=month_end.day)
elif time_period == "weekly":
    start_date = today - timedelta(days=today.weekday(),weeks=1)
    end_date = start_date + timedelta(days=6)
else:
    start_date=today
    end_date=today

print(start_date,end_date)
print(time_period)


