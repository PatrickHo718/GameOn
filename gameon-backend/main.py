from fastapi import FastAPI
import pandas as pd
import boto3
import io
import os
from dotenv import load_dotenv
from utils import load_s3_csv

app = FastAPI()

@app.get("/expenses")
def get_expenses():
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    return df.to_dict(orient="records")

@app.get("/summary")
def get_summary():
    # same S3 loading code as before
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    summary = df.groupby("Category")["Amount"].sum().reset_index()
    return summary.to_dict(orient="records")

@app.get("/expenses/category/{category}")
def get_by_category(category: str):
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    filtered = df[df["Category"].str.lower() == category.lower()]
    return filtered.to_dict(orient="records")

@app.get("/expenses/date/{year}/{month}")
def get_by_month(year: int, month: int):
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    filtered = df[(df["Date"].dt.year == year) & (df["Date"].dt.month == month)]
    return filtered.to_dict(orient="records")

@app.get("/summary/monthly")
def monthly_summary():
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    summary = df.groupby("Month")["Amount"].sum().reset_index()
    return summary.to_dict(orient="records")

@app.post("/split")
def split_expenses(names: list[str]):
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    total = df["Amount"].sum()
    per_person = round(total / len(names), 2)
    return {name: per_person for name in names}


@app.get("/summary/categories")
def category_summary():
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    summary = df.groupby("Category")["Amount"].sum().reset_index()
    return summary.to_dict(orient="records")

@app.get("/expenses/top/{n}")
def top_expenses(n: int = 5):
    df = load_s3_csv("gameon-data-dev", "Expense.csv")
    top_n = df.sort_values("Amount", ascending=False).head(n)
    return top_n.to_dict(orient="records")
