# Databricks notebook source
import sqlite3
import os
os.chdir("https://github.com/emmy-1/subscriber_cancellations/blob/main/Database/dev/cademycode_updated.db")

con = sqlite3.connect("cademycode.db")
cur = con.cursor()
