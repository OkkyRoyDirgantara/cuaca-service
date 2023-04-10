import os
import time

from datetime import datetime, timedelta

import mysql.connector
import pytz
import requests
import schedule as schedule
from bs4 import BeautifulSoup as bs


def get_weather(time):
    url = "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-JawaTimur.xml"
    response = requests.get(url, verify=True)
    r = response.text

    Lamongan = bs(r, "xml")
    weatherLamongan = Lamongan.find(id="501285").find(id="weather")

    datetime = time.strftime("%Y%m%d")
    h0 = weatherLamongan.find(datetime=f"{datetime}1800").value.string
    h6 = weatherLamongan.find(datetime=f"{datetime}0600").value.string
    h12 = weatherLamongan.find(datetime=f"{datetime}1200").value.string
    h18 = weatherLamongan.find(datetime=f"{datetime}1800").value.string
    return [h0, h6, h12, h18]


mydb = mysql.connector.connect(
    host=os.getenv("HOST", "localhost"),
    user=os.getenv("USER", "root"),
    password=os.getenv("PASSWORD", ""),
    database=os.getenv("DATABASE", "telegram_bot")
)


def query_db(sql, val):
    mydb.connect()
    rundb = mydb.cursor()
    mydb.start_transaction()
    rundb.execute(sql, val)
    mydb.commit()
    mydb.close()


def query_all(sql):
    mydb.connect()
    rundb = mydb.cursor()
    rundb.execute(sql)
    return rundb.fetchall()


def save_weather():
    nowTZ = pytz.timezone('Asia/Jakarta')
    now = datetime.now(nowTZ)
    strNow = now.strftime("%Y-%m-%d %H:%M:%S")

    weatherToday = get_weather(now)
    parseNow = now.strftime("%Y%m%d")
    next_run = schedule.next_run()
    print("Jadwal berikutnya dijalankan pada", next_run)

    # Cek apakah 2 jam ke depan berada pada hari selanjutnya
    two_hours_later = now + timedelta(hours=2)
    if two_hours_later.date() > now.date():
        weatherTomorrow = get_weather(two_hours_later)
        sql_check = f"SELECT * FROM weathers WHERE datetime = {two_hours_later.strftime('%Y%m%d')}"
        checkifsame = query_all(sql_check)
        if checkifsame[0][1] != two_hours_later.strftime("%Y%m%d"):
            sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (two_hours_later.strftime("%Y%m%d"), weatherTomorrow[0], weatherTomorrow[1], weatherTomorrow[2],
                   weatherTomorrow[3], two_hours_later)
            try:
                query_db(sql, val)
                print('inserted')
            except Exception as e:
                print(f"error : {e}")

    sql_check = f"SELECT * FROM weathers WHERE datetime = {parseNow}"
    checkifsame = query_all(sql_check)
    if checkifsame != []:
        for x in checkifsame:
            date = x[1]
            if date == parseNow:
                sql = "UPDATE weathers SET h0 = %s, h6 = %s, h12 = %s, h18 = %s, updated_at = %s WHERE datetime = %s"
                val = (weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow, parseNow)
                query_db(sql, val)
                print('updated')
            else:
                sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
                val = (parseNow, weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow)
                query_db(sql, val)
                print('inserted')
    else:
        sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (parseNow, weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow)
        query_db(sql, val)
        print('inserted')


def check_bot_status() -> bool:
    sql = "SELECT * FROM bot_status where id = 2"
    status = query_all(sql)
    if status[0][1] == 1:
        return True
    else:
        return False


def bot_stop(now):
    status = 0
    sql = "UPDATE bot_status SET is_run = %s, stop_at = %s WHERE id = 2"
    val = (status, now)
    query_db(sql, val)
    raise SystemExit


def bot_start(now):
    status = 1
    sql = "UPDATE bot_status SET is_run = %s, run_at = %s WHERE id = 2"
    val = (status, now)
    query_db(sql, val)


if __name__ == '__main__':
    # schedule.every(2).hours.do(save_weather)
    schedule.clear()
    schedule.every(5).seconds.do(save_weather)
    try:
        now = pytz.timezone('Asia/Jakarta')
        now = datetime.now(now)
        bot_start(now)
        while check_bot_status():
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        now = pytz.timezone('Asia/Jakarta')
        now = datetime.now(now)
        bot_stop(now)
        print("Terminated")
        exit(0)
    except Exception as e:
        now = pytz.timezone('Asia/Jakarta')
        now = datetime.now(now)
        bot_stop(now)
        print(f"Error: {e} now: {now}")
        exit(0)
    finally:
        now = pytz.timezone('Asia/Jakarta')
        now = datetime.now(now)
        print(f"Stop Finally: {now}")
        bot_stop(now)
        print("Terminated")
        exit(0)
