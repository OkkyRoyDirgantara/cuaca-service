import os
import time

from datetime import datetime, timedelta

import mysql.connector
import pytz
import requests
import schedule as schedule
from bs4 import BeautifulSoup as bs
from dotenv import load_dotenv
load_dotenv()


def get_weather(time):
    url = "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-JawaTimur.xml"
    try:
        response = requests.get(url)
    except Exception as e:
        raise e
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
    try:
        mydb.start_transaction()
        rundb.execute(sql, val)
        mydb.commit()
    except Exception as e:
        mydb.rollback()
        raise e
    finally:
        rundb.close()
        mydb.close()



def query_all(sql):
    mydb.connect()
    rundb = mydb.cursor()
    try:
        rundb.execute(sql)
    except Exception as e:
        raise e
    result = rundb.fetchall()
    rundb.close()
    mydb.close()
    return result


def save_weather():
    nowTZ = pytz.timezone('Asia/Jakarta')
    now = datetime.now(nowTZ)
    strNow = now.strftime("%Y-%m-%d %H:%M:%S")


    parseNow = now.strftime("%Y%m%d")
    next_run = schedule.next_run()
    print("Jadwal berikutnya dijalankan pada", next_run)

    two_hours_next_day = now + timedelta(hours=2)

    sql_check_today = f"SELECT * FROM weathers WHERE datetime = {parseNow}"
    checkifsametoday = query_all(sql_check_today)

    sql_check_tomorrow = f"SELECT * FROM weathers WHERE datetime = {two_hours_next_day.strftime('%Y%m%d')}"
    checkifsametomorrow = query_all(sql_check_tomorrow)
    if two_hours_next_day.strftime("%Y%m%d") != parseNow:
        if checkifsametomorrow == []:
            weatherTomorrow = get_weather(two_hours_next_day)
            sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (two_hours_next_day.strftime("%Y%m%d"), weatherTomorrow[0], weatherTomorrow[1], weatherTomorrow[2],
                   weatherTomorrow[3], strNow)
            try:
                query_db(sql, val)
                print('inserted when tomorrow')
            except Exception as e:
                print(f"error : {e}")
        elif checkifsametomorrow[0][1] == two_hours_next_day.strftime("%Y%m%d"):
            weatherTomorrow = get_weather(two_hours_next_day)
            sql = "UPDATE weathers SET h0 = %s, h6 = %s, h12 = %s, h18 = %s, updated_at = %s WHERE datetime = %s"
            val = (weatherTomorrow[0], weatherTomorrow[1], weatherTomorrow[2], weatherTomorrow[3], strNow, two_hours_next_day.strftime("%Y%m%d"))
            try:
                query_db(sql, val)
                print('updated when tomorrow')
            except Exception as e:
                print(f"error : {e}")
    elif checkifsametoday != []:
        if checkifsametoday[0][1] == parseNow:
            weatherToday = get_weather(now)
            sql = "UPDATE weathers SET h0 = %s, h6 = %s, h12 = %s, h18 = %s, updated_at = %s WHERE datetime = %s"
            val = (weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow, parseNow)
            try:
                query_db(sql, val)
                print('updated when not null and same date')
            except Exception as e:
                print(f"error : {e}")
        elif checkifsametoday[0][1] != parseNow:
            weatherToday = get_weather(now)
            sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (parseNow, weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow)
            try:
                query_db(sql, val)
                print('inserted when not null')
            except Exception as e:
                print(f"error : {e}")
    elif checkifsametoday == []:
        weatherToday = get_weather(now)
        sql = "INSERT INTO weathers (datetime, h0, h6, h12, h18, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (parseNow, weatherToday[0], weatherToday[1], weatherToday[2], weatherToday[3], strNow)
        try:
            query_db(sql, val)
            print('inserted when null')
        except Exception as e:
            print(f"error : {e}")


def check_bot_status() -> bool:
    sql = "SELECT * FROM bot_status where id = 2"
    try:
        status = query_all(sql)
        if status[0][1] == 1:
            return True
        else:
            return False
    except Exception as e:
        print(f"error : {e}")


def bot_stop(now):
    status = 0
    sql = "UPDATE bot_status SET is_run = %s, stop_at = %s WHERE id = 2"
    val = (status, now)
    try:
        query_db(sql, val)
        raise SystemExit
    except Exception as e:
        print(f"error : {e}")


def bot_start(now):
    status = 1
    sql = "UPDATE bot_status SET is_run = %s, run_at = %s WHERE id = 2"
    val = (status, now)
    try:
        query_db(sql, val)
    except Exception as e:
        print(f"error : {e}")


if __name__ == '__main__':
    # schedule.every(2).hours.do(save_weather)
    schedule.every(5).minutes.do(save_weather)
    try:
        now = pytz.timezone('Asia/Jakarta')
        now = datetime.now(now)
        bot_start(now)
        while check_bot_status():
            schedule.run_pending()
            time.sleep(15)
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
