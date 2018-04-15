import requests.sessions
import datetime
import sys
import zipfile
import io
import os

base_url = "https://www.nseindia.com/content/historical/EQUITIES"
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
    'cookie': 'SID=3QVFqjhiXuk2KcR9YRkEJR95600pfw7K7VGO90rYC_2CugAxrObnXKUtIganua4Qe7THvA.; HSID=Axd4AMYgJhNwhQFY7; SSID=AnSMqrQh5_qtUefGs; APISID=SlbkxerZSnMCgSDw/AP3P9EGfePfEmalLD; SAPISID=uO2rFYFTDmdLdMpR/ACg77kXkO5Ee-nv33; OSID=3QVFqrPeNcrRyU7Ie2G-3CVpmYP7Up1ORjBt9gLDUIdMUhq_lwlPm-a8qqyNnrFdViWX0A.; OTZ=4349786_34_34__34_; NID=128=gA5fCTialhT23KHsyh-coVp2DuBIW30EdZcCrg6XV9y21ndximrULmmjICmpxiJ1oE7azh_EuuGEsnNwyPJDrVMS3wHGQCKOihRVr2TlBcwGKy_-eUpfl3wpzdDtxjHN7y90vBXfM1a_sGMMgvs93esN5zlKnjTDPs9rZahscZCDQ4XW580ehGhttRm0PTaQtnaHiqJjwY9_; SIDCC=AEfoLeZP92l0qqbXsoYywJx2ihc5o4VvkW63Ieze30ajKV8DgwTYG9tIuA51dqQ7Ctvcr1qdKg4',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'Connection': 'keep-alive'}


def generate_param(date):
    return "cm{day}{month}{year}bhav.csv.zip".format(day=date.strftime('%d'), month=date.strftime('%b').upper(), year=date.year)


def generate_url(date):
    request_url = "{url}/{year}/{month}/{param}".format(url=base_url, year=date.year, month=date.strftime('%b').upper(), param=generate_param(date))
    return request_url


def get_response_for_request(sess, url, date):
    response = sess.get(url)
    if response.status_code == 403:
        print("Response status is %s\tCould not download for date %s." % (response.status, date))
    elif response.status_code == 404:
        print("File not found\tCould not download %s.", url)
    elif response.status_code != 200:
        print("Response status is %s \tCould not download for date %s." % (response.status, date))
    else:
        print("Downloaded File for date %s", date)
    return response


def zip_to_csv(response, date):
    zfile = zipfile.ZipFile(io.BytesIO(response.content))
    csv = zfile.read(zfile.namelist()[0])
    write_to_csv(csv, date)


def download_csv_data(from_date, to_date):
    current_date = to_date
    with requests.Session() as sess:
        sess.headers = header
        while current_date >= from_date:
            url = generate_url(current_date)
            response = get_response_for_request(sess, url, current_date)
            if response.status_code == 200:
                zip_to_csv(response, current_date)
            current_date += datetime.timedelta(days=-1)


def download_csv_latest_data(current_date):
    with requests.Session() as sess:
        sess.headers = header
        while True:
            url = generate_url(current_date)
            response = get_response_for_request(sess, url, current_date)
            if response.status_code == 200:
                zip_to_csv(response, current_date)
                break
            current_date += datetime.timedelta(days=-1)


def write_to_csv(data, date):
    if not os.path.exists('data'):
        os.makedirs('data')
    file_name = "nse" + date.strftime('%d%m%Y') + ".csv"
    with open(os.path.join("data", file_name), 'w') as file:
        file.write(data.decode())
        file.close()


def print_usage():
    print("python main.py -from mmddyy -to mmddyy")  # Get data between FROM and TO
    print("python main.py -update")  # Get Latest Data
    print("python main.py")  # Get Today's Data


def main(args):
    if args:
        if args[0] == "-from":
            from_date = datetime.datetime.strptime(args[1], '%d%m%Y')
            to_date = datetime.datetime.strptime(args[3], '%d%m%Y')
        elif args[0] == "-update":
            from_date = datetime.date.today()
            to_date = None
        else:
            from_date = None
            print_usage()
    else:
        from_date = datetime.date.today()
        to_date = datetime.date.today()
    if from_date and to_date:
        download_csv_data(from_date, to_date)


if __name__ == "__main__":
    main(sys.argv[1:])
