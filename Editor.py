#! /usr/bin/env python3
import gspread
import datetime
import time as tm
import requests
import untangle
import psycopg2
from psycopg2 import Error


def create_google_table():
    """
    Создание таблицы в базе
    """
    try:
        connection = psycopg2.connect(user="google_bot",
                                      password="start",
                                      host="127.0.0.1",
                                      port="5432",
                                      dbname='google_base'
                                      )
        cursor = connection.cursor()
        create_table_query = '''CREATE TABLE IF NOT EXISTS google_table
                              (Id SERIAL PRIMARY KEY,
                              Номер_Заказа INT NOT NULL,
                              Стоимость_в_$ decimal NOT NULL,
                              Стоимость_в_P decimal NOT NULL,
                              Срок_поставки TEXT,
                              Сообщения BOOL
                              ); '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица `google_table` успешно создана в PostgreSQL")

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    else:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


class Database:

    def __init__(self):
        self.connection = psycopg2.connect(user="google_bot",
                                           password="start",
                                           host="127.0.0.1",
                                           port="5432",
                                           dbname='google_base'
                                           )
        self.cursor = self.connection.cursor()

    def add_entry(self, supply, price_d, price_r, purchase_time):
        with self.connection:
            insert_query = 'INSERT INTO google_table (Номер_Заказа, ' \
                           'Стоимость_в_$, Стоимость_в_P, Срок_поставки, Сообщения) VALUES (%s,%s,%s,%s,%s)'
            item_tuple = (supply, price_d, price_r, purchase_time, False)
            return self.cursor.execute(insert_query, item_tuple)

    def update_entry(self, supply, price_d, price_r, purchase_time):
        with self.connection:
            return self.cursor.execute(
                f"UPDATE google_table SET Стоимость_в_$='{price_d}', Стоимость_в_P='{price_r}',"
                f" Срок_поставки='{purchase_time}' "
                f"WHERE Номер_Заказа = '{supply}'")

    def update_send_mess(self, supply):
        with self.connection:
            return self.cursor.execute(
                f"UPDATE google_table SET Сообщения='True' WHERE Номер_Заказа = '{supply}'")

    def select_entry(self):
        with self.connection:
            self.cursor.execute(f"SELECT * FROM google_table")
            result = self.cursor.fetchall()
            return result

    def select_num_order(self, num_elem):
        with self.connection:
            self.cursor.execute(f"SELECT Номер_Заказа, Стоимость_в_$, Срок_поставки "
                                f"FROM google_table WHERE Номер_Заказа='{num_elem}'")
            result = self.cursor.fetchall()
            return result[0]

    def send_mess_num_order(self, num_elem):
        with self.connection:
            self.cursor.execute(f"SELECT Сообщения FROM google_table WHERE Номер_Заказа='{num_elem}'")
            result = self.cursor.fetchall()
            return result[0][0]

    def delete_elem(self, id_elem):
        with self.connection:
            self.cursor.execute(f"DELETE FROM google_table WHERE id='{id_elem}'")


class EditorTable(Database):
    """
    Синхронизация таблицы в базе с google таблицей
    """
    _worksheet = None
    _API_TOKEN = ''
    _ADMIN = [1026987498, ]

    def __init__(self, id_sheet):
        Database.__init__(self)
        self._worksheet = self.init_worksheet(id_sheet)

    def response_course(self) -> float:
        """
        Запрос на стоимость доллара США, источник ЦБ России
        :return: float
        """
        try:
            date = datetime.datetime.now().strftime("%d/%m/%Y ")
            result = requests.get(f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={date}")
            obj = untangle.parse(result.text)
            for enum in range(len(obj.ValCurs)):
                if "Доллар США" in obj.ValCurs.Valute[enum].children[-2].cdata:
                    course = str(obj.ValCurs.Valute[enum].children[-1].cdata).split(',')
                    return float('.'.join(course))
        except Exception as error:
            return 0

    def init_worksheet(self, id_sheet: str) -> object:
        """
        Обьект google таблицы
        :param id_sheet: str
        :return: object
        """
        gs = gspread.service_account(filename='cred.json')
        sh = gs.open_by_key(id_sheet)
        worksheet = sh.sheet1
        return worksheet

    def loop(self):
        """
        Проверка, на наличие в базе из таблицы, запись, обновление, удаление из базы,
        если срок доставки истёк отправка сообщения админам, отправляется один раз

        """
        while True:
            date_now = datetime.date.today()
            res_dict_table = self._worksheet.get_all_records()
            res_list_base = self.select_entry()
            price_doll = self.response_course()
            price_rub = lambda x, y: round(float(y) * x, 2)
            if not res_list_base:
                for elem in res_dict_table:
                    self.add_entry(elem.get('заказ №', 0),
                                   elem.get('стоимость,$', 0),
                                   price_rub(elem.get('стоимость,$', 1), price_doll),
                                   elem.get('срок поставки', 0))
            else:
                send_telegram = []
                elem_table = []
                for elem in res_dict_table:
                    number_order = elem.get('заказ №', 0)
                    price_table_dol = elem.get('стоимость,$', 0)
                    elem_table.append(number_order)
                    if price_table_dol == '':
                        price_table_dol = 1
                    if number_order == '':
                        number_order = 0
                    last_rub = price_rub(price_table_dol, price_doll)
                    if number_order not in [number[1] for number in res_list_base]:
                        self.add_entry(number_order,
                                       price_table_dol,
                                       last_rub,
                                       elem.get('срок поставки', 0))
                    else:
                        tuple_table = number_order, \
                                      price_table_dol, \
                                      elem.get('срок поставки', 0)

                        tuple_base = self.select_num_order(number_order)
                        if tuple_table != tuple_base:
                            self.update_entry(number_order,
                                              price_table_dol,
                                              last_rub,
                                              elem.get('срок поставки', 0))

                    date = []
                    for j in elem.get('срок поставки', 0).split('.'):
                        if j == "":
                            continue
                        else:
                            date.append(int(j))
                    if date:
                        d = datetime.date(date[2], date[1], date[0])
                        if date_now > d:
                            if not self.send_mess_num_order(number_order):
                                send_telegram.append(number_order)
                                self.update_send_mess(number_order)
                for elem_base in res_list_base:
                    if elem_base[1] not in elem_table:
                        self.delete_elem(elem_base[0])
                self.send_message(send_telegram)
            tm.sleep(3)

    def send_message(self, order: list):
        """
        Оповещения в телеграм спящим админам
        :param order: list
        """
        for endorder in order:
            message = f"Время доставки заказа № {endorder} закончилось"
            url = f"https://api.telegram.org/bot{self._API_TOKEN}/sendMessage"
            for admin in self._ADMIN:
                message_last = f"`🅱🅾🆃\n\n{message}`"
                message_data = {'chat_id': f"{admin}", "text": f"{message_last}", "parse_mode": "MARKDOWN"}
                requests.post(url, data=message_data)


if __name__ == '__main__':
    id_table = "1aOT84CEkK5e4jUB57pJg2PPVEMUy87jL49Ne_u1c5Kk"
    create_google_table()
    ed = EditorTable(id_table)
    ed.loop()
