import ssl
from smtplib import SMTP_SSL, SMTPException

__author__ = 'debalid'


class Notifier(object):
    def __init__(self, postgres_injection, smtp_config, santa_config):
        self.__postgres = postgres_injection
        if Notifier.__is_smtp_config_valid(smtp_config) and Notifier.__is_santa_config_valid(santa_config):
            self.__smtp_config = smtp_config
            self.__santa_config = santa_config
        else:
            raise ValueError("SMTP or Santa config are not proper.")

    def send(self, emails):
        ssl_context = ssl.create_default_context()
        with SMTP_SSL(host=self.__smtp_config["host"], port=self.__smtp_config["port"], context=ssl_context) as smtp:
            smtp.login(user=self.__smtp_config["user"], password=self.__smtp_config["password"])
            for x in emails:
                smtp.send_message(x["message"], from_addr=self.__smtp_config["from_address"], to_addrs=x["to_address"])

    def notify_unaware(self):
        with self.__postgres.connection() as connection, connection.cursor() as curs:
            curs.execute("SELECT id, email FROM human WHERE is_santa = TRUE AND email_sent = FALSE")

            if curs.rowcount > 0:
                unawared = curs.fetchall()

                from email.mime.text import MIMEText

                def make_message_to_unawared_one(id):
                    curs.execute("SELECT name, address, post_index FROM human WHERE santa_id = %s", (id,))
                    if curs.rowcount == 0:
                        raise Exception("Wrong state of santa with id: " + id)
                    (name, address, post_index) = curs.fetchone()
                    return MIMEText(self.__santa_config["message"] + "<br/>" + name + "<br/>" + address + "<br/>" +
                                    post_index, "html")

                emails = list(map(
                    lambda x: {
                        "to_address": x[1],
                        "message": make_message_to_unawared_one(x[0])
                    },
                    unawared
                ))

                try:
                    self.send(emails)
                    for e in unawared:
                        curs.execute("UPDATE human SET email_sent = TRUE WHERE id = %s", (e[0],))
                    print("Sent.")
                except SMTPException:
                    connection.rollback()
                    print("Exception during sending.")

            else:
                print("All santas already warned.")

    @staticmethod
    def __is_smtp_config_valid(smtp_config):
        return smtp_config["host"] and smtp_config["port"] and smtp_config["user"] and smtp_config["password"] \
               and smtp_config["from_address"]

    @staticmethod
    def __is_santa_config_valid(santa_config):
        return santa_config["message"]
