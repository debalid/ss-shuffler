import json, codecs
from shuffler import Shuffler
from postgres import PostgresInjection
from notifier import Notifier

__author__ = 'debalid'


def main():
    config_json = json.load(codecs.open("config.json", "r", encoding="UTF-8"))
    db_config = config_json["database"]
    smtp_config = config_json["smtp"]
    santa_config = config_json["santa"]

    postgres = PostgresInjection(db_config)

    with Shuffler(postgres, santa_config) as shuffler:
        shuffler.work()

    notifier = Notifier(postgres, smtp_config, santa_config)
    notifier.notify_unaware()

main()
