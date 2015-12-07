__author__ = 'debalid'


class Shuffler(object):
    def __init__(self, postgres_injection, santa_config):
        self.__postgres = postgres_injection
        if Shuffler.__is_santa_config_valid(santa_config):
            self.__santa_config = santa_config
        else:
            raise ValueError("Santa configs is not proper.")

    def __enter__(self):
        self.__connection = self.__postgres.connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.__connection.rollback()
            print("Rollback.")
        else:
            self.__connection.commit()
        self.__connection.close()

    def __get_human_ids_without_santa(self):
        with self.__connection.cursor() as curs:
            curs.execute("SELECT id FROM human WHERE santa_id IS NULL ORDER BY date_created LIMIT %s",
                         (self.__santa_config["shuffle_limit"],))
            if curs.rowcount > 0:
                return curs.rowcount, curs.fetchall()
            else:
                return 0, []

    def __get_human_ids_without_client(self):
        with self.__connection.cursor() as curs:
            curs.execute("SELECT id FROM human WHERE is_santa = FALSE ORDER BY date_created LIMIT %s",
                         (self.__santa_config["shuffle_limit"],))
            if curs.rowcount > 0:
                return curs.rowcount, curs.fetchall()
            else:
                return 0, []

    def __load_ids_map(self, ids_map):
        with self.__connection.cursor() as curs:
            '''map(
                lambda x: curs.execute("""
                UPDATE human SET is_santa = true WHERE id = %s;
                UPDATE human SET santa_id = %s WHERE id = %s;
                """, x),
                ids_map
            )'''
            # ids_map: (santa_id -> client_id)
            curs.executemany("""
                UPDATE human SET is_santa = true WHERE id = %s;
                UPDATE human SET santa_id = %s WHERE id = %s;
                """, list(map(lambda x: (x[0], x[0], x[1]), ids_map)))

    def work(self):
        (santas_num, santas_ids) = self.__get_human_ids_without_client()
        (clients_num, clients_ids) = self.__get_human_ids_without_client()

        if santas_num > 1 and clients_num > 1:
            '''
            Change order of clients to make it random.
            Folks from StackOverflow say that this is the most efficient way to make a derangement.
            '''
            from random import shuffle
            def make_derangement(some):
                randomized = some[:]
                while True:
                    shuffle(randomized)
                    for a, b in zip(some, randomized):
                        if a == b:
                            break
                    else:
                        return randomized

            clients_ids = make_derangement(clients_ids)

            # Setting clients to santas
            ids_map = list(map(
                lambda x: (santas_ids[x], clients_ids[x]),
                range(0, min(santas_num, clients_num))
            ))

            self.__load_ids_map(ids_map)

            print("Assigned!")
        else:
            print("There is no enough santas or clients to make assignment!")

    @staticmethod
    def __is_santa_config_valid(santa_config):
        return santa_config["shuffle_limit"]
