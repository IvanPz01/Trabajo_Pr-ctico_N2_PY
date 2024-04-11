import csv as c
from connection import select_db as sdb, count_localidades as cldb, insert_db as idb, connect_db as cdb, create_db as ctdb


def super_funcion():
    def read_csv():
        with open('localidades.csv', mode='r', encoding='utf-8') as f:
            reader = c.DictReader(f)
            db = cdb()
            ctdb(db)
            list_insert = []
            for row in reader:
                provincia = row['provincia']
                id = int(row['id'])
                localidades = row['localidad']
                cp = int(row['cp']) if row['cp'] != '' else None
                id_prov_mstr = int(row['id_prov_mstr']) if row['id_prov_mstr'] != '' else None
                list_insert.append((provincia, id, localidades, cp, id_prov_mstr))
            idb(db, list_insert)

    def create_scv():
        db = cdb()
        result = sdb(db)
        count = cldb(db)

        provincias = {}
        provincia_total = {}


        for row in result:
            provincia = row[0]
            if provincia not in provincias:
                provincias[provincia] = []
            provincias[provincia].append(row)

        for row in count:
            provincia = row[0]
            total = row[1]
            provincia_total[provincia] = total

        for provincia, localidades in provincias.items():
            with open(f'{provincia}.csv', mode='w', encoding='utf-8', newline='') as f:
                fieldnames = ['localidad']
                writer = c.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for localidad in localidades:
                    writer.writerow({'localidad': localidad[2]})

            with open(f'{provincia}.csv', mode='a', encoding='utf-8', newline='') as f:
                fieldnames = ['Total']
                writer = c.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for pro_total, total in provincia_total.items():
                    if provincia == pro_total:
                        writer.writerow({'Total': total})
    create_scv()
    read_csv()