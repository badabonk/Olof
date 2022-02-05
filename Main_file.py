
import pandas as pd
from Class_file import Covid
import os.path
# kalla på klassen

if __name__ == "__main__":
    final_file = Covid('vaccin_covid.csv', 'db_cursor', 'Corona_fil.db', 'countries', 'vaccinated', 'vaccine','fix_file')

#läs in csv till dataframe

    df = final_file.my_csv()

#Skapa ett IF statement som endast skapar databas om den ej redan finns

if not os.path.exists('Corona_fil.db'):
    my_database = final_file.create_db()

#Indexera och splitta kolumner med mer en ett värde i sin row.

    my_split = final_file.fix_csv()
    
#Skapa en databas

    my_table = final_file.create_table()

#ta bort Nan värden för att få en så informationsrik databas som möjligt
    
    clean_table = final_file.clean_data()
    
#importera in specifika kolumner data in i temporära dataframes

    done_rows = final_file.fill_rows()

#Fyll din databas med din dataframe    

    done_tables = final_file.fill_tables()






     







