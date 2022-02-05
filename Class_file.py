from xml.etree.ElementInclude import FatalIncludeError
import pandas as pd
import sqlite3



class Covid:
    def __init__(self, filename, mydb, db_name, table_name1, table_name2, table_name3, fix_file):
        self.filename = filename
        self.mydb = mydb
        self.db_name = db_name
        self.table_name1 = table_name1
        self.table_name2 = table_name2
        self.table_name3 = table_name3
        self.fix_file = fix_file
        
        #Importera CSV_filen
    def my_csv(self):
        self.filename = pd.read_csv('vaccin_covid.csv')
        
        return self.filename

       
        #Skapa Databas och länka samman den med en cursor för att använda "SQL-språk"

    def create_db(self):
        

        self.mydb = sqlite3.connect(self.db_name)
        self.mycursor = self.mydb.cursor()
        
        #splitta row in i kolumner på ",". set index för dem kolumner som ej ska påverkas,\
        #stack för att inte få 4 separata vaccin-kolumner, unstack för att få bort tomma rader. reset index /
        #  för att få med alla kolumner i dataframet.

    def fix_csv(self):
        self.fix_file = self.filename.set_index(['country','iso_code','total_vaccinations','people_vaccinated','people_fully_vaccinated'\
            ,'source_name', 'source_website','date', 'daily_vaccinations_raw', 'total_vaccinations_per_hundred','people_vaccinated_per_hundred'\
                ,'people_fully_vaccinated_per_hundred','daily_vaccinations_per_million', 'daily_vaccinations']).stack()\
            .str.split(',', expand=True).stack().unstack(-2).reset_index(-1, drop=True).reset_index()
        return self.fix_file

        #städa datan (ta bort saknade värden)

    def clean_data(self):
        self.fix_clean = self.fix_file.dropna()
        return self.fix_clean
        
        #Skapa tables i databasen tillsammans med rows

    def create_table(self):
        self.table_name1 = self.mycursor.execute('CREATE TABLE IF NOT EXISTS countries (country varchar(256), iso_code FLOAT)')
        self.table_name2 = self.mycursor.execute('CREATE TABLE IF NOT EXISTS vaccinated (total_vaccinations FLOAT,\
        people_vaccinated FLOAT, people_fully_vaccinated FLOAT)')
        self.table_name3 = self.mycursor.execute('CREATE TABLE IF NOT EXISTS vaccine (vaccines VARCHAR(256), date FLOAT,\
            country varchar(256))')
     

        #skapa temporära DF där endast dem relevanta kolumnerna (för rätt table i db) finns med.
        # detta för att det går fortare att ladda datan då.
        

    def fill_rows(self):
        #self.country_columns = pd.DataFrame(self.fix_file, columns = [['country','iso_code']])
        self.country_code = self.fix_clean[['country','iso_code']].squeeze()
        self.vaccinated_code = self.fix_clean[['total_vaccinations','people_vaccinated','people_fully_vaccinated']].squeeze()
        self.vaccine_code = self.fix_clean[['vaccines','date','country']].squeeze()
            


    # till sist, släng in värderna i databasen : OBS måste vara en list för att göras, går ej som DataFrame.
    
    def fill_tables(self):
            self.mycursor.executemany('INSERT INTO countries VALUES (?,?)',self.country_code.values.tolist())
            self.mycursor.executemany('INSERT INTO vaccinated VALUES (?,?,?)', self.vaccinated_code.values.tolist())
            self.mycursor.executemany('INSERT INTO vaccine VALUES (?,?,?)', self.vaccine_code.values.tolist())
            self.mydb.commit()


        

