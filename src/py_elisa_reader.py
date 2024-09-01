#%%
import sys, pyodbc
import datetime
import fitz

"""TODO:
Füge Testlaufbefunde (IBV,AI) hinzu, Abgrenzung small table
Wichtig: Prüfe für neg_sus_pos, ob immer die ersten 4 Vertiefungen die Tests sind
Mach das parsing allgemeiner..
"""

TABLE_NAME = 'LaborbefundT'
COLUMNS_BIG = 'BelegKomplett, PositionLab, ErgebnissDatum, Labornummer, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na, Titer, cv'
COLUMNS_SMALL = 'BelegKomplett, PositionLab, ErgebnissDatum, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na'

db_path = r"C:\Synch\MMT.mdb"

#pdf_path = r"C:\Users\konst\Documents\Py Workspace\py_elisa\problem_pdfs\Pilsenhöhe-AI.pdf"
#"C:\Users\konst\Documents\Py Workspace\py_elisa\pdfs\Alain ET LW 18 Copy do not use\Altenmarhorst 18.LW.pdf"
pdf_path= " ".join(sys.argv[1::])
print(pdf_path)

connection_str = (
    rf"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path}"
    )
connection = pyodbc.connect(connection_str)

cursor = connection.cursor()

doc = fitz.open(pdf_path)

def insert_sql(cursor, table, columns, params):
    
    #Insertion call
    print(f"Inserting: {params}")
    values_str = len(columns.split(","))*' ?,'
    values_str = values_str.strip().strip(",")
    cursor.execute(f"""INSERT INTO 
            {TABLE_NAME}({columns})
            values ({values_str})""", params)

    cursor.commit()
    print (f"New Entry for {params[0]}-LabPos {params[1]} created!")


def fetch_values_big_table(content_list:list):
    # Positions of values in loaded pdf data
    block_position_dict = {"beleg_komplett":7, "ergebnis_datum":19,
    "labor_nummer":5,"kennzeichnung":12,"krankheit":20,"probenanzahl":23, #Problem: steht vor DTiter
    "neg_sus_pos":22,"titer":27, "cv":30, # check this one!!
    }
    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum" #hardcoded change for others
    methode = "ELISA" #hardcoded change for others

    def default_clean(raw_string:str):
        return raw_string.split("\n")[1].replace(" ","")

    # Fetching and cleaning values from pdf data

    for sub_list in content_list:
        if sub_list[CONST_INDEX]==CONST_BLOCK:
            if sub_list[POS_INDEX]==block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4]
                beleg_komplett = default_clean(beleg_komplett_raw)

            elif sub_list[POS_INDEX]==block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.replace(" ","").replace("\n","").replace(".","/")

            elif sub_list[POS_INDEX]==block_position_dict["labor_nummer"]:
                labor_nummer_raw = sub_list[4]
                labor_nummer = default_clean(labor_nummer_raw)
            
            elif sub_list[POS_INDEX]==block_position_dict["kennzeichnung"]:
                kennzeichnung_raw = sub_list[4]
                kennzeichnung = default_clean(kennzeichnung_raw)

            elif sub_list[POS_INDEX]==block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = krankheit_raw.replace("\n","").replace(" ","")
                krankheit = "-".join([krankheit.upper(),"BioChek"])

            elif sub_list[POS_INDEX]==block_position_dict["probenanzahl"]:
                probenanzahl_raw = sub_list[4]
                probenanzahl = int(probenanzahl_raw.split("\n")[0]) #watch this, it is weird that it is in front of Titer

            elif sub_list[POS_INDEX]==block_position_dict["neg_sus_pos"]:
                neg_sus_pos_raw = sub_list[4]
                neg_sus_pos = neg_sus_pos_raw.split("\n")[0].split("/")
                neg = int(neg_sus_pos[0])
                sus = int(neg_sus_pos[1])
                pos = int(neg_sus_pos[2])

            elif sub_list[POS_INDEX]==block_position_dict["titer"]:
                titer_raw = sub_list[4]
                titer = int(titer_raw.replace("\n","").replace(" ",""))

            elif sub_list[POS_INDEX]==block_position_dict["cv"]:
                cv_raw = sub_list[4]
                cv = int(cv_raw.replace("\n","").replace(" ",""))
    return (beleg_komplett, ergebnis_datum, labor_nummer, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus, titer, cv)

def fetch_values_small_table(content_list:list):

    block_position_dict = {"beleg_komplett":30, "ergebnis_datum":14,
    "kennzeichnung":30,"krankheit": 26,"probenanzahl":16, #Problem: steht vor DTiter
    }   
    #Count pos, neg, sus
    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum" #hardcoded change for others
    methode = "ELISA" #hardcoded change for others
    pos = 0
    neg = 0
    sus = 0
    for sub_list in content_list:
        if sub_list[CONST_INDEX]==CONST_BLOCK:
            if sub_list[POS_INDEX]==block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4].replace(",",";").split(";")
                for snip in beleg_komplett_raw:
                    if "Lab" in snip:
                        beleg_snip = snip
                print(beleg_snip.strip().split("\n")[0].split(" ")[1].strip().replace("\n",""))
                
                beleg_komplett = beleg_snip.strip().split("\n")[0].split(" ")[1].strip().replace("\n","")
               
            if sub_list[POS_INDEX]==block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.replace(" ","").replace("\n","").replace(".","/")
                
            if sub_list[POS_INDEX]==block_position_dict["kennzeichnung"]:
                kennzeichnung_raw = sub_list[4].replace(",",";").split(";")
                stall_found = False
                for snip in kennzeichnung_raw:
                    if "Stall" in snip:
                        kennzeichnung_snip = snip
                        stall_found = True
                if stall_found:       
                    kennzeichnung = kennzeichnung_snip.strip().replace("\n","")
                else:
                    
                    kennzeichnung = sub_list[4].strip().replace("\n","")


                kennzeichnung = kennzeichnung_raw[2].replace(" ","").replace("\n","")
            if sub_list[POS_INDEX]==block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = krankheit_raw.strip().split("\n")[-1].replace(" ", "").replace("\n","")
                krankheit = "-".join([krankheit.upper(),"IDEXX"])
            if sub_list[POS_INDEX]==block_position_dict["probenanzahl"]:
                probenanzahl_raw = sub_list[4]
                probenanzahl = int(probenanzahl_raw.replace("\n","").replace(" ", ""))
                
            
        if sub_list[POS_INDEX]>35:
            if "Pos" in sub_list[4]:
                pos +=1
            if "Neg" in sub_list[4]:
                neg +=1
            if "Sus" in sub_list[4]:
                sus +=1
                
    return (beleg_komplett, ergebnis_datum, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus)

def fetch_values_small_table_big_font(content_list:list):

    block_position_dict = {"beleg_komplett":5, "ergebnis_datum":3,
    "kennzeichnung":5,"krankheit": 5,"probenanzahl":5, "neg_sus_pos":6 #Problem: steht vor DTiter
    }   
    #Count pos, neg, sus

    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum" #hardcoded change for others
    methode = "ELISA" #hardcoded change for others
    pos = 0
    neg = 0
    sus = 0
    for sub_list in content_list:
        if sub_list[CONST_INDEX]==CONST_BLOCK:
            if sub_list[POS_INDEX]==block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4]
                #print(beleg_komplett_raw)
                beleg_stall_krankheit_snip = beleg_komplett_raw.split("Kommentar")[1]
                stall_found = False
                for snip in beleg_stall_krankheit_snip.replace(",",";").split(";"):
                    if "Lab" in snip:
                        beleg_komplett = snip.split(":")[1].strip().replace(" ","")
                        if "\n" in beleg_komplett:
                            beleg_komplett = beleg_komplett.split("\n")[0]
                    if "Stall" in snip:
                        for sub_snip in snip.split("\n"):
                            if "Stall" in sub_snip:
                                stall_found = True
                                kennzeichnung = sub_snip.strip()
                if not stall_found:
                    kennzeichnung = beleg_stall_krankheit_snip
               
            if sub_list[POS_INDEX]==block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.replace(" ","").replace("\n","").split("Datum")[1].strip().replace(".","/")

                
            if sub_list[POS_INDEX]==block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = krankheit_raw.strip().split("\n")[-1].replace(" ", "").replace("\n","")
                krankheit = "-".join([krankheit.upper(),"IDEXX"])
            if sub_list[POS_INDEX]==block_position_dict["probenanzahl"]:
                probenanzahl_raw = sub_list[4]
                probenanzahl = int(probenanzahl_raw.split("Anzahl")[1].strip().strip("\n").split("\n")[0].strip())
                #print(probenanzahl)
            if sub_list[POS_INDEX]==block_position_dict["neg_sus_pos"]:
                try:
                    neg_sus_pos = sub_list[4].split("Ergebnis")[1].split("A4")[1]

                except IndexError as e : 
                    # This is a very lazy fix, change the parsing and 
                    # look for unique identifiers...
                    print(e)
                    sub_list = content_list[7]
                    neg_sus_pos = sub_list[4].split("Ergebnis")[1].split("A4")[1]
                    
            
                for snip in neg_sus_pos.split("\n"):
        
                    if "Pos" in snip:
                        pos +=1
                    if "Neg" in snip:
                        neg +=1
                    if "Sus" in snip:
                        sus +=1
    
    
    return (beleg_komplett, ergebnis_datum, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus)

def fetch_values_ipv_table(content_list:list):

    material = "Serum" #hardcoded change for others
    methode = "ELISA"
    neg = None
    pos = None
    sus = None
    labor_nummer = None

    def comment_parse(comment_str :str):
        stall_found = False
        for snip in comment_str.replace(",",";").split(";"):
                    if "Lab" in snip:
                        beleg_komplett = snip.split(":")[1].strip().replace(" ","")
                        if "\n" in beleg_komplett:
                            beleg_komplett = beleg_komplett.split("\n")[0]
                    
                    if "Stall" in snip:
                        for sub_snip in snip.split("\n"):
                            if "Stall" in sub_snip:
                                stall_found = True
                                kennzeichnung = sub_snip.strip()
        if not stall_found:
            kennzeichnung = comment_str
        return beleg_komplett, kennzeichnung

    for sublist in content_list:
        if sublist[5]==16:
            comment_str_raw = sublist[4]
            beleg_komplett, kennzeichnung = comment_parse(comment_str_raw)
        if sublist[5]==5:
            titer = int(sublist[4].split("\n")[1])
        if sublist[5]==3:
            probenanzahl = int(sublist[4].split("\n")[1])
        if sublist[5]==11:
            ergebnis_datum = sublist[4].split("\n")[1].strip().replace(".","/")
        if sublist[5]==12:
            krankheit = sublist[4].split("-")[-1].strip()
            krankheit = "-".join([krankheit.upper(),"IDEXX"])
        
        if sublist[5]==7:
            cv = int(float(sublist[4].split("\n")[1].replace(",","."))) # Fix this here are floats!!
        


    return(beleg_komplett, ergebnis_datum, labor_nummer, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus, titer, cv)

def check_no_dup_get_position_big(compare_vals:list):
    global TABLE_NAME
    global COLUMNS

    beleg_komplett, ergebnis_datum, labor_nummer, material, methode, kennzeichnung, krankheit, probenanzahl, pos, neg, sus, titer, cv = compare_vals


    elisa_selection = cursor.execute(f"""Select * 
    FROM {TABLE_NAME}
    WHERE 
    Methode='ELISA'
    """)

    existing_rows = elisa_selection.execute(f"""Select {COLUMNS_BIG}
    FROM {TABLE_NAME}
    WHERE 
    BelegKomplett='{beleg_komplett}'
    """).fetchall()

    duplicate = False 
    date_compare = datetime.datetime.strptime(ergebnis_datum,"%d/%m/%Y")

    for row in existing_rows:
        #check if duplicate
        if (row[0]==beleg_komplett and row[2]==date_compare and
            row[3]==labor_nummer and 
            row[12]==titer and row[13]==cv):
            # Be more selective about krankheit:
            # str(krankheit).lower() in str(row[7]).lower() and
            duplicate = True

    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [el for el in existing_position_labs if el <100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1
        
        return (True, new_position_lab)

def check_no_dup_get_position_small(compare_vals:list):
    global TABLE_NAME
    global COLUMNS_SMALL

    beleg_komplett, ergebnis_datum, material, methode, kennzeichnung, krankheit, probenanzahl, pos, neg, sus= compare_vals


    elisa_selection = cursor.execute(f"""Select * 
    FROM {TABLE_NAME}
    WHERE 
    Methode='ELISA'
    """)

    existing_rows = elisa_selection.execute(f"""Select {COLUMNS_SMALL}
    FROM {TABLE_NAME}
    WHERE 
    BelegKomplett='{beleg_komplett}'
    """).fetchall()


    duplicate = False 
    date_compare = datetime.datetime.strptime(ergebnis_datum,"%d/%m/%Y")
    for row in existing_rows:
        #check if duplicate
        if (row[0]==beleg_komplett and row[2]==date_compare and row[3]==material and
            str(krankheit).lower() in str(row[6]).lower() and row[7]==probenanzahl and row[8]==pos and 
            row[9]==neg and row[10]==sus):
            duplicate = True
    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [el for el in existing_position_labs if el <100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1
        
        return (True, new_position_lab)

def check_no_dup_get_position_ipv(compare_vals:list):
    global TABLE_NAME
    global COLUMNS

    beleg_komplett, ergebnis_datum, labor_nummer, material, methode, kennzeichnung, krankheit, probenanzahl, pos, neg, sus, titer, cv = compare_vals


    elisa_selection = cursor.execute(f"""Select * 
    FROM {TABLE_NAME}
    WHERE 
    Methode='ELISA'
    """)

    existing_rows = elisa_selection.execute(f"""Select {COLUMNS_BIG}
    FROM {TABLE_NAME}
    WHERE 
    BelegKomplett='{beleg_komplett}'
    """).fetchall()

    duplicate = False 
    date_compare = datetime.datetime.strptime(ergebnis_datum,"%d/%m/%Y")

    for row in existing_rows:
        #check if duplicate
        if (row[0]==beleg_komplett and row[2]==date_compare and
            row[12]==titer):
            # Be more selective about krankheit:
            # str(krankheit).lower() in str(row[7]).lower() and
            duplicate = True

    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [el for el in existing_position_labs if el <100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1
        
        return (True, new_position_lab)



# process each page of pdf individually
for page in doc.pages():
    try:
        content_list = page.get_text("blocks")
        big_table = False
        small_table = False
        small_table_big_font = False
        small_ipv_table = False
        for block in content_list:
            # Check which kind of page (big table, small table, not parseable)
            if block[5]==7 and "Firma" in block[4]:
                big_table = True
                break
            if "Titergruppen" in block[4]:
                small_ipv_table = True
                break
            if block[5]==13 and "Test" in block[4]:
                small_table = True
                for block in content_list:
                    if "Titergruppen" in block[4]:
                        small_table = False
                        small_ipv_table = True
                        break               
                break
            if block[5]==5 and "Test" in block[4]:
                small_table_big_font = True
                break
        if big_table:
            values = fetch_values_big_table(content_list)
            no_dup_and_position = check_no_dup_get_position_big(values)
            if no_dup_and_position[0]:
                position_lab = no_dup_and_position[1]
                params = list(values)
                params.insert(1, position_lab)
                insert_sql(cursor,TABLE_NAME,COLUMNS_BIG,params)
            else:
                print("Entry already exists, Skipping ...")
        elif small_table:
            values = fetch_values_small_table(content_list)
            no_dup_and_position = check_no_dup_get_position_small(values)
            if no_dup_and_position[0]:
                position_lab = no_dup_and_position[1]
                params = list(values)
                params.insert(1, position_lab)
                insert_sql(cursor,TABLE_NAME,COLUMNS_SMALL,params)
            else:
                print("Entry already exists, Skipping ...")
        elif small_ipv_table:
            values = fetch_values_ipv_table(content_list)
            no_dup_and_position = check_no_dup_get_position_ipv(values)
            if no_dup_and_position[0]:
                position_lab = no_dup_and_position[1]
                params = list(values)
                params.insert(1, position_lab)
                insert_sql(cursor,TABLE_NAME,COLUMNS_BIG,params)
            else:
                print("Entry already exists, Skipping ...")


        elif small_table_big_font:
            values = fetch_values_small_table_big_font(content_list)
            no_dup_and_position = check_no_dup_get_position_small(values)
            if no_dup_and_position[0]:
                position_lab = no_dup_and_position[1]
                params = list(values)
                params.insert(1, position_lab)
                insert_sql(cursor,TABLE_NAME,COLUMNS_SMALL,params)
            else:
                print("Entry already exists, Skipping ...")        
        else:
            print("Could not parse page.")
            values = None
            continue
    except UnboundLocalError as e:
        print(e)
        continue
    except IndexError as e:
        print(e)
        continue       


# %%
