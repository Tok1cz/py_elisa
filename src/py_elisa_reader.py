# %%
import sys
import pyodbc
import datetime
import fitz

"""
Table Types:
Biocheck - big_table 
    - All biocheck tables.
IDEXX big font - small_table_big_font
    - Idexx Tables with big font
IDEXX SMALL font - small_table
    - Idexx tables with small font
IDEXX Small Font MultiHist - small_ipv_table 
    - IDEXX Table with big histogram nbins>2
"""

"""
Ready again_
uncomment sql insert
reinstate pdf path

"""

TABLE_NAME = 'LaborbefundT'
COLUMNS_BIG = 'BelegKomplett, PositionLab, ErgebnissDatum, Labornummer, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na, Titer, cv'
COLUMNS_SMALL = 'BelegKomplett, PositionLab, ErgebnissDatum, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na'
COLUMNS_MULTI = 'BelegKomplett, PositionLab, ErgebnissDatum,Labornummer, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, Titer, cv'

db_path = r"C:/Synch/MMT.mdb"


pdf_path = " ".join(sys.argv[1::])

# pdf_path = r"C:\Users\konst\Documents\py_workspace\py_elisa\pdfs\completeNew\Taucha_Sonder IBD_Histrogramme.pdf"


connection_str = (
    rf"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path}"
)
connection = pyodbc.connect(connection_str)

cursor = connection.cursor()

doc = fitz.open(pdf_path)


def insert_sql(cursor, table, columns, params):

    # Insertion call
    print(f"Inserting: {params}")
    values_str = len(columns.split(","))*' ?,'
    values_str = values_str.strip().strip(",")

    cursor.execute(f"""INSERT INTO
            {TABLE_NAME}({columns})
            values ({values_str})""", params)

    cursor.commit()

    print(f"New Entry for {params[0]}-LabPos {params[1]} created!")


def fetch_values_big_table(content_list: list):
    # Positions of values in loaded pdf data
    block_position_dict = {"beleg_komplett": 5, "ergebnis_datum": 14,
                           # Problem: steht vor DTiter
                           "labor_nummer": 3, "kennzeichnung": 10, "krankheit": 13, "probenanzahl_titer": 15,
                           "neg_sus_pos": 16, "cv": 18,  # check this one!!
                           }
    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum"  # hardcoded change for others
    methode = "ELISA"  # hardcoded change for others

    def default_clean(raw_string: str):
        return raw_string.split("\n")[1].replace(" ", "")

    # Fetching and cleaning values from pdf data

    for sub_list in content_list:
        if sub_list[CONST_INDEX] == CONST_BLOCK:
            if sub_list[POS_INDEX] == block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4]
                beleg_komplett = default_clean(beleg_komplett_raw)

            elif sub_list[POS_INDEX] == block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.split(
                    "Test Datum")[1].replace(":", "").strip().replace(".", "/")

            elif sub_list[POS_INDEX] == block_position_dict["labor_nummer"]:
                labor_nummer_raw = sub_list[4]
                labor_nummer = default_clean(labor_nummer_raw)

            elif sub_list[POS_INDEX] == block_position_dict["kennzeichnung"]:
                kennzeichnung_raw = sub_list[4]
                kennzeichnung = default_clean(kennzeichnung_raw)

            elif sub_list[POS_INDEX] == block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = "-".join([krankheit_raw.split("Lot")[0].replace(
                    "Test", "").replace(":", "").strip().upper(), "BioChek"])

            elif sub_list[POS_INDEX] == block_position_dict["probenanzahl_titer"]:
                probenanzahl_titer_raw = sub_list[4]
                # watch this, it is weird that it is in front of Titer
                titer_raw, probenanzahl_raw = probenanzahl_titer_raw.split(
                    "No. Proben")
                probenanzahl = int(probenanzahl_raw.replace(":", "").strip())
                titer = int(titer_raw.replace(
                    "Durchschnittstiter", "").replace(":", "").strip())

            elif sub_list[POS_INDEX] == block_position_dict["neg_sus_pos"]:
                neg_sus_pos_raw = sub_list[4]
                neg_sus_pos = neg_sus_pos_raw.split(
                    "Neg/Sus/Pos")[1].replace(":", "").strip().split("/")
                neg = int(neg_sus_pos[0])
                sus = int(neg_sus_pos[1])
                pos = int(neg_sus_pos[2])

            elif sub_list[POS_INDEX] == block_position_dict["cv"]:
                cv_raw = sub_list[4]
                cv = int(cv_raw.replace("%CV", "").replace(":", "").strip())

    return (beleg_komplett, ergebnis_datum, labor_nummer, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus, titer, cv)


def fetch_values_small_table(content_list: list):

    block_position_dict = {"beleg_komplett": 30, "ergebnis_datum": 14,
                           "kennzeichnung": 30, "krankheit": 26, "probenanzahl": 16,  # Problem: steht vor DTiter
                           }
    # Count pos, neg, sus
    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum"  # hardcoded change for others
    methode = "ELISA"  # hardcoded change for others
    pos = 0
    neg = 0
    sus = 0
    for sub_list in content_list:
        if sub_list[CONST_INDEX] == CONST_BLOCK:
            if sub_list[POS_INDEX] == block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4].replace(",", ";").split(";")
                for snip in beleg_komplett_raw:
                    if "Lab" in snip:
                        beleg_snip = snip

                beleg_komplett = beleg_snip.strip().split("\n")[0].split(" ")[
                    1].strip().replace("\n", "")

            if sub_list[POS_INDEX] == block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.replace(
                    " ", "").replace("\n", "").replace(".", "/")

            if sub_list[POS_INDEX] == block_position_dict["kennzeichnung"]:
                kennzeichnung_raw = sub_list[4].replace(",", ";").split(";")
                stall_found = False
                for snip in kennzeichnung_raw:
                    if "Stall" in snip:
                        kennzeichnung_snip = snip
                        stall_found = True
                if stall_found:
                    kennzeichnung = kennzeichnung_snip.strip().replace("\n", "")
                else:

                    kennzeichnung = kennzeichnung_raw[2].replace(
                        " ", "").replace("\n", "")

            if sub_list[POS_INDEX] == block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = krankheit_raw.strip().split(
                    "\n")[-1].replace(" ", "").replace("\n", "")
                krankheit = "-".join([krankheit.upper(), "IDEXX"])
            if sub_list[POS_INDEX] == block_position_dict["probenanzahl"]:
                probenanzahl_raw = sub_list[4]
                try:
                    probenanzahl = int(probenanzahl_raw.replace(
                        "\n", "").replace(" ", ""))
                except ValueError as e:
                    print(e)
        if len(sub_list[4].split("\n")) > 1:

            if not sub_list[4].split("\n")[1] in ["A1", "A2", "A3", "A4"]:
                if "Pos" in sub_list[4]:
                    pos += 1
                if "Neg" in sub_list[4]:
                    neg += 1
                if "Sus" in sub_list[4]:
                    sus += 1

    return (beleg_komplett, ergebnis_datum, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus)


def fetch_values_small_table_big_font(content_list: list):

    block_position_dict = {"beleg_komplett": 5, "ergebnis_datum": 3,
                           # Problem: steht vor DTiter
                           "kennzeichnung": 5, "krankheit": 5, "probenanzahl": 5, "neg_sus_pos": 6
                           }
    # Count pos, neg, sus

    # hopefully constant parts of positions
    CONST_BLOCK = 0
    CONST_INDEX = 6
    POS_INDEX = 5
    material = "Serum"  # hardcoded change for others
    methode = "ELISA"  # hardcoded change for others
    pos = 0
    neg = 0
    sus = 0
    for sub_list in content_list:
        if sub_list[CONST_INDEX] == CONST_BLOCK:
            if sub_list[POS_INDEX] == block_position_dict["beleg_komplett"]:
                beleg_komplett_raw = sub_list[4]
                # print(beleg_komplett_raw)
                beleg_stall_krankheit_snip = beleg_komplett_raw.split("Kommentar")[
                    1]
                stall_found = False
                for snip in beleg_stall_krankheit_snip.replace(",", ";").split(";"):
                    if "Lab" in snip:
                        beleg_komplett = snip.split(
                            ":")[1].strip().replace(" ", "")
                        if "\n" in beleg_komplett:
                            beleg_komplett = beleg_komplett.split("\n")[0]
                    if "Stall" in snip:
                        for sub_snip in snip.split("\n"):
                            if "Stall" in sub_snip:
                                stall_found = True
                                kennzeichnung = sub_snip.strip()
                if not stall_found:
                    kennzeichnung = beleg_stall_krankheit_snip

            if sub_list[POS_INDEX] == block_position_dict["ergebnis_datum"]:
                ergebnis_datum_raw = sub_list[4]
                ergebnis_datum = ergebnis_datum_raw.replace(" ", "").replace(
                    "\n", "").split("Datum")[1].strip().replace(".", "/")

            if sub_list[POS_INDEX] == block_position_dict["krankheit"]:
                krankheit_raw = sub_list[4]
                krankheit = krankheit_raw.strip().split(
                    "\n")[-1].replace(" ", "").replace("\n", "")
                krankheit = "-".join([krankheit.upper(), "IDEXX"])
            if sub_list[POS_INDEX] == block_position_dict["probenanzahl"]:
                probenanzahl_raw = sub_list[4]
                probenanzahl = int(probenanzahl_raw.split("Anzahl")[
                                   1].strip().strip("\n").split("\n")[0].strip())
                # print(probenanzahl)
            if sub_list[POS_INDEX] == block_position_dict["neg_sus_pos"]:
                try:
                    neg_sus_pos = sub_list[4].split(
                        "Ergebnis")[1].split("A4")[1]

                except IndexError as e:
                    # This is a very lazy fix, change the parsing and
                    # look for unique identifiers...
                    print(e)
                    sub_list = content_list[7]
                    neg_sus_pos = sub_list[4].split(
                        "Ergebnis")[1].split("A4")[1]

                for snip in neg_sus_pos.split("\n"):

                    if "Pos" in snip:
                        pos += 1
                    if "Neg" in snip:
                        neg += 1
                    if "Sus" in snip:
                        sus += 1

    return (beleg_komplett, ergebnis_datum, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus)


def fetch_values_ipv_table(content_list: list):

    material = "Serum"  # hardcoded change for others
    methode = "ELISA"
    neg = None
    pos = None
    sus = None
    labor_nummer = None

    def comment_parse(comment_str: str):
        stall_found = False
        for snip in comment_str.replace(",", ";").split(";"):
            if "Lab" in snip:
                beleg_komplett = snip.split(":")[1].strip().replace(" ", "")
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
        if sublist[5] == 16:
            comment_str_raw = sublist[4]
            beleg_komplett, kennzeichnung = comment_parse(comment_str_raw)
        if sublist[5] == 5:
            titer = int(sublist[4].split("\n")[1])
        if sublist[5] == 3:
            probenanzahl = int(sublist[4].split("\n")[1])
        if sublist[5] == 11:
            ergebnis_datum = sublist[4].split(
                "\n")[1].strip().replace(".", "/")
        if sublist[5] == 12:
            krankheit = sublist[4].split("-")[-1].strip()
            krankheit = "-".join([krankheit.upper(), "IDEXX"])

        if sublist[5] == 7:
            # Fix this here are floats!!
            cv = int(float(sublist[4].split("\n")[1].replace(",", ".")))

    return (beleg_komplett, ergebnis_datum, labor_nummer, material, kennzeichnung, methode, krankheit, probenanzahl, pos, neg, sus, titer, cv)


def find_element_by_content_string(search_str: str, content_list: list):
    return [el for el in content_list if search_str in el[4]][0]


def get_values_multiflock_content(content_list: list):
    material = "Serum"  # hardcoded change for others
    methode = "ELISA"
    labor_nummer = None

    labor_nummer_raw = find_element_by_content_string("Lab code", content_list)[
        4]
    labor_nummer = labor_nummer_raw.replace(
        "Lab code", "").replace(":", "").strip()
    beleg_komplett_raw = find_element_by_content_string("Firma", content_list)[
        4]
    beleg_komplett = beleg_komplett_raw.replace(
        "Firma", "").replace(":", "").strip()
    ergebnis_datum_raw = find_element_by_content_string(
        "Test date", content_list)[4]
    ergebnis_datum = ergebnis_datum_raw.split("Test date")[1].replace(
        ":", "").strip().strip("\n").replace(".", "/")
    stallnr_raw = find_element_by_content_string(
        "Stallnummer", content_list)[4]
    kennzeichnung = stallnr_raw.replace(
        "Stallnummer", "").replace(":", "").strip()
    krankheit_raw = find_element_by_content_string(
        "Assay", content_list)[4]
    krankheit = "-".join([krankheit_raw.split("Lot")[0].replace(
        "Assay", "").replace(":", "").strip().upper(), "BioChek"])
    probenanzahl_raw = find_element_by_content_string(
        "No.  samples", content_list)[4]
    probenanzahl = int(probenanzahl_raw.split("No")[0].strip())
    titer_cv_raw = find_element_by_content_string(
        "Mean\nTiter", content_list)[4]
    titer = int(titer_cv_raw.split("Titer")[1].split("VI")[
        0].replace(":", "").strip())
    cv = int(titer_cv_raw.split("Titer")[1].split(
        "VI")[1].replace(":", "").split("\n")[1].strip())

    return (beleg_komplett, ergebnis_datum, labor_nummer, material, kennzeichnung, methode, krankheit, probenanzahl,  titer, cv)


def check_no_dup_get_position_big(compare_vals: list):
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
    date_compare = datetime.datetime.strptime(ergebnis_datum, "%d/%m/%Y")

    for row in existing_rows:
        # check if duplicate
        if (row[0] == beleg_komplett and row[2] == date_compare and
            row[3] == labor_nummer and
                row[12] == titer and row[13] == cv):
            # Be more selective about krankheit:
            # str(krankheit).lower() in str(row[7]).lower() and
            duplicate = True

    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [
            el for el in existing_position_labs if el < 100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1

        return (True, new_position_lab)


def check_no_dup_get_position_small(compare_vals: list):
    global TABLE_NAME
    global COLUMNS_SMALL

    beleg_komplett, ergebnis_datum, material, methode, kennzeichnung, krankheit, probenanzahl, pos, neg, sus = compare_vals

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
    date_compare = datetime.datetime.strptime(ergebnis_datum, "%d/%m/%Y")
    for row in existing_rows:
        # check if duplicate
        if (row[0] == beleg_komplett and row[2] == date_compare and row[3] == material and
            str(krankheit).lower() in str(row[6]).lower() and row[7] == probenanzahl and row[8] == pos and
                row[9] == neg and row[10] == sus):
            duplicate = True
    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [
            el for el in existing_position_labs if el < 100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1

        return (True, new_position_lab)


def check_no_dup_get_position_ipv(compare_vals: list):
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
    date_compare = datetime.datetime.strptime(ergebnis_datum, "%d/%m/%Y")

    for row in existing_rows:
        # check if duplicate
        if (row[0] == beleg_komplett and row[2] == date_compare and
                row[12] == titer):
            # Be more selective about krankheit:
            # str(krankheit).lower() in str(row[7]).lower() and
            duplicate = True

    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [
            el for el in existing_position_labs if el < 100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1

        return (True, new_position_lab)


def check_no_dup_get_position_multi(compare_vals: list):
    global TABLE_NAME
    global COLUMNS

    beleg_komplett, ergebnis_datum, labor_nummer, material, methode, kennzeichnung, krankheit, probenanzahl, titer, cv = compare_vals

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
    date_compare = datetime.datetime.strptime(ergebnis_datum, "%d/%m/%Y")

    for row in existing_rows:
        # check if duplicate
        if (row[0] == beleg_komplett and row[2] == date_compare and
            row[3] == labor_nummer and
                row[12] == titer and row[13] == cv):
            # Be more selective about krankheit:
            # str(krankheit).lower() in str(row[7]).lower() and
            duplicate = True

    if duplicate:
        return (False, -999)
    else:
        existing_position_labs = [row[1] for row in existing_rows]
        # Remove values over a hundred, as there can different not relevant entries in the tables
        clean_existing_position_labs = [
            el for el in existing_position_labs if el < 100]
        if clean_existing_position_labs:
            new_position_lab = max(clean_existing_position_labs) + 1
        else:
            new_position_lab = 1

        return (True, new_position_lab)


def correct_next_page_postions(next_page: list, page_before: list) -> list:
    # We need to adjust page positions for multiple page reports, as positions of blocks are used on following pages
    page_before_last_index = page_before[-1][-2]
    new_start_index = page_before_last_index + 1
    for i in range(len(next_page)):
        next_page[i] = list(next_page[i])
        next_page[i][-2] += new_start_index

    return next_page


# %%
# Pages that are processed as next pages and therefore skipped in the main loop
skip_page_indices = []

pages = list(doc.pages())

is_multi_flock = "Multiple Flocks" in [
    c for c in pages[0].get_text("blocks") if c[5] == 2][0][4]

if is_multi_flock:
    full_content = []
    for page in pages:
        full_content.extend(page.get_text("blocks"))

    start_indices = []
    end_indices = []

    for i in range(len(full_content)):
        if "Lab code" in full_content[i][4]:
            start_indices.append((i))
        if "Comment" in full_content[i][4]:
            end_indices.append((i))

    if len(start_indices) != len(end_indices):
        print("Could not parse histogram pdf. Number of start elements does not match number of end elements!")
    content_lists = []
    for i in range(len(start_indices)):
        content_lists.append(full_content[start_indices[i]:end_indices[i]])
    for content_list in content_lists:
        values = get_values_multiflock_content(content_list)
        no_dup_and_position = check_no_dup_get_position_multi(values)
        if no_dup_and_position[0]:
            position_lab = no_dup_and_position[1]
            params = list(values)
            params.insert(1, position_lab)
            insert_sql(cursor, TABLE_NAME, COLUMNS_MULTI, params)
        else:
            print("Entry already exists, Skipping ...")

else:

    for i in range(len(pages)):

        if i in skip_page_indices:
            continue
        try:
            content_list = pages[i].get_text("blocks")
            big_table = False
            small_table = False
            small_table_big_font = False
            small_ipv_table = False

            for block in content_list:
                # Check which kind of page (big table, small table, not parseable)
                # def is_big_table
                if (block[5] == 7 and "Firma" in block[4]) or block[5] == 5 and "Firma" in block[4]:
                    big_table = True
                    break
                if "Titergruppen" in block[4]:
                    small_ipv_table = True
                    break
                if block[5] == 13 and "Test" in block[4]:
                    small_table = True
                    for block in content_list:
                        if "Titergruppen" in block[4]:
                            small_table = False
                            small_ipv_table = True
                            break
                    break
                if block[5] == 5 and "Test" in block[4]:
                    small_table_big_font = True
                    break

            # Determine whether it is a multi-page report:
            might_have_next_page = True
            potential_next_page_index = i + 1
            page_before_content = content_list
            while might_have_next_page:
                if potential_next_page_index >= len(pages):
                    break
                # TODO: Implement logice for small_table_big_font and small_ipv_table
                next_page_content = pages[potential_next_page_index].get_text(
                    "blocks")
                is_next_page_condition = False

                if small_table:
                    # Das ist mal wieder ein Vorschlaghammerapproach hier...
                    if len(next_page_content) >= 4:
                        is_next_page_condition = "Vertiefung" in next_page_content[3][4]
                    else:
                        might_have_next_page = False
                # Not necessary - mutliple pages are counted differently..
                if big_table:
                    # using position number would be prettier...
                    if next_page_content:
                        is_next_page_condition = "pos" in next_page_content[0][
                            4] or "neg" in next_page_content[0][4] or "sus" in next_page_content[0][4]
                # TODO: Add:
                #  if small_table_big font:
                # ...
                # if small_ipv_table:
                # ...

                if is_next_page_condition:
                    next_page_content = correct_next_page_postions(
                        next_page_content, page_before_content)  # We have to adjust position indices to avoid problems down the road.
                    page_before_content = next_page_content
                    content_list = content_list + next_page_content
                    skip_page_indices.append(potential_next_page_index)
                    potential_next_page_index += 1

                else:
                    might_have_next_page = False

            if big_table:  # Assign the function
                values = fetch_values_big_table(content_list)
                no_dup_and_position = check_no_dup_get_position_big(values)
                if no_dup_and_position[0]:
                    position_lab = no_dup_and_position[1]
                    params = list(values)
                    params.insert(1, position_lab)
                    insert_sql(cursor, TABLE_NAME, COLUMNS_BIG, params)
                else:
                    print("Entry already exists, Skipping ...")
            elif small_table:
                values = fetch_values_small_table(content_list)
                no_dup_and_position = check_no_dup_get_position_small(values)
                if no_dup_and_position[0]:
                    position_lab = no_dup_and_position[1]
                    params = list(values)
                    params.insert(1, position_lab)
                    insert_sql(cursor, TABLE_NAME, COLUMNS_SMALL, params)
                else:
                    print("Entry already exists, Skipping ...")
            elif small_ipv_table:
                values = fetch_values_ipv_table(content_list)
                no_dup_and_position = check_no_dup_get_position_ipv(values)
                if no_dup_and_position[0]:
                    position_lab = no_dup_and_position[1]
                    params = list(values)
                    params.insert(1, position_lab)
                    insert_sql(cursor, TABLE_NAME, COLUMNS_BIG, params)
                else:
                    print("Entry already exists, Skipping ...")

            elif small_table_big_font:
                values = fetch_values_small_table_big_font(content_list)
                no_dup_and_position = check_no_dup_get_position_small(values)
                if no_dup_and_position[0]:
                    position_lab = no_dup_and_position[1]
                    params = list(values)
                    params.insert(1, position_lab)
                    insert_sql(cursor, TABLE_NAME, COLUMNS_SMALL, params)
                else:
                    print("Entry already exists, Skipping ...")
            else:
                print("Could not parse page.")
                values = None
                continue

            print(values)
        except UnboundLocalError as e:
            print(e)
            continue
        except IndexError as e:
            print(e)
            continue


# %%
