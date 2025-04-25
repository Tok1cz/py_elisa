# %%
import fitz

import pyodbc
import datetime
from py_elisa_reader import check_no_dup_get_position_big, check_no_dup_get_position_ipv, check_no_dup_get_position_small, fetch_values_big_table, fetch_values_ipv_table, fetch_values_small_table, fetch_values_small_table_big_font, insert_sql
import sys
import os


pdf_path = r"C:\Users\konst\Documents\py_workspace\py_elisa\pdfs\pdfs_standard\Alain ET LW 24 Copy do not use\Groß Stieten3 LW 24.pdf"

doc = fitz.open(pdf_path)
db_path = r"C:/Synch/MMT.mdb"

connection_str = (
    rf"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path}"
)
connection = pyodbc.connect(connection_str)

cursor = connection.cursor()

TABLE_NAME = 'LaborbefundT'
COLUMNS_BIG = 'BelegKomplett, PositionLab, ErgebnissDatum, Labornummer, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na, Titer, cv'
COLUMNS_SMALL = 'BelegKomplett, PositionLab, ErgebnissDatum, Material, Kennzeichnung, Methode, Krankheit, Probenanzahl, AnzahlPos, AnzahlNeg, na'

# %%


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
# %%
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
            if block[5] == 7 and "Firma" in block[4]:  # def is_big_table
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
                    # We have to adjust position indices to avoid problems down the road.
                    next_page_content, page_before_content)
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
# %%


"""
While might_have_next_page:
 if next page:
    FU pageType
        if next page has ordinal in key Position:
            content_list append page content
        else:
            might_have_next_page = False
 else:
  might_have_next_page = False

"""
