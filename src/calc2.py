
#%%
import sys, os, pyodbc
import datetime
import fitz

pdf_path = r"C:\Users\konst\Documents\Py Workspace\py_elisa\pdfs\Alain ET LW 18 Copy do not use\versendet an Alain\Mockrehna 40.LW APV; IBV.pdf"
# Test auf 5

doc = fitz.open(pdf_path)
for page in doc.pages():

    content_list = page.get_text("blocks")
    with open(r"C:\Users\konst\Documents\Py Workspace\py_elisa\Code\out\small4.txt", "w") as f:
        f.write(str(content_list))
    




    # %%
