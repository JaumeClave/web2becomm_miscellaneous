import os
import pandas as pd
import numpy as np
from openpyxl import load_workbook


GREAT_BRITAIN_ABBREVIATION = "GB"
DE_COUNTRY = "DE"
ES_COUNTRY = "ES"
FR_COUNTRY = "FR"
IT_COUNTRY = "IT"
NL_COUNTRY = "NL"
SE_COUNTRY = "SE"
DE_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_DE.xlsx"
ES_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_ES.xlsx"
FR_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_FR.xlsx"
IT_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_IT.xlsx"
NL_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_NL.xlsx"
SE_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_SE.xlsx"
UK_INVENTORY_HEALTH_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files\{}_daily\{}_daily_Inventory Health_GB.xlsx"
EU7_INVENTORY_HEALTH_SAVE_PATH_PLACEHOLDER = r"C:\Users\gbjaudom\LEGO\WE B2B eCommerce CoE - CoE only - Documents\General\2. Amazon KAM\01. EU Wide\2022\30. Operations\AMZ Data\{}_eu7_inventory_health.csv"
DAILY_VC_FILE_PATH = r'C:\Users\gbjaudom\LEGO\WE eComm CoE Analytics Team - Documents\Data\VC Files\Daily Files'


def get_latest_daily_vc_file_date():
    """
    Function finds the latest daily VC file directory by scanning the Daily File directory and returning the last item in list
    :return: date of last saved Daily VC file
    """
    sub_folders = [name for name in os.listdir(DAILY_VC_FILE_PATH) if os.path.isdir(os.path.join(DAILY_VC_FILE_PATH, name))]
    last_date = sub_folders[-1][:8]
    return last_date


def make_inventory_health_file_paths(date):
    """
    Function makes all the file paths (Inventory Health) for EU7
    :param date: date of file in format YYYYMMDD
    :return: file paths for EU7
    """
    de_inv_health_path = DE_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    es_inv_health_path = ES_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    fr_inv_health_path = FR_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    it_inv_health_path = IT_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    nl_inv_health_path = NL_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    se_inv_health_path = SE_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    uk_inv_health_path = UK_INVENTORY_HEALTH_PATH_PLACEHOLDER.format(date, date)
    return de_inv_health_path, es_inv_health_path, fr_inv_health_path, it_inv_health_path, nl_inv_health_path, se_inv_health_path, uk_inv_health_path


def make_eu7_inventory_health_file(inv_health_paths):
    """
    Function creates the EU7 Inv. Health dataframe by combining all the individual market dataframes
    :return: EU7 Inv. Health dataframe
    """
    eu7_inv_heatlh = pd.DataFrame()
    for path in inv_health_paths:
        temp_df = pd.read_excel(path, skiprows=1)
        temp_df["country"] = path[-7:-5]
        eu7_inv_heatlh = eu7_inv_heatlh.append(temp_df)
    return eu7_inv_heatlh


def make_save_of_eu7_inventory_health_file(date, eu7_inv_heatlh):
    """
    Function saves the EU7 Inv. Health file in a specifed location with the called date
    :param date: date of files called/saved
    :return: None
    """
    try:
        eu7_inv_heatlh.to_csv(EU7_INVENTORY_HEALTH_SAVE_PATH_PLACEHOLDER.format(date))
        print("Successfully saved {} EU7 Inv. Health file".format((date)))
    except:
        print("Failed to save {} EU7 Inv. Health file".format((date)))
    return None


def pipeline_make_eu7_inventory_health_file():
    """
    Function pipelines all the steps needed to create and save the EU7 Inv. Health file
    :param date: date to be called
    :return: None
    """
    date = get_latest_daily_vc_file_date()
    inv_health_paths = make_inventory_health_file_paths(date)
    eu7_inv_heatlh = make_eu7_inventory_health_file(inv_health_paths)
    make_save_of_eu7_inventory_health_file(date, eu7_inv_heatlh)
    return None


pipeline_make_eu7_inventory_health_file()
