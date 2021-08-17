import mysql.connector
import xml.etree.ElementTree as ET
import time
import datetime


def create_db_connection():

    sales_report_DB  = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123123",
    database="sales_report"
    )

    return sales_report_DB


def prepare_sql_insert(row_elements):

    CLIENT_NAME = row_elements["client_name"] if row_elements["client_name"] else ""
    SITE_ID = row_elements["site_id"] if row_elements["site_id"] else ""
    LOCALE = row_elements["locale"] if row_elements["locale"] else ""
    ORDER_DATE_TIME = row_elements["order_date_time"] if row_elements["order_date_time"] else ""
    ORDER_DATE = row_elements["order_date"].replace("T", " ") if row_elements["order_date"] else ""
    ORDER_DATE_HOUR = row_elements["order_date_hour"] if row_elements["order_date_hour"] else ""
    ISO_WEEK = row_elements["iso_week"] if row_elements["iso_week"] else ""
    ISO_MONTH = row_elements["iso_month"] if row_elements["iso_month"] else ""
    ORDER_ID = row_elements["order_id"]
    LINE_ITEM_ID = row_elements["line_item_id"]
    ORDER_STATUS = row_elements["order_status"] if row_elements["order_status"] else ""
    PAYMENT_METHOD_NAME = row_elements["payment_method_name"] if row_elements["payment_method_name"] else ""
    PAYMENT_METHOD_STATUS = row_elements["payment_method_status"] if row_elements["payment_method_status"] else ""
    QUANTITY = row_elements["quantity"]
    SHOPPING_CURRENCY = row_elements["shopping_currency"] if row_elements["shopping_currency"] else ""
    SALES_PRICE_LC = row_elements["sales_price"] if row_elements["sales_price"] else ""
    DISCOUNT_LC = row_elements["discount"] if row_elements["discount"] else ""
    PRODUCT_ID = row_elements["product_id"]
    PRODUCT_NAME = row_elements["product_name"]
    SKU = row_elements["sku"]
    MANUFACTURER_PART_NUMBER = row_elements["manufacturer_part_number"]
    REQ_OFFER_ID = row_elements["req_offer_id"] if row_elements["req_offer_id"] else ""
    ORDER_OFFER_NAME = row_elements["order_offer_name"] if row_elements["order_offer_name"] else ""
    COUPON_STRING = row_elements["coupon_string"] if row_elements["coupon_string"] else ""
    LINE_OFFER_ID = row_elements["line_offer_id"] if row_elements["line_offer_id"] else ""
    LINE_OFFER_NAME = row_elements["line_offer_name"] if row_elements["line_offer_name"] else ""
    LINE_COUPON_STRING = row_elements["line_coupon_string"] if row_elements["line_coupon_string"] else ""
    PROGRAM_ID = row_elements["program_id"] if row_elements["program_id"] else ""
    PROGRAM_NAME = row_elements["program_name"] if row_elements["program_name"] else ""
    SHIP_TO_FIRST_NAME = row_elements["ship_to_first_name"] if row_elements["ship_to_first_name"] else ""
    SHIP_TO_LAST_NAME = row_elements["ship_to_last_name"] if row_elements["ship_to_last_name"] else ""
    SHIP_TO_COMPANY = row_elements["ship_to_company"] if row_elements["ship_to_company"] else ""
    SHIP_TO_EMAIL_ADDRESS = row_elements["ship_to_email_address"]
    SHIP_TO_LINE1 = row_elements["ship_to_line1"] if row_elements["ship_to_line1"] else ""
    SHIP_TO_LINE2 = row_elements["ship_to_line2"] if row_elements["ship_to_line2"] else ""
    SHIP_TO_CITY = row_elements["ship_to_city"] if row_elements["ship_to_city"] else ""
    SHIP_TO_STATE = row_elements["ship_to_state"] if row_elements["ship_to_state"] else ""
    SHIP_TO_ZIPCODE = row_elements["ship_to_zipcode"] if row_elements["ship_to_zipcode"] else ""
    SHIP_TO_COUNTRY = row_elements["ship_to_country"] if row_elements["ship_to_country"] else ""
    SHIP_TO_PHONE = row_elements["ship_to_phone"] if row_elements["ship_to_phone"] else ""
    BILL_TO_FIRST_NAME = row_elements["bill_to_first_name"] if row_elements["bill_to_first_name"] else ""
    BILL_TO_LAST_NAME = row_elements["bill_to_last_name"] if row_elements["bill_to_last_name"] else ""
    BILL_TO_COMPANY = row_elements["bill_to_company"] if row_elements["bill_to_company"] else ""
    BILL_TO_EMAIL_ADDRESS = row_elements["bill_to_email_address"]
    BILL_TO_LINE1 = row_elements["bill_to_line1"] if row_elements["bill_to_line1"] else ""
    BILL_TO_LINE2 = row_elements["bill_to_line2"] if row_elements["bill_to_line2"] else ""
    BILL_TO_CITY = row_elements["bill_to_city"] if row_elements["bill_to_city"] else ""
    BILL_TO_STATE = row_elements["bill_to_state"] if row_elements["bill_to_state"] else ""
    BILL_TO_ZIPCODE = row_elements["bill_to_zipcode"] if row_elements["bill_to_zipcode"] else ""
    BILL_TO_COUNTRY = row_elements["bill_to_country"] if row_elements["bill_to_country"] else ""
    BILL_TO_PHONE = row_elements["bill_to_phone"] if row_elements["bill_to_phone"] else ""
    DIVISION = row_elements["division"] if row_elements["division"] else ""
    OPT_IN = row_elements["opt_in"] if row_elements["opt_in"] else ""
    CUSTOMER_ID = row_elements["customer_id"] if row_elements["customer_id"] else ""
    USER_EXTERNAL_REFERENCE = row_elements["user_external_reference"] if row_elements["user_external_reference"] else ""
    USER_LOGIN_ID = row_elements["user_login_id"] if row_elements["user_login_id"] else ""
    THE_SECONDARY_ORDER_ID = row_elements["the_secondary_order_id"] if row_elements["the_secondary_order_id"] else ""
    USER_NAME = row_elements["user_name"] if row_elements["user_name"] else ""
    USER_DESIGNATION = row_elements["user_designation"] if row_elements["user_designation"] else ""
    PURCHASE_PLAN_ID = row_elements["purchase_plan_id"] if row_elements["purchase_plan_id"] else ""
    PURCHASE_PLAN_NAME = row_elements["purchase_plan_name"] if row_elements["purchase_plan_name"] else ""
    MARKET_ID = row_elements["market_id"] if row_elements["market_id"] else ""
    ACTIVATION_NAME = row_elements["activation_name"] if row_elements["activation_name"] else ""
    THEME_NAME = row_elements["theme_name"] if row_elements["theme_name"] else ""
    CARD_TYPE = row_elements["card_type"] if row_elements["card_type"] else ""
    DELIVERY_TYPE = row_elements["delivery_type"] if row_elements["delivery_type"] else ""
    FINANCING_TERM_DESCRIPTION = row_elements["financing_term_description"] if row_elements["financing_term_description"] else ""
    FINANCING_TERM_ID = row_elements["financing_term_id"] if row_elements["financing_term_id"] else ""
    IP_ADDRESS = row_elements["ip_address"] if row_elements["ip_address"] else ""
    LINE_SHIP_STRING = row_elements["line_ship_string"] if row_elements["line_ship_string"] else ""
    ORDER_SHIP_STRING = row_elements["order_ship_string"] if row_elements["order_ship_string"] else ""
    TRANSACTION_TYPE = row_elements["transaction_type"] if row_elements["transaction_type"] else ""
    PRODUCT_FAMILY = row_elements["product_family"] if row_elements["product_family"] else ""
    PRODUCT_OWNING_COMPANY = row_elements["product_owning_company"] if row_elements["product_owning_company"] else ""
    PRODUCT_TYPE = row_elements["product_type"] if row_elements["product_type"] else ""
    SHIPPING_METHOD = row_elements["shipping_method"] if row_elements["shipping_method"] else ""
    ORDER_ORIGIN_TYPE = row_elements["order_origin_type"] if row_elements["order_origin_type"] else ""
    LANDED_COST_AMT_LC = row_elements["landed_cost_amt"] if row_elements["landed_cost_amt"] else ""
    SALES_TAX_AMT_LC = row_elements["sales_tax_amt"] if row_elements["sales_tax_amt"] else ""
    SHIPPING_DISCOUNT_AMT_LC = row_elements["shipping_discount_amt"] if row_elements["shipping_discount_amt"] else ""

    sql = 'insert into dr_order_us ( \
    CLIENT_NAME, \
    SITE_ID, \
    LOCALE, \
    ORDER_DATE_TIME, \
    ORDER_DATE, \
    ORDER_DATE_HOUR, \
    ISO_WEEK, \
    ISO_MONTH, \
    ORDER_ID, \
    LINE_ITEM_ID, \
    ORDER_STATUS, \
    PAYMENT_METHOD_NAME, \
    PAYMENT_METHOD_STATUS, \
    QUANTITY, \
    SHOPPING_CURRENCY, \
    SALES_PRICE_LC, \
    DISCOUNT_LC, \
    PRODUCT_ID, \
    PRODUCT_NAME, \
    SKU, \
    MANUFACTURER_PART_NUMBER, \
    REQ_OFFER_ID, \
    ORDER_OFFER_NAME, \
    COUPON_STRING, \
    LINE_OFFER_ID, \
    LINE_OFFER_NAME, \
    LINE_COUPON_STRING, \
    PROGRAM_ID, \
    PROGRAM_NAME, \
    SHIP_TO_FIRST_NAME, \
    SHIP_TO_LAST_NAME, \
    SHIP_TO_COMPANY, \
    SHIP_TO_EMAIL_ADDRESS, \
    SHIP_TO_LINE1, \
    SHIP_TO_LINE2, \
    SHIP_TO_CITY, \
    SHIP_TO_STATE, \
    SHIP_TO_ZIPCODE, \
    SHIP_TO_COUNTRY, \
    SHIP_TO_PHONE, \
    BILL_TO_FIRST_NAME, \
    BILL_TO_LAST_NAME, \
    BILL_TO_COMPANY, \
    BILL_TO_EMAIL_ADDRESS, \
    BILL_TO_LINE1, \
    BILL_TO_LINE2, \
    BILL_TO_CITY, \
    BILL_TO_STATE, \
    BILL_TO_ZIPCODE, \
    BILL_TO_COUNTRY, \
    BILL_TO_PHONE, \
    DIVISION, \
    OPT_IN, \
    CUSTOMER_ID, \
    USER_EXTERNAL_REFERENCE, \
    USER_LOGIN_ID, \
    THE_SECONDARY_ORDER_ID, \
    USER_NAME, \
    USER_DESIGNATION, \
    PURCHASE_PLAN_ID, \
    PURCHASE_PLAN_NAME, \
    MARKET_ID, \
    ACTIVATION_NAME, \
    THEME_NAME, \
    CARD_TYPE, \
    DELIVERY_TYPE, \
    FINANCING_TERM_DESCRIPTION, \
    FINANCING_TERM_ID, \
    IP_ADDRESS, \
    LINE_SHIP_STRING, \
    ORDER_SHIP_STRING, \
    TRANSACTION_TYPE, \
    PRODUCT_FAMILY, \
    PRODUCT_OWNING_COMPANY, \
    PRODUCT_TYPE, \
    SHIPPING_METHOD, \
    ORDER_ORIGIN_TYPE, \
    LANDED_COST_AMT_LC,\
    SALES_TAX_AMT_LC,\
    SHIPPING_DISCOUNT_AMT_LC) \
    \
    Values ("'\
    + CLIENT_NAME + '", "'\
    + SITE_ID + '", "'\
    + LOCALE + '", "'\
    + ORDER_DATE_TIME + '", "'\
    + ORDER_DATE + '", "'\
    + ORDER_DATE_HOUR + '", "'\
    + ISO_WEEK + '", "'\
    + ISO_MONTH + '", "'\
    + ORDER_ID + '", "'\
    + LINE_ITEM_ID + '", "'\
    + ORDER_STATUS + '", "'\
    + PAYMENT_METHOD_NAME + '", "'\
    + PAYMENT_METHOD_STATUS + '", "'\
    + QUANTITY + '", "'\
    + SHOPPING_CURRENCY + '", "'\
    + SALES_PRICE_LC + '", "'\
    + DISCOUNT_LC + '", "'\
    + PRODUCT_ID + '", "'\
    + PRODUCT_NAME + '", "'\
    + SKU + '", "'\
    + MANUFACTURER_PART_NUMBER + '", "'\
    + REQ_OFFER_ID + '", "'\
    + ORDER_OFFER_NAME + '", "'\
    + COUPON_STRING + '", "'\
    + LINE_OFFER_ID + '", "'\
    + LINE_OFFER_NAME + '", "'\
    + LINE_COUPON_STRING + '", "'\
    + PROGRAM_ID + '", "'\
    + PROGRAM_NAME + '", "'\
    + SHIP_TO_FIRST_NAME + '", "'\
    + SHIP_TO_LAST_NAME + '", "'\
    + SHIP_TO_COMPANY + '", "'\
    + SHIP_TO_EMAIL_ADDRESS + '", "'\
    + SHIP_TO_LINE1 + '", "'\
    + SHIP_TO_LINE2 + '", "'\
    + SHIP_TO_CITY + '", "'\
    + SHIP_TO_STATE + '", "'\
    + SHIP_TO_ZIPCODE + '", "'\
    + SHIP_TO_COUNTRY + '", "'\
    + SHIP_TO_PHONE + '", "'\
    + BILL_TO_FIRST_NAME + '", "'\
    + BILL_TO_LAST_NAME + '", "'\
    + BILL_TO_COMPANY + '", "'\
    + BILL_TO_EMAIL_ADDRESS + '", "'\
    + BILL_TO_LINE1 + '", "'\
    + BILL_TO_LINE2 + '", "'\
    + BILL_TO_CITY + '", "'\
    + BILL_TO_STATE + '", "'\
    + BILL_TO_ZIPCODE + '", "'\
    + BILL_TO_COUNTRY + '", "'\
    + BILL_TO_PHONE + '", "'\
    + DIVISION + '", "'\
    + OPT_IN + '", "'\
    + CUSTOMER_ID + '", "'\
    + USER_EXTERNAL_REFERENCE + '", "'\
    + USER_LOGIN_ID + '", "'\
    + THE_SECONDARY_ORDER_ID + '", "'\
    + USER_NAME + '", "'\
    + USER_DESIGNATION + '", "'\
    + PURCHASE_PLAN_ID + '", "'\
    + PURCHASE_PLAN_NAME + '", "'\
    + MARKET_ID + '", "'\
    + ACTIVATION_NAME  + '", "'\
    + THEME_NAME + '", "'\
    + CARD_TYPE + '", "'\
    + DELIVERY_TYPE + '", "'\
    + FINANCING_TERM_DESCRIPTION + '", "'\
    + FINANCING_TERM_ID + '", "'\
    + IP_ADDRESS + '", "'\
    + LINE_SHIP_STRING + '", "'\
    + ORDER_SHIP_STRING + '", "'\
    + TRANSACTION_TYPE + '", "'\
    + PRODUCT_FAMILY + '", "'\
    + PRODUCT_OWNING_COMPANY + '", "'\
    + PRODUCT_TYPE + '", "'\
    + SHIPPING_METHOD + '", "'\
    + ORDER_ORIGIN_TYPE + '", "'\
    + LANDED_COST_AMT_LC + '", "'\
    + SALES_TAX_AMT_LC + '", "'\
    + SHIPPING_DISCOUNT_AMT_LC + '" )'

    return sql
    

def prepare_sql_update(ORDER_ID, LINE_ITEM_ID, ORDER_STATUS, PAYMENT_METHOD_STATUS):

    sql = "update dr_order_us\
    set ORDER_STATUS = '" + ORDER_STATUS + "', PAYMENT_METHOD_STATUS = '" + PAYMENT_METHOD_STATUS + "' \
    where ORDER_ID = '" + ORDER_ID + "' and \
    LINE_ITEM_ID = '" + LINE_ITEM_ID + "' and \
    (ORDER_STATUS <> '" + ORDER_STATUS + "' or PAYMENT_METHOD_STATUS <> '" + PAYMENT_METHOD_STATUS + "')"
   
    return sql


def whether_to_update(ORDER_ID, LINE_ITEM_ID, ORDER_STATUS, PAYMENT_METHOD_STATUS):

    sql = "select ORDER_STATUS from dr_order_us \
    where ORDER_ID = '" + ORDER_ID + "' and \
    LINE_ITEM_ID = '" + LINE_ITEM_ID + "' and \
    (ORDER_STATUS <> '" + ORDER_STATUS + "' or PAYMENT_METHOD_STATUS <> '" + PAYMENT_METHOD_STATUS + "')"
    cursor.execute(sql)
    selected = cursor.fetchall()
    if(selected):
        return True
    else:
        return False


tree = ET.parse("Demand Sales Transaction Detail - All sites 2021-08-03 084715.xml")
root = tree.getroot()
deprecated_tags = ["order_date_min", "order_date_sec", "shipping_amt", "product_data_id", "is_private_store", "themeid", "email_address_hashed_value", "line_discount_amt", "list_price_per_qty", "order_disc_amt_ext"]
sales_report_DB = create_db_connection()
cursor = sales_report_DB.cursor()
row_elements = {}
for data in root.iter("data"):
    for row in data:
        for i in range(0, len(row)):
            if(row[i].tag not in deprecated_tags):
                row_elements[row[i].tag] = row[i].text

        need_to_update = whether_to_update(row_elements["order_id"], row_elements["line_item_id"], row_elements["order_status"], row_elements["payment_method_status"])      
        
        if(need_to_update):
            sql = prepare_sql_update(row_elements["order_id"], row_elements["line_item_id"], row_elements["order_status"], row_elements["payment_method_status"])
        else:
            sql = prepare_sql_insert(row_elements)
    
        cursor.execute(sql)
        sales_report_DB.commit()
        