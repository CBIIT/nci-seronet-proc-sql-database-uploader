import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import email.utils
import yaml
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import json
import boto3
import re
import os
from dateutil.parser import parse
import datetime
import numpy as np
#from import_loader_v2 import pd, np, pd_s3, boto3, datetime, os, re
#from import_loader_v2 import get_box_data_v2, parse, pathlib
#import mysql.connector
import copy
import pathlib
import urllib3
from decimal import Decimal
import pandas as pd
import sqlalchemy as sd
#import get_box_data_v2

error_msg = list()
success_msg = list()

def lambda_handler(event, context):
    global error_msg
    global success_msg
    error_msg.clear()
    success_msg.clear()
    #print(error_msg)
    #print(success_msg)
    # TODO implement
    s3_client = boto3.client("s3")
    ssm = boto3.client("ssm")
    http = urllib3.PoolManager()
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    sub_folder = "Vaccine Response Submissions"
    #set up parameters base on the event
    Update_Assay_Data = False
    Update_Study_Design = False
    Update_BSI_Tables = False
    Add_Blinded_Results = False
    update_CDC_tables = False
    key_list = []
    db_name = "seronetdb-Vaccine_Response"
    for record in event['Records']:
        key_list.append(record['s3']['object']['key'])
    print(key_list)
    if any('Serology_Data_Files/Reference_Panel_Files/Reference_Panel_Submissions/' in key for key in key_list):
        Add_Blinded_Results = True
    if any('CBC_Folders/Assay_Data/' in key for key in key_list):
        Update_Assay_Data = True
    if any('Vaccine_Respone_Study_Design/' in key for key in key_list):
        Update_Study_Design = True
    if any('Serology_Data_Files/biorepository_id_map/' in key for key in key_list):
        Update_BSI_Tables = True
    if any('Reference+Pannel+Submissions' in key for key in key_list):
        sub_folder = 'Reference Pannel Submissions'
        db_name = "seronetdb-Validated"
    connection_tuple = connect_to_sql_db(host_client, user_name, user_password, db_name)
    kwargs = {'Update_Assay_Data': Update_Assay_Data, 'Update_Study_Design': Update_Study_Design, 'Update_BSI_Tables': Update_BSI_Tables, 'Add_Blinded_Results': Add_Blinded_Results, "update_CDC_tables": update_CDC_tables}
    file_key = key_list[0]
    sql_table_dict, all_submissions = Db_loader_main(file_key, sub_folder, connection_tuple, s3_client, bucket_name, **kwargs)
    ''''''
    email_host = "email-smtp.us-east-1.amazonaws.com"
    email_port = 587
    USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")
    PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")
    SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")
    RECIPIENT_RAW = ssm.get_parameter(Name="SQL_Database_Uploader_Recipents", WithDecryption=True).get("Parameter").get("Value")
    RECIPIENT = RECIPIENT_RAW.replace(" ", "")
    RECIPIENT_LIST = RECIPIENT.split(",")
    SUBJECT = 'SQL Database Uploader'
    SENDERNAME = 'SeroNet Data Team (Data Curation)'

    if len(error_msg) > len(all_submissions):
        message_slack_fail = ""
        for error_message in error_msg:
            message_slack_fail = message_slack_fail + '\n'+ error_message
        failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
        message_slack_fail = message_slack_fail + '\n'+ 'The data was failed to be uploaded into the database'
        print('The data was failed to be uploaded into the database')
        data={"text": message_slack_fail}
        r=http.request("POST", failure, body=json.dumps(data), headers={"Content-Type":"application/json"})

        for recipient in RECIPIENT_LIST:
            msg_text = message_slack_fail
            msg = MIMEMultipart('alternative')
            msg['Subject'] = SUBJECT
            msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
            part1 = MIMEText(msg_text, "plain")
            msg.attach(part1)
            msg['To'] = recipient
            send_email_func(email_host, email_port, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)

    #send the message to slack channel
    else:
        message_slack_success = ""
        for success_message in success_msg:
            message_slack_success = message_slack_success + '\n'+ success_message
        #send the message to slack channel
        message_slack_success = message_slack_success + '\n' + 'The data was successfully uploaded into the database'
        print('The data was successfully uploaded into the database')
        data={"text": message_slack_success}
        success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
        r=http.request("POST", success, body=json.dumps(data), headers={"Content-Type":"application/json"})
        
        for recipient in RECIPIENT_LIST:
            msg_text = message_slack_success
            msg = MIMEMultipart('alternative')
            msg['Subject'] = SUBJECT
            msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
            part1 = MIMEText(msg_text, "plain")
            msg.attach(part1)
            msg['To'] = recipient
            send_email_func(email_host, email_port, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def connect_to_sql_db(host_client, user_name, user_password, file_dbname):
    global error_msg
    #host_client = "Host Client"
    #user_name = "User Name"
    #user_password = "User Password"
    #file_dbname = "Name of database"

    sql_column_df = pd.DataFrame(columns=["Table_Name", "Column_Name", "Var_Type", "Primary_Key", "Autoincrement",
                                          "Foreign_Key_Table", "Foreign_Key_Column"])
    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    engine = engine.execution_options(autocommit=False)
    conn = engine.connect()
    metadata = sd.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        try:
            curr_table = metadata.tables[t]
            curr_table = curr_table.columns.values()
            for curr_row in range(len(curr_table)):
                curr_dict = {"Table_Name": t, "Column_Name": str(curr_table[curr_row].name),
                             "Var_Type": str(curr_table[curr_row].type),
                             "Primary_Key": str(curr_table[curr_row].primary_key),
                             "Autoincrement": False,
                             "Foreign_Key_Count": 0,
                             "Foreign_Key_Table": 'None',
                             "Foreign_Key_Column": 'None'}
                curr_dict["Foreign_Key_Count"] = len(curr_table[curr_row].foreign_keys)
                if curr_table[curr_row].autoincrement is True:
                    curr_dict["Autoincrement"] = True
                if len(curr_table[curr_row].foreign_keys) == 1:
                    key_relation = list(curr_table[curr_row].foreign_keys)[0].target_fullname
                    key_relation = key_relation.split(".")
                    curr_dict["Foreign_Key_Table"] = key_relation[0]
                    curr_dict["Foreign_Key_Column"] = key_relation[1]


                sql_column_df = pd.concat([sql_column_df, pd.DataFrame.from_records([curr_dict])])
        except Exception as e:
            error_msg.append(str(e))
            print(e)
    print("## Sucessfully Connected to " + file_dbname + " ##")
    sql_column_df.reset_index(inplace=True, drop=True)
    return sql_column_df, engine, conn

def send_email_func(HOST, PORT, USERNAME_SMTP, PASSWORD_SMTP, SENDER, recipient, msg):
    server = smtplib.SMTP(HOST, PORT)
    server.ehlo()
    server.starttls()
    #stmplib docs recommend calling ehlo() before & after starttls()
    server.ehlo()
    server.login(USERNAME_SMTP, PASSWORD_SMTP)

    server.sendmail(SENDER, recipient, msg.as_string())
    server.close()




def Db_loader_main(file_key, sub_folder, connection_tuple, s3_client, bucket_name, **kwargs):
    global error_msg
    global success_msg
    """main function that will import data from s3 bucket into SQL database"""
    pd.options.mode.chained_assignment = None
    #s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,region_name='us-east-1')
    #bucket_name = "nci-cbiit-seronet-submissions-passed"
    #bucket_name = "seronet-demo-submissions-passed"
    data_release = "2.0.0"

    if sub_folder == "Reference Pannel Submissions":
        sql_table_dict = get_sql_dict_ref(s3_client, bucket_name)
    elif sub_folder == "Vaccine Response Submissions":
        sql_table_dict = get_sql_dict_vacc(s3_client, bucket_name)
    else:
        return
    Update_Assay_Data = get_kwarg_parms("Update_Assay_Data", kwargs)
    Update_Study_Design = get_kwarg_parms("Update_Study_Design", kwargs)
    Update_BSI_Tables = get_kwarg_parms("Update_BSI_Tables", kwargs)
    loading_result = ''
############################################################################################################################
    try:
        conn = connection_tuple[2]
        engine = connection_tuple[1]
        sql_column_df = connection_tuple[0]

        done_submissions = pd.read_sql(("SELECT * FROM Submission"), conn)  # list of all submissions previously done in db
        done_submissions.drop_duplicates("Submission_S3_Path", inplace=True)

        # cohort_doc = pd.read_excel(r"C:\Users\breadsp2\Downloads\Release_1.0.0_by_cohort (2).xlsx")
        # cohort_doc.rename(columns={"Cohort": "Primary_Study_Cohort"}, inplace=True)
        # cohort_doc.drop("CBC", axis=1, inplace=True)
        # for index in cohort_doc.index:
        #     part_id = cohort_doc["Research_Participant_ID"][index]
        #     cohort_name = cohort_doc["Primary_Study_Cohort"][index]
        #     sql_query = f"Update Participant_Visit_Info set Primary_Study_Cohort = '{cohort_name}' where Research_Participant_ID = '{part_id}'"
        #     engine.execute(sql_query)
        #     conn.connection.commit()

        #part_data = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Participant;"), conn)
        #v1 = part_data.query("Sunday_Prior_To_First_Visit > datetime.date(2000,1,1,)")
        #v2 = part_data.query("Sunday_Prior_To_First_Visit == datetime.date(2000,1,1,)")

        #part_list = "'" + ("', '").join(v1["Research_Participant_ID"].tolist()) + "'"
        #sql_qry = f"update Participant_Visit_Info set Data_Release_Version = '1.1.0' where Research_Participant_ID in ({part_list})"
        #engine.execute(sql_qry)
        #conn.connection.commit()

        #part_list = "'" + ("', '").join(v2["Research_Participant_ID"].tolist()) + "'"
        #sql_qry = f"update Participant_Visit_Info set Data_Release_Version = '2.0.0' where Research_Participant_ID in ({part_list})"
        #engine.execute(sql_qry)
        #conn.connection.commit()


        all_submissions = []  # get list of all submissions by CBC
        cbc_code = []
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Feinstein_CBC01", 41, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "UMN_CBC02", 27, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "ASU_CBC03", 32, all_submissions, cbc_code)
        all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Mt_Sinai_CBC04", 14, all_submissions, cbc_code)
        #all_submissions = [os.path.dirname(file_key)]
        file_key = os.path.dirname(file_key)
        file_key = file_key.replace('+', ' ')

        all_submissions = [i for i in all_submissions if file_key in i]
        print(all_submissions)
        time_stamp = [i.split("/")[2] for i in all_submissions]
        for i in time_stamp:
            if i[2] == '-':
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%H-%M-%S-%m-%d-%Y")
            else:
                time_stamp[time_stamp.index(i)] = datetime.datetime.strptime(i, "%Y-%m-%d-%H-%M-%S")

        #  sort need to work by date time
        all_submissions = [x for _, x in sorted(zip(time_stamp, all_submissions))]  # sort submission list by time submitted
        cbc_code = [x for _, x in sorted(zip(time_stamp, cbc_code))]  # sort submission list by time submitted

        all_submissions = [i for i in enumerate(zip(all_submissions, cbc_code))]
        # Filter list by submissions already done
        all_submissions = [i for i in all_submissions if i[1][0] not in done_submissions["Submission_S3_Path"].tolist()]
        if len(all_submissions) == 0:
            loading_result = 'submission duplicated'
        #  all_submissions = [i for i in all_submissions if "CBC02" in i[1][0]]

    except Exception as e:
        all_submissions = []
        error_msg.append(str(e))
        print(e)
############################################################################################################################
    master_dict = {}  # dictionary for all submissions labeled as create
    update_dict = {}  # dictionary for all submissions labeled as update
    #  all_submissions = [(99, done_submissions["Submission_S3_Path"].tolist()[99])]
    #  all_submissions = all_submissions[-5:]

    try:
        for curr_sub in all_submissions:
            try:
                index = curr_sub[0]# + 1
            except Exception as e:
                error_msg.append(str(e))
                print(e)

            folder_path, folder_tail = os.path.split(curr_sub[1][0])
            file_name = curr_sub[1][0].split("/")
            folders = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)["Contents"]
            print(f"\nWorking on Submision #{index}: {file_name[1]}:  {file_name[2]} \n {file_name[3]}")
            success_msg.append(f"Working on Submision #{index}: {file_name[1]}:  {file_name[2]} {file_name[3]}")
            error_msg.append(f"Working on Submision #{index}: {file_name[1]}:  {file_name[2]} {file_name[3]}")

            upload_date, intent, sub_name = get_upload_info(s3_client, bucket_name, curr_sub, sub_folder)  # get submission info
            if intent == "Create":
                master_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict, data_release)
            elif intent == "Update":
                update_dict = get_tables_to_load(s3_client, bucket_name, folders, curr_sub, conn, sub_name, index, upload_date, intent, update_dict, data_release)
            else:
                print(f"Submission Intent: {intent} is not valid")

        master_dict = fix_aliquot_ids(master_dict, "last", sql_column_df)
        update_dict = fix_aliquot_ids(update_dict, "last", sql_column_df)
        master_data_dict = get_master_dict(master_dict, update_dict, sql_column_df, sql_table_dict)

        #if "baseline_visit_date.csv" in master_data_dict and "baseline.csv" in master_data_dict:
        #    x = master_data_dict["baseline_visit_date.csv"]["Data_Table"]
        #    x.rename(columns={"Sunday_Of_Week": "Sunday_Prior_To_First_Visit"}, inplace=True)
        #    x["Sunday_Prior_To_First_Visit"] = pd.to_datetime(x["Sunday_Prior_To_First_Visit"])#
        #
        #    for curr_part in x.index:
        #        sql_qry = (f"update Participant set Sunday_Prior_To_First_Visit = '{x['Sunday_Prior_To_First_Visit'][curr_part].date()}' " +
        #                   f"where Research_Participant_ID = '{x['Research_Participant_ID'][curr_part]}'")
        #        engine.execute(sql_qry)
        #        conn.connection.commit()
        #
        #    y = master_data_dict["baseline.csv"]
        #    y = master_data_dict["baseline.csv"]["Data_Table"]
        #    master_data_dict["baseline.csv"]["Data_Table"] = y.merge(x[["Research_Participant_ID","Sunday_Prior_To_First_Visit"]])
        #    master_data_dict = {"baseline.csv": master_data_dict["baseline.csv"]}

        if "baseline.csv" in master_data_dict:
            master_data_dict = update_obesity_values(master_data_dict)
        #    master_data_dict = {"baseline.csv": master_data_dict["baseline.csv"], "submission.csv": master_data_dict["submission.csv"]}

        #if study_type == "Vaccine_Response":
        #    cohort_file = r"C:\Users\breadsp2\Downloads\Release_1.0.0_by_cohort.xlsx"
        #    x = pd.read_excel(cohort_file)
        #    visit_table = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info;"), sql_tuple[1])
        #    x.drop("CBC", axis=1, inplace=True)
        #    visit_table.rename(columns={"Cohort": "CBC_Grouping"}, inplace=True)
        #    y = visit_table.merge(x)
        #    update_tables(conn, engine, ["Visit_Info_ID"], y, "Participant_Visit_Info")
        if Update_Assay_Data is True:
            master_data_dict = upload_assay_data(master_data_dict, bucket_name, s3_client)
        if Update_Study_Design is True:
            master_data_dict["study_design.csv"] = {"Data_Table": []}
            #master_data_dict["study_design.csv"]["Data_Table"] = get_box_data_v2.get_study_design()
            master_data_dict["study_design.csv"]["Data_Table"] = get_study_design(s3_client, bucket_name)
        if Update_BSI_Tables is True:
            master_data_dict = get_bsi_files(s3_client, bucket_name, sub_folder, master_data_dict)
        if "secondary_confirmation_test_result.csv" in master_data_dict:
            master_data_dict = update_secondary_confirm(master_data_dict, sql_column_df)

        if "Add_Blinded_Results" in kwargs:
            eval_data = []
            if kwargs["Add_Blinded_Results"] is True:
                success_msg.append("## The blinded result data has been updated")
                #bucket = "nci-cbiit-seronet-submissions-passed"
                bucket = bucket_name
                key = "Serology_Data_Files/Reference_Panel_Files/Reference_Panel_Submissions/"
                resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
                for curr_file in resp["Contents"]:
                    try:
                        if "blinded_validation_panel_results_example" in curr_file["Key"]:  #testing file, ignore
                            continue
                        elif ".xlsx" in curr_file["Key"]:
                            obj = s3_client.get_object(Bucket=bucket, Key= curr_file["Key"])
                            x = pd.read_excel(obj['Body'].read(), engine='openpyxl') # 'Body' is a key word
                            #x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="xlsx",
                                                       #format="xlsx", na_filter=False, output_type="pandas")
                        elif ".csv" in curr_file["Key"]:
                            obj = s3_client.get_object(Bucket=bucket, Key= curr_file["Key"])
                            x = pd.read_csv(obj['Body'])
                            #x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="csv",
                                                       #format="csv", na_filter=False, output_type="pandas")
                        else:
                            continue
                        if len(eval_data) == 0:
                            eval_data = x
                        else:
                            eval_data = pd.concat([eval_data, x])
                    except Exception as e:
                        error_msg.append(str(e))
                        print(e)
                eval_data.reset_index(inplace=True, drop=True)
                sub_id = eval_data.loc[eval_data['Subaliquot_ID'].str.contains('FD|FS')]
                bsi_id = eval_data.loc[~eval_data['Subaliquot_ID'].str.contains('FD|FS')]
                sub_id.rename(columns={'Subaliquot_ID': "CGR_Aliquot_ID"}, inplace=True)

                child_data = pd.read_sql(("SELECT Biorepository_ID, Subaliquot_ID, CGR_Aliquot_ID FROM BSI_Child_Aliquots"), conn)
                eval_data = pd.concat([sub_id.merge(child_data), bsi_id.merge(child_data)])
                master_data_dict = add_assay_to_dict(master_data_dict, "Blinded_Evaluation_Panels.csv", eval_data)

        if "update_CDC_tables" in kwargs:
            if kwargs["update_CDC_tables"] is True:
                #bucket = "nci-cbiit-seronet-submissions-passed"
                bucket = bucket_name
                key = "Serology_Data_Files/CDC_Confirmation_Results/"
                resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
                CDC_data_IgG = pd.DataFrame()
                CDC_data_IgM = pd.DataFrame()
                for curr_file in resp["Contents"]:
                    #  curr_file = get_recent_date(resp["Contents"])
                    try:
                        if ".xlsx" in curr_file["Key"]:
                            file_date = curr_file["Key"][-13:-5]
                            CDC_data_IgG = create_CDC_data(s3_client, bucket, curr_file, "IgG", CDC_data_IgG, file_date)
                            CDC_data_IgM = create_CDC_data(s3_client, bucket, curr_file, "IgM", CDC_data_IgM, file_date)
                    except Exception as e:
                        error_msg.append(str(e))
                        print(e)
                CDC_Data = pd.concat([CDC_data_IgM, CDC_data_IgG])

                CDC_Data["BSI_Parent_ID"] = [i[:7] + " 0001" for i in CDC_Data["Patient ID"].tolist()]
                master_data_dict = add_assay_to_dict(master_data_dict, "CDC_Data.csv", CDC_Data)
        if len(master_data_dict) > 0:
            valid_files = [i for i in sql_table_dict if "_sql.csv" not in i]
            valid_files = [i for i in valid_files if i in master_data_dict]
            filtered_tables = [value for key, value in sql_table_dict.items() if key in valid_files]
            tables_to_check = list(set([item for sublist in filtered_tables for item in sublist]))
            #print(master_data_dict['assay_data.csv']['Data_Table'].keys())
            # master_data_dict = {"submission.csv": master_data_dict["submission.csv"]}
            if "covid_history.csv" in master_data_dict:
                master_data_dict = check_decision_tree(master_data_dict)
            error_length = len(error_msg)
            add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, [])
            if len(error_msg) == error_length: #if the function does not generate new errors
                conn.connection.commit()
            else:
                print("roll back")
                conn.connection.rollback()

    except Exception as e:
        display_error_line(e)
        error_msg.append(str(e))
    return sql_table_dict, all_submissions


def create_CDC_data(s3_client, bucket, curr_file, sheet, df, file_date):
    obj = s3_client.get_object(Bucket=bucket, Key= curr_file["Key"])
    x = pd.read_excel(obj['Body'].read(), sheet_name=sheet, engine='openpyxl')
    #x = pd_s3.get_df_from_keys(s3_client, bucket, curr_file["Key"], suffix="xlsx",
                               #format="xlsx", na_filter=False, output_type="pandas", sheet_name=sheet)
    x["file_date"] = file_date
    x["Measurand_Antibody"] = sheet
    x["Patient ID"] = [i.replace(" 9002", "") for i in x["Patient ID"]]
    df = pd.concat([df, x])
    df.drop_duplicates(["Patient ID"], inplace=True, keep='last')
    return df


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def get_recent_date(files):
    curr_date = (files[0]["LastModified"]).replace(tzinfo=None)
    index = -1
    for file in files:
        index = index + 1
        if (file["LastModified"]).replace(tzinfo=None) > curr_date:
            curr_date = (file["LastModified"]).replace(tzinfo=None)
            curr_value = index
    return files[curr_value]["Key"]


def get_kwarg_parms(update_str, kwargs):
    if update_str in kwargs:
        Update_Table = kwargs[update_str]
    else:
        Update_Table = False
    return Update_Table


def check_decision_tree(master_data_dict):
    data_table = master_data_dict["covid_history.csv"]["Data_Table"]
    data_table.replace("No COVID Event Reported", "No COVID event reported", inplace=True)

    no_covid_data = data_table.query("COVID_Status in ['No COVID event reported', 'No COVID data collected']")
    pos_data = data_table[data_table["COVID_Status"].str.contains("Positive")]  # samples that contain at least 1 positive
    neg_data = data_table[data_table["COVID_Status"].str.contains("Negative")]
    neg_data = neg_data[~neg_data["COVID_Status"].str.contains("Positive")]     # samples that are all negative

    no_covid_data = set_col_vals(no_covid_data, "N/A", "N/A", "N/A", 'Not Reported', 'N/A')
    neg_data = set_col_vals(neg_data, "N/A", "N/A", "N/A", 0, 'N/A')

    pos_data["SARS-CoV-2_Variant"] = pos_data["SARS-CoV-2_Variant"].replace("Unavailable", "Unknown")
    pos_data["SARS-CoV-2_Variant"] = pos_data["SARS-CoV-2_Variant"].replace("N/A", "Unknown")

    pos_yes_sys = pos_data.query("Symptomatic_COVID == 'Yes'")
    pos_no_sys = pos_data.query("Symptomatic_COVID == 'No'")
    pos_ukn_sys = pos_data.query("Symptomatic_COVID == 'Unknown'")
    pos_no_sys = set_col_vals(pos_no_sys, True, True, "No", 1, 'N/A')       # value of true means keep same
    pos_ukn_sys = set_col_vals(pos_ukn_sys, True, True, "Unknown", True, 'No symptoms reported')     # value of true means keep same

    data_table_2 = pd.concat([pos_yes_sys, pos_no_sys, pos_ukn_sys, neg_data, no_covid_data])
    master_data_dict["covid_history.csv"]["Data_Table"] = data_table_2
    return master_data_dict


def set_col_vals(df_data, breakthrough, variant, covid, disease, symptoms):
    if breakthrough is not True:
        df_data["Breakthrough_COVID"] = breakthrough
    if variant is not True:
        df_data["SARS-CoV-2_Variant"] = variant
    if covid is not True:
        df_data["Symptomatic_COVID"] = covid
    if disease is not True:
        df_data["Disease_Severity"] = disease
    if symptoms is not True:
        df_data["Symptoms"] = symptoms
    return df_data


def get_all_submissions(s3_client, bucket_name, sub_folder, cbc_name, cbc_id, all_submissions, cbc_code):
    global error_msg
    """ scans the buceket name and provides a list of all files paths found """
    uni_submissions = []
    Prefix=sub_folder + "/" + cbc_name
    try:
        key_list = s3_client.list_objects(Bucket=bucket_name, Prefix=sub_folder + "/" + cbc_name)
        if 'Contents' in key_list:
            key_list = key_list["Contents"]
            key_list = [i["Key"] for i in key_list if ("UnZipped_Files" in i["Key"])]
            file_parts = [os.path.split(i)[0] for i in key_list]
            file_parts = [i for i in file_parts if "test/" not in i[0:5]]
            file_parts = [i for i in file_parts if "Submissions_in_Review" not in i]
            uni_submissions = list(set(file_parts))
        else:
            uni_submissions = []  # no submissions found for given cbc
    except Exception as e:
        error_msg.append(str(e))
        print("Erorr found")
    finally:
        cbc_code = cbc_code + [str(cbc_id)]*len(uni_submissions)
        return all_submissions + uni_submissions, cbc_code


def get_cbc_id(conn, cbc_name):
    cbc_table = pd.read_sql("Select * FROM CBC", conn)
    cbc_table = cbc_table.query("CBC_Name == @cbc_name")
    cbc_id = cbc_table["CBC_ID"].tolist()
    return cbc_id[0]


def get_sql_info(conn, sql_table, sql_column_df):
    col_names = sql_column_df.query("Table_Name==@sql_table")
    prim_key = col_names.query("Primary_Key == 'True' and Var_Type not in ['INTEGER', 'FLOAT']")
    sql_df = pd.read_sql(f"Select * FROM {sql_table}", conn)
    sql_df = sql_timestamp(sql_df, sql_column_df)
    primary_keys = prim_key["Column_Name"].tolist()
    return primary_keys, col_names, sql_df


def clean_tube_names(curr_table):
    curr_cols = curr_table.columns.tolist()
    curr_cols = [i.replace("Collection_Tube", "Tube") for i in curr_cols]
    curr_cols = [i.replace("Aliquot_Tube", "Tube") for i in curr_cols]
    # curr_cols = [i.replace("Tube_Type_Expiration_", "Tube_Lot_Expiration_") for i in curr_cols]
    curr_cols = [i.replace("Biospecimen_Company", "Biospecimen_Collection_Company") for i in curr_cols]
    curr_table.columns = curr_cols
    return curr_table


def get_upload_info(s3_client, bucket_name, curr_sub, sub_folder):
    # read the submission.csv and return info
    file_sep = os.path.sep
    if sub_folder in curr_sub[1][0]:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=curr_sub[1][0])
    else:
        sub_obj = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=sub_folder + file_sep + curr_sub[1][0])
    try:
        upload_date = sub_obj["Contents"][0]["LastModified"]
        upload_date = upload_date.replace(tzinfo=None)  # removes timezone element from aws
    except Exception:
        upload_date = 0
    submission_file = [i["Key"] for i in sub_obj["Contents"] if "submission.csv" in i["Key"]]
    submission_file = submission_file[0]

    obj = s3_client.get_object(Bucket=bucket_name, Key = submission_file)
    curr_table = pd.read_csv(obj['Body'])
    #curr_table = pd_s3.get_df_from_keys(s3_client, bucket_name, submission_file, suffix="csv", format="csv", na_filter=False, output_type="pandas")
    intent = curr_table.iloc[3][1]
    sub_name = curr_table.columns[1]
    return upload_date, intent, sub_name


def get_tables_to_load(s3_client, bucket, folders, curr_sub, conn, sub_name, index, upload_date, intent, master_dict, data_release):
    """Takes current submission and gets all csv files into pandas tables """
    files = [i["Key"] for i in folders if curr_sub[1][0] in i["Key"]]
    files = [i for i in files if ".csv" in i]
    data_dict = get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent, data_release)

    if len(data_dict) > 0:
        master_dict = combine_dictionaries(master_dict, data_dict)
    return master_dict


def get_data_dict(s3_client, bucket, files, conn, curr_sub, sub_name, index, upload_date, intent, data_release):
    data_dict = {}
    global error_msg
    for curr_file in files:
        split_path = os.path.split(curr_file)
        try:
            if "study_design" in split_path[1]:
                continue
            elif "submission" in split_path[1]:
                curr_table = populate_submission(conn, curr_sub, sub_name, index, upload_date, intent, data_dict)
            else:
                obj = s3_client.get_object(Bucket=bucket, Key = curr_file)
                curr_table = pd.read_csv(obj['Body'])
                #curr_table = pd_s3.get_df_from_keys(s3_client, bucket, split_path[0], suffix=split_path[1],
                                                    #format="csv", na_filter=False, output_type="pandas")
                if "Age" in curr_table.columns:
                    err_idx = curr_table.query("Research_Participant_ID in ['14_M95508'] and Age in ['93', 93] or " +
                                               "Research_Participant_ID in ['14_M80341'] and Age in ['96', 96]")
                    curr_table["Data_Release_Version"] = data_release
                    if len(err_idx) > 0:
                        curr_table = curr_table.drop(err_idx.index)
                curr_table["Submission_Index"] = str(index)
            curr_table = curr_table.loc[~(curr_table == '').all(axis=1)]
            curr_table.rename(columns={"Cohort": "CBC_Classification"})
        except Exception as e:
            error_msg.append(str(e))
            print(e)
        curr_table = clean_up_tables(curr_table)
        curr_table["Submission_CBC"] = curr_sub[1][1]
        if "secondary_confirmation" in split_path[1]:
            data_dict["secondary_confirmation_test_result.csv"] = {"Data_Table": []}
            data_dict["secondary_confirmation_test_result.csv"]["Data_Table"] = curr_table
        else:
            data_dict[split_path[1]] = {"Data_Table": []}
            #  curr_table.dropna(inplace=True)
            data_dict[split_path[1]]["Data_Table"] = curr_table
    return data_dict


def update_obesity_values(master_data_dict):
    global error_msg
    baseline = master_data_dict["baseline.csv"]["Data_Table"]
    try:
        baseline["BMI"] = baseline["BMI"].replace("Not Reported", -1e9)
        baseline["BMI"] = baseline["BMI"].replace("N/A", -1e9)
        baseline["BMI"] = [float(i) for i in baseline["BMI"]]

        baseline.loc[baseline.query("BMI < 0").index, "Obesity"] = "Not Reported"
        baseline.loc[baseline.query("BMI < 18.5 and BMI > 0").index, "Obesity"] = "Underweight"
        baseline.loc[baseline.query("BMI >= 18.5 and BMI <= 24.9").index, "Obesity"] = "Normal Weight"
        baseline.loc[baseline.query("BMI >= 25.0 and BMI <= 29.9").index, "Obesity"] = "Overweight"
        baseline.loc[baseline.query("BMI >= 30.0 and BMI <= 34.9").index, "Obesity"] = "Class 1 Obesity"
        baseline.loc[baseline.query("BMI >= 35.0 and BMI <= 39.9").index, "Obesity"] = "Class 2 Obesity"
        baseline.loc[baseline.query("BMI >= 40").index, "Obesity"] = "Class 3 Obesity"

        baseline["BMI"] = baseline["BMI"].replace(-1e9, np.nan)
    except Exception as e:
        error_msg.append(str(e))
        print(e)

    master_data_dict["baseline.csv"]["Data_Table"] = baseline
    return master_data_dict


def add_error_flags(curr_table, err_idx):
    if len(err_idx) > 0:
        curr_table.loc[err_idx.index, "Error_Flag"] = "Yes"
    return curr_table


def get_master_dict(master_data_dict, master_data_update, sql_column_df, sql_table_dict):
    global error_msg
    for key in master_data_dict.keys():
        try:
            table = sql_table_dict[key]
            primary_key = sql_column_df.query(f"Table_Name == {table} and Primary_Key == 'True'")["Column_Name"].tolist()
        except Exception:
            primary_key = "Biospecimen_ID"
        if key in ['shipping_manifest.csv']:
            primary_key = 'Current Label'
        if "Test_Result" in primary_key:
            primary_key[primary_key.index("Test_Result")] = 'SARS_CoV_2_PCR_Test_Result'

        if isinstance(primary_key, str):
            primary_key = [primary_key]
        if key in master_data_update.keys():
            try:
                x = pd.concat([master_data_dict[key]["Data_Table"], master_data_update[key]["Data_Table"]])
                x.reset_index(inplace=True, drop=True)
                x = correct_var_types(x, sql_column_df, table)
                if "Visit_Info_ID" in primary_key and "Visit_Info_ID" not in x.columns:
                    x, primary_key = add_visit_info(x, key, primary_key)
                if key not in ["submission.csv"]:
                    primary_key = [i for i in primary_key if i in x.columns]
                    primary_key = list(set(primary_key))
                    if len(primary_key) > 0:
                        x = x.drop_duplicates(primary_key, keep='last')
                master_data_dict[key]["Data_Table"] = x
            except Exception as e:
                error_msg.append(str(e))
                print(e)

    for key in master_data_update.keys():  # key only in update
        if key not in master_data_dict.keys():
            master_data_dict[key] = master_data_update[key]

    return master_data_dict


def add_visit_info(df, curr_file, primary_key):
    if curr_file == "baseline.csv":
        df['Type_Of_Visit'] = "Baseline"
        df['Visit_Number'] = "1"
        df['Unscheduled_Visit'] = "No"
    elif curr_file == "follow_up.csv":
        df['Type_Of_Visit'] = "Follow_up"
        base = df.query("Baseline_Visit == 'Yes'")
        if len(base) > 0:
            df.loc[base.index, "Type_Of_Visit"] = "Baseline"
    else:
        primary_key.append("Research_Participant_ID")
        primary_key.append("Cohort")
        primary_key.append("Visit_Number")
        return df, primary_key
    df["Visit_Info_ID"] = (df["Research_Participant_ID"] + " : " + [i[0] for i in df["Type_Of_Visit"]] +
                           ["%02d" % (int(i),) for i in df['Visit_Number']])
    return df, primary_key


def convert_data_type(v, var_type):
    if isinstance(v, datetime.datetime) and var_type.lower() == "datetime":
        return v
    if isinstance(v, datetime.date) and var_type.lower() == "date":
        return v
    if isinstance(v, datetime.time) and var_type.lower() == "time":
        return v

    if v == "Baseline(1)":
        v = 1

    if str(v).find('_') > 0:
        return v
    try:
        float(v)        # value is a number
        if (float(v) * 10) % 10 == 0 or (float(v) * 10) % 10 == 0.0:
            return int(float(v))
        else:
            return round(float(v), 5)
    except ValueError:
        try:
            if var_type.lower() == "datetime":
                return parse(v)
            elif var_type.lower() == "date":
                return parse(v).date()
        except ValueError:
            return v


def correct_var_types(data_table, sql_column_df, curr_table):
    col_names = data_table.columns
    for curr_col in col_names:
        if "Derived_Result" in data_table.columns:
            data_table = updated_derived(data_table)
        z = sql_column_df.query("Column_Name == @curr_col and Table_Name == @curr_table").drop_duplicates("Column_Name")
        if len(z) > 0:
            var_type = z.iloc[0]["Var_Type"]
        else:
            var_type = "VARCHAR(255)"
        if var_type in ['INTEGER', 'FLOAT', 'DOUBLE']:
            data_table[curr_col].replace("N/A", np.nan, inplace=True)
        if curr_col in ["Age", "Storage_Time_in_Mr_Frosty", "Biospecimen_Collection_to_Test_Duration", "BMI"]:
            data_table = round_data(data_table, curr_col)
        elif curr_col in ["Sample_Dilution"] and "Subaliquot_ID" in data_table.columns:  # special formating for reference panel testing
            data_table[curr_col].replace("none", "1", inplace=True)   #no dilution is same as 1:1 dilution
            data_table[curr_col] = [str(f"{Decimal(i):.2E}") for i in data_table[curr_col]]
        elif "varchar" in var_type.lower():
            data_table[curr_col] = [str(i) for i in data_table[curr_col]]
        else:
            data_table[curr_col] = [convert_data_type(c, var_type) for c in data_table[curr_col]]
    return data_table


def round_data(data_table, test_col):
    for x in data_table.index:
        try:
            if data_table.loc[x, test_col] == "90+":
                data_table.loc[x, test_col] = 90
            curr_data = round(float(data_table.loc[x, test_col]), 1)
            if (curr_data * 10) % 10 == 0.0:
                curr_data = int(curr_data)
            data_table.loc[x, test_col] = curr_data
        except Exception:
            data_table.loc[x, test_col] = str(data_table.loc[x, test_col])
    return data_table


def clean_up_tables(curr_table):
    if "Submission_Index" in curr_table.columns:
        x = curr_table.drop("Submission_Index", axis=1)
        x.replace("", float("NaN"), inplace=True)
        #x.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
        x.dropna(axis=0, how="all", subset=None, inplace=True)
        z = curr_table["Submission_Index"].to_frame()
        curr_table = x.merge(z, left_index=True, right_index=True)
    else:
        curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]
        for iterC in curr_table.columns:
            try:
                curr_table[iterC] = curr_table[iterC].apply(lambda x: x.replace('–', '-'))
            except Exception:
                pass
    if "Comments" in curr_table.columns:
        curr_table = curr_table.query("Comments not in ['Invalid data entry; do not include']")
    return curr_table


def sql_timestamp(sql_df, sql_column_df):
    new_list = sql_column_df.query("Var_Type == 'TIME'")
    for i in new_list["Column_Name"].tolist():
        if i in sql_df.columns.tolist():
            curr_col = sql_df[i]
            for iterC in curr_col.index:
                if (sql_df[i][iterC] == sql_df[i][iterC]) and (isinstance(sql_df[i][iterC], pd.Timedelta)):
                    hours = int((sql_df[i][iterC].seconds/60)/60)
                    minutes = int((sql_df[i][iterC].seconds % 3600)/60)
                    seconds = sql_df[i][iterC].seconds - (hours*3600 + minutes*60)
                    sql_df[i][iterC] = datetime.time(hours, minutes, seconds)
    return sql_df


def populate_submission(conn, curr_sub, sub_name, index, upload_date, intent, data_dict):
    x_name = pathlib.PurePath(curr_sub[1][0])
    part_list = x_name.parts
    try:
        curr_time = datetime.datetime.strptime(part_list[2], "%H-%M-%S-%m-%d-%Y")
    except Exception:  # time stamp was corrected
        curr_time = datetime.datetime.strptime(part_list[2], "%Y-%m-%d-%H-%M-%S")
    cbc_id = get_cbc_id(conn, sub_name)
    file_name = re.sub("submission_[0-9]{3}_", "", part_list[3])
    sql_df = pd.DataFrame([[index, cbc_id, curr_time, sub_name, file_name, curr_sub[1][0], upload_date, intent]],
                          columns=["Submission_Index", "Submission_CBC_ID", "Submission_Time",
                                   "Submission_CBC_Name", "Submission_File_Name", "Submission_S3_Path",
                                   "Date_Submission_Validated", "Submission_Intent"])
    return sql_df


def add_submission_data(data_dict, conn, csv_sheet):
    sub_data = data_dict["submission.csv"]["Data_Table"]
    curr_table = data_dict[csv_sheet]["Data_Table"]
    curr_table["Submission_CBC"] = get_cbc_id(conn, sub_data.columns[1])
    return curr_table


def combine_dictionaries(master_data_dict, data_dict):
    global error_msg
    for curr_file in data_dict:
        try:
            if curr_file not in master_data_dict:
                master_data_dict[curr_file] = {"Data_Table": data_dict[curr_file]["Data_Table"]}
            else:
                x = pd.concat([master_data_dict[curr_file]["Data_Table"], data_dict[curr_file]["Data_Table"]],
                              axis=0, ignore_index=True).reset_index(drop=True)
                master_data_dict[curr_file]["Data_Table"] = x
        except Exception as e:
            error_msg.append(str(e))
            print(e)
    return master_data_dict


def fix_aliquot_ids(master_data_dict, keep_order, sql_column_df):
    if "aliquot.csv" in master_data_dict:
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"].tolist()
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_ID"] = zero_pad_ids(z)
        z = master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"].tolist()
        z = [(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["aliquot.csv"]["Data_Table"]["Aliquot_Volume"] = z
        z = master_data_dict["aliquot.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        z.drop_duplicates("Aliquot_ID", keep=keep_order, inplace=True)
        master_data_dict["aliquot.csv"]["Data_Table"] = correct_var_types(z, sql_column_df, "Aliquot")
    if "shipping_manifest.csv" in master_data_dict:
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]
        z['Current Label'] = z['Current Label'].fillna(value="0")
        master_data_dict["shipping_manifest.csv"]["Data_Table"] = z.query("`Current Label` not in ['0']")

        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"].tolist()
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Current Label"] = zero_pad_ids(z)
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"].tolist()
        z = [(i.replace("N/A", "0")) if isinstance(i, str) else i for i in z]
        master_data_dict["shipping_manifest.csv"]["Data_Table"]["Volume"] = z
        z = master_data_dict["shipping_manifest.csv"]["Data_Table"]
        z.sort_values("Submission_Index", axis=0, ascending=True, inplace=True)
        z.drop_duplicates("Current Label", keep=keep_order, inplace=True)
        master_data_dict["shipping_manifest.csv"]["Data_Table"] = correct_var_types(z, sql_column_df, "Shipping_Manifest")
    return master_data_dict


def zero_pad_ids(data_list):
    global error_msg
    try:
        digits = [len(i[14:16]) for i in data_list]
        for i in enumerate(digits):
            if i[1] == 1:
                if data_list[i[0]][14:15].isdigit():
                    data_list[i[0]] = data_list[i[0]][:15]
            elif i[1] == 2:
                if data_list[i[0]][14:16].isdigit():
                    data_list[i[0]] = data_list[i[0]][:16]
                elif data_list[i[0]][14:15].isdigit():
                    data_list[i[0]] = data_list[i[0]][:15]

        data_list = [i[0:13] + "_0" + i[14] if i[-2] == '_' else i for i in data_list]
    except Exception as e:
        error_msg.append(str(e))
        print(e)
    finally:
        return data_list


def upload_assay_data(data_dict, bucket_name, s3_client):
    global success_msg
    #  populate the assay data tables which are independant from submissions
    #assay_data, assay_target, all_qc_data, converion_file = get_box_data_v2.get_assay_data("CBC_Data")
    assay_data, assay_target, all_qc_data, converion_file = get_assay_data(s3_client, "CBC_Data", bucket_name)
    if len(assay_data) > 0 and len(all_qc_data) > 0 and len(assay_target) > 0 and len(converion_file) > 0:
        assay_data["Calibration_Type"] = assay_data["Calibration_Type"].replace("", "No Data Provided")
        assay_data["Calibration_Type"] = assay_data["Calibration_Type"].replace("N/A", "No Data Provided")
        data_dict = add_assay_to_dict(data_dict, "assay_data.csv", assay_data)
        all_qc_data.replace("", "No Data Provided", inplace=True)
        all_qc_data.replace(np.nan, "No Data Provided", inplace=True)
        all_qc_data["Comments"] = all_qc_data["Comments"].replace("No Data Provided", "")
        data_dict = add_assay_to_dict(data_dict, "assay_qc.csv", all_qc_data)
        data_dict = add_assay_to_dict(data_dict, "assay_target.csv", assay_target)
        data_dict = add_assay_to_dict(data_dict, "assay_conversion.csv", converion_file)
        print("## The assay data has been uploaded")
        success_msg.append("## The assay data has been uploaded")


    # validation_panel_assays = get_box_data.get_assay_data("Validation")
    # data_dict = add_assay_to_dict(data_dict, "validation_assay_data.csv", validation_panel_assays)
    return data_dict


def add_assay_to_dict(data_dict, csv_file, data_table):
    data_table.rename(columns={"Target_Organism": "Assay_Target_Organism"}, inplace=True)
    data_dict[csv_file] = {"Data_Table": []}
    data_dict[csv_file]["Data_Table"] = data_table
    return data_dict


def add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, done_tables):
    global error_msg
    global success_msg
    not_done = []
    key_count = pd.crosstab(sql_column_df["Table_Name"], sql_column_df["Foreign_Key_Count"])
    key_count.reset_index(inplace=True)
    key_count = key_count.query("Table_Name not in @done_tables")
    if len(key_count) == 0:
        return
    error_msg_len = len(error_msg)
    for curr_table in key_count["Table_Name"].tolist():
        if curr_table in tables_to_check:
            check_foreign = key_count.query("Table_Name == @curr_table and 1.0 > 0")
            if len(check_foreign) > 0:
                foreign_table = sql_column_df.query("Table_Name == @curr_table and Foreign_Key_Table not in ['', 'None']")["Foreign_Key_Table"].tolist()
                check = all(item in done_tables for item in foreign_table)
                if check is False:
                    not_done.append(curr_table)
                    continue    # dependent tables not checked yet

        done_tables.append(curr_table)
        if curr_table not in tables_to_check:
            continue
        y = sql_column_df.query("Table_Name == @curr_table")
        y = y.query("Autoincrement != True")
        sql_df = pd.read_sql((f"Select * from {curr_table}"), conn)
        sql_df = sql_df[y["Column_Name"].tolist()]
        # sql_df = sql_df.astype(str)
        sql_df.replace("No Data", "NULL", inplace=True)  # if no data in sql this is wrong, used to correct
        sql_df.fillna("No Data", inplace=True)  # replace NULL values from sql with "No Data" for merge purposes

        num_cols = sql_column_df.query("Var_Type in ('INTEGER', 'INTERGER', 'FLOAT', 'DOUBLE')")["Column_Name"]
        num_cols = list(set(num_cols))
        char_cols = sql_column_df[sql_column_df['Var_Type'].str.contains("CHAR")]["Column_Name"]
        char_cols = list(set(char_cols))

        csv_file = [key for key, value in sql_table_dict.items() if curr_table in value]
        output_file = pd.DataFrame(columns=y["Column_Name"].tolist())
        processing_file = []
        processing_data = []
        if len(csv_file) == 0:
            continue
        else:
            for curr_file in csv_file:
                if curr_file in master_data_dict:
                    x = copy.copy(master_data_dict[curr_file]["Data_Table"])
                    if "PCR_Test_Date_Duration_From_Index" in x.columns:
                        z = x[["PCR_Test_Date_Duration_From_Index", "Rapid_Antigen_Test_Date_Duration_From_Index", "Antibody_Test_Date_Duration_From_Index"]]
                        z = correct_var_types(z, sql_column_df, curr_table)
                        z = z.mean(axis=1, numeric_only=True)
                        x['Average_Duration_Of_Test'] = z.fillna("NAN")
                else:
                    continue
                x.rename(columns={"Comments": curr_table + "_Comments"}, inplace=True)
                if curr_table == "Tube":
                    x = clean_tube_names(x)
                    if curr_file == "aliquot.csv":
                        x["Tube_Used_For"] = "Aliquot"
                    elif curr_file == "biospecimen.csv":
                        x["Tube_Used_For"] = "Biospecimen_Collection"
                if curr_table == "Assay_Bio_Target":
                    x = get_bio_target(x, conn)
                try:
                    x.replace({np.nan: 'N/A', 'nan': "N/A", '': "N/A", True: '1', False: '0'}, inplace=True)
                    x = x.astype(str)
                    if curr_file in ["equipment.csv", "consumable.csv", "reagent.csv"]:
                        processing_data = copy.copy(x)
                    x = get_col_names(x, y, conn, curr_table, curr_file, sql_column_df)
                    if len(x) == 0:
                        continue
                    x.drop_duplicates(inplace=True)
                except Exception as e:
                    error_msg.append(str(e))
                    print(e)
                output_file = pd.concat([output_file, x])
                if len(processing_data) > 0 and len(processing_file) > 0:
                    processing_file = pd.concat([processing_file, processing_data])
                elif len(processing_data) > 0:
                     processing_file = processing_data

            if len(output_file) > 0:
                output_file = correct_var_types(output_file, sql_column_df, curr_table)
                sql_df = fix_num_cols(sql_df, num_cols, "sql")
                output_file = fix_num_cols(output_file, num_cols, "file")
                output_file = fix_char_cols(output_file, char_cols)
                output_file.drop_duplicates(inplace=True)

                output_file.reset_index(inplace=True, drop=True)
                output_file = fix_aliquot_ids(output_file, "first", sql_column_df)
                output_file.replace("N/A", "No Data", inplace=True)

                comment_col = [i for i in output_file.columns if "Comments" in i]
                if len(comment_col) > 0:
                    output_file[comment_col[0]] = output_file[comment_col[0]].replace("No Data", "")
                    sql_df[comment_col[0]] = sql_df[comment_col[0]].replace("No Data", "")

                if curr_table not in ["Tube", "Aliquot", "Consumable", "Reagent", "Equipment"]:
                    col_list = [i for i in sql_df.columns if i not in ["Derived_Result", "Raw_Result", "BMI"]]
                    sql_df[col_list] = sql_df[col_list].replace("\.0", "", regex=True)

                    col_list = [i for i in output_file.columns if i not in ["Derived_Result", "Raw_Result", "BMI", 'Sample_Dilution']]
                    output_file[col_list] = output_file[col_list].replace("\.0", "", regex=True)

                number_col = sql_column_df.query("Table_Name == @curr_table and Var_Type in ['FLOAT', 'INTEGER', 'DOUBLE']")["Column_Name"].tolist()
                for n_col in number_col:
                    if n_col in output_file.columns:
                        output_file[n_col].replace("", -1e9, inplace=True)
                        output_file[n_col].replace("N/A", -1e9, inplace=True)
                for cell_col in sql_df:
                    if "Hemocytometer_Count" in cell_col or "Automated_Count" in cell_col:
                        sql_df[cell_col].replace("No Data", -1e9, inplace=True)

                try:
                    primary_keys = sql_column_df.query("Table_Name == @curr_table and Primary_Key == 'True' and Autoincrement != True")
                    if len(primary_keys) > 0:
                        primary_keys = primary_keys["Column_Name"].tolist()
                        for i in primary_keys:
                            output_file[i] = output_file[i].replace("No Data", "")   # primary keys cant be null

                    sql_df.replace("", "No Data", inplace=True)
                    output_file.replace({"": "No Data"}, inplace=True)
                    sql_df = sql_df.replace(np.nan, -1e9)
                    output_file = output_file.replace(np.nan, -1e9)
                    if "Sunday_Prior_To_First_Visit" in output_file.columns:
                        output_file["Sunday_Prior_To_First_Visit"] = output_file["Sunday_Prior_To_First_Visit"].replace(-1e9, datetime.date(2000,1,1))
                        sql_df["Sunday_Prior_To_First_Visit"] = sql_df["Sunday_Prior_To_First_Visit"].replace("No Data", datetime.date(2000,1,1))

                    try:
                        z = output_file.merge(sql_df, how="outer", indicator=True)
                    except Exception:
                        sql_df["Sample_Dilution"] = sql_df["Sample_Dilution"].to_string()
                        z = output_file.merge(sql_df, how="outer", indicator=True)
                    finally:
                        new_data = z.query("_merge == 'left_only'")       # new or update data
                except Exception as e:
                    error_msg.append(str(e))
                    print(e)
                    new_data = []

                if len(new_data) > 0:
                    new_data.drop("_merge", inplace=True, axis=1)
                    x = sql_column_df.query("Var_Type in ['INTEGER', 'FLOAT']")
                    x = x.query("Column_Name in @new_data.columns")
                    update_data = []

                    try:
                        merge_data = new_data.merge(sql_df[primary_keys], how="left", indicator=True)
                        merge_data.replace({-1e9: np.nan}, inplace=True)
                        new_data = merge_data.query("_merge == 'left_only'")
                        update_data = merge_data.query("_merge == 'both'")
                    except Exception as e:
                        error_msg.append(str(e))
                        print(e)

                    if len(new_data) > 0:
                        if "_merge" in new_data.columns:
                            new_data.drop("_merge", inplace=True, axis=1)
                        try:
                            if len(new_data) > 0:
                                print(f"## Adding New Rows to table: {curr_table} ##")
                                success_msg.append(f"## Adding New Rows to table: {curr_table} ##")
                                new_data.replace("No Data", np.nan, inplace=True)
                                new_data.replace(datetime.date(2000,1,1), np.nan, inplace=True)
                                if "Current Label" in new_data.columns:
                                    new_data = new_data.query("`Current Label` not in ['32_441013_102_02', '32_441006_311_01', '32_441131_101_02', " +
                                                              "'32_441040_102_02', '32_441040_102_01', '32_441057_102_02', '32_441057_102_01', '32_441047_102_02', " +
                                                              "'32_441047_102_01', '32_441040_101_02', '32_441040_101_01', '32_441041_101_01', '32_441041_101_02', " +
                                                              "'32_441047_101_01', '32_441047_101_02', '32_441057_101_01','32_441057_101_02']")

                                    new_data = new_data.query("`Current Label` not in ['32_441083_311_01','32_441153_311_01','32_441112_304_01','32_441095_305_01','32_441108_301_01', " +
                                                              "'32_441139_305_01','32_441013_311_01','32_441146_304_01','32_441165_304_01','32_441131_101_01']")

                                #new_data.to_sql(name=curr_table, con=engine, if_exists="append", index=False)
                                #Replace to_sql because to_sql have auto_commit
                                col_list = new_data.columns.tolist()
                                col_string = ''
                                for i in range(0, len(col_list)):
                                    if i == 0:
                                        col_string = '(' + '`' + str(col_list[i]) + '`'
                                    elif i == len(col_list) - 1:
                                        col_string = col_string + ', ' + '`' + str(col_list[i]) + '`' + ')'
                                    else:
                                        col_string = col_string + ', ' + '`' + str(col_list[i]) + '`'
                                for index in new_data.index:
                                    curr_data = new_data.loc[index, col_list].values.tolist()
                                    curr_data = curr_data
                                    for i in range(0, len(curr_data)):
                                        if pd.isna(curr_data[i]):
                                            curr_data[i] = 'NULL'
                                    curr_data_tuple = tuple(curr_data)
                                    sql_query = (f"INSERT INTO {curr_table} {col_string} VALUES {curr_data_tuple}")
                                    conn.execute(sql_query)
                                #conn.connection.commit()
                        except Exception as e:
                            #print(e)
                            error_msg.append(str(e))
                            print("error loading table")
                    if len(update_data) > 0:
                        try:
                            row_count = len(update_data)
                            print(f"\n## Updating {row_count} Rows in table: {curr_table} ##\n")
                            success_msg.append(f"## Updating {row_count} Rows in table: {curr_table} ##")
                            update_tables(conn, engine, primary_keys, update_data, curr_table)
                        except:
                            print(e)
                            error_msg.append(str(e))

                else:
                    print(f" \n## {curr_table} has been checked, no data to add")
                    success_msg.append(f"## {curr_table} has been checked, no data to add")
            else:
                print(f" \n## {curr_table} was not found in submission, nothing to add")
            if len(processing_file) > 0 and "Biospecimen" in done_tables:
                prim_table = pd.read_sql((f"Select * from {curr_table}"), conn)
                prim_table.fillna("No Data", inplace=True)  # replace NULL values from sql with "No Data" for merge purposes
                processing_file.replace("N/A", "No Data", inplace=True)
                prim_table = prim_table[[i for i in prim_table.columns if "Comments" not in i]]
                df_obj = processing_file.select_dtypes(['object'])
                processing_file[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
                processing_file = correct_var_types(processing_file, sql_column_df, curr_table)
                processing_data = processing_data.merge(prim_table, indicator=True, how="left")
                test_table = processing_data[["Biospecimen_ID", curr_table + "_Index"]]

                sql_table = pd.read_sql((f"SELECT Biospecimen_ID, {curr_table}_Index FROM Biospecimen_{curr_table}"), conn)
                test_table = test_table.merge(sql_table, how="left", indicator=True)
                test_table = test_table.query("_merge == 'left_only'").drop("_merge", axis=1)
                test_table.drop_duplicates(inplace=True)
                #test_table.to_sql(name="Biospecimen_" + curr_table, con=engine, if_exists="append", index=False)
                col_list = test_table.columns.tolist()
                col_string = ''
                test_table = test_table.replace(np.nan, None)
                for i in range(0, len(col_list)):
                    if i == 0:
                        col_string = '(' + str(col_list[i])
                    elif i == len(col_list) - 1:
                        col_string = col_string + ', ' + str(col_list[i]) + ')'
                    else:
                        col_string = col_string + ', ' + str(col_list[i])
                test_table_name = "Biospecimen_" + curr_table
                for index in test_table.index:
                    curr_data = test_table.loc[index, col_list].values.tolist()
                    curr_data_tuple = tuple(curr_data)
                    sql_query = (f"INSERT INTO {test_table_name} {col_string} VALUES {curr_data_tuple}")
                    conn.execute(sql_query)
                #conn.connection.commit()
    if len(not_done) > 0:
        add_tables_to_database(engine, conn, sql_table_dict, sql_column_df, master_data_dict, tables_to_check, done_tables)

def fix_num_cols(df, num_cols, data_type):
    for col_name in df.columns:
        if col_name in num_cols:
            if data_type == "file":
                # df[col_name] = [str(i) for i in df[col_name]]
                df[col_name] = df[col_name].replace("N/A", np.nan)
                df[col_name] = df[col_name].replace("Not Reported", np.nan)
                df[col_name] = df[col_name].replace(-1000000000, np.nan)
                df[col_name] = df[col_name].replace(-1e+09, np.nan)

            df[col_name] = df[col_name].replace("No Data", np.nan)
            df[col_name] = df[col_name].replace("nan", np.nan)
    return df


def fix_char_cols(df, char_cols):
    for col_name in df.columns:
        if col_name in char_cols:
            df[col_name] = [str(i).strip() for i in df[col_name]]  # remove trail white space
            df[col_name] = [str(i).replace("'", "") for i in df[col_name]]  # remove trail white space
            df[col_name] = [str(i).replace("", "No Data") if len(i) == 0 else i for i in df[col_name]]  # fill blank cells with no data
    return df


def get_col_names(x, y, conn, curr_table, curr_file, sql_column_df):
    global error_msg
    col_list = y["Column_Name"].tolist()
    if "Normalized_Cohort" in col_list:   # this will be removed once tempaltes updated
        col_list.remove("Normalized_Cohort")
    if "Visit_Info_Comments" in col_list:   # this will be removed once tempaltes updated
        col_list.remove("Visit_Info_Comments")
    if "Data_Release_Version" in col_list and curr_file not in ["baseline.csv"]:
        col_list.remove("Data_Release_Version")

    if curr_table == "Participant":
        if "Sunday_Prior_To_First_Visit" not in x.columns:
            x["Sunday_Prior_To_First_Visit"] =  datetime.date(2000,1,1)

    if curr_table == "Participant_Visit_Info":
        if "Primary_Study_Cohort" not in x.columns:
            x["Primary_Study_Cohort"] = "None"
        if "CBC_Classification" not in x.columns:
            x.rename(columns={"Cohort": "CBC_Classification"}, inplace=True)
        x, primary_key = add_visit_info(x, curr_file, [])
    else:
        if "Visit_Info_ID" in col_list:
            visit_data = pd.read_sql(("SELECT Visit_Info_ID, Research_Participant_ID, Visit_Number FROM Participant_Visit_Info;"), conn)
            visit_data["Visit_Number"] = [int(i) for i in visit_data["Visit_Number"]]
            if curr_file == "baseline.csv":
                x["Visit_Number"] = 1
            x.replace("Baseline(1)", 1, inplace=True)
            try:
                x["Visit_Number"] = [int(i) for i in x["Visit_Number"]]
                x = x.merge(visit_data)
            except Exception as e:
                error_msg.append(str(e))
                print(e)
    if "Biospecimen_" in curr_table and "Test_Results" not in curr_table:
        table_name = curr_table.replace("Biospecimen_", "")
        table_data = pd.read_sql((f"SELECT * FROM {table_name}"), conn)
        x = x.merge(table_data)

    if curr_table in ["Biospecimen", "Aliquot"]:
        tube_data = pd.read_sql(("SELECT * FROM Tube;"), conn)
        tube_data.fillna("N/A", inplace=True)
        if curr_table == "Biospecimen":
            tube_data.columns = [i.replace("Tube", "Collection_Tube") for i in tube_data.columns]
            x["Collection_Tube_Type_Expiration_Date"] = [i if i == "N/A" else parse(i).date()
                                                         for i in x["Collection_Tube_Type_Expiration_Date"].tolist()]
        elif curr_table == "Aliquot":
            tube_data.columns = [i.replace("Tube", "Aliquot_Tube") for i in tube_data.columns]
            x["Aliquot_Tube_Type_Expiration_Date"] = [i if i == "N/A" else parse(i).date() for i in x["Aliquot_Tube_Type_Expiration_Date"].tolist()]
        try:
            tube_data.replace("N/A", "No Data", inplace=True)
            x.replace("N/A", "No Data", inplace=True)
            for curr_col in x.columns:
                try:
                    x[curr_col] = x[curr_col].str.strip()
                except AttributeError:
                    pass  # not a character col
            x = x.merge(tube_data, how="left", indicator=True)
        except Exception as e:
            error_msg.append(str(e))
            print(e)

        if curr_table == "Biospecimen":
            x.rename(columns={"Collection_Tube_ID": "Biospecimen_Tube_ID"}, inplace=True)
        else:
            x.rename(columns={"Collection_Tube_ID": "Aliquot_Tube_ID"}, inplace=True)
    try:
        if "Submission_CBC" in col_list and "Submission_CBC" not in x.columns:
            if "Research_Participant_ID" in x.columns:
                x['Submission_CBC'] = [str(i[:2]) for i in x["Research_Participant_ID"]]
            else:
                x['Submission_CBC'] = [str(i[:2]) for i in x["Biospecimen_ID"]]
        x = x[col_list]
    except Exception as e:
        error_msg.append(str(e))
        display_error_line(e)
        return []
    return x


def updated_derived(x):
    x["Derived_Result"] = [str(i).replace("<", "") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace(">", "") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace("Nonreactive", "-1e9") for i in x["Derived_Result"]]
    x["Derived_Result"] = [str(i).replace("Reactive", "-1e9") for i in x["Derived_Result"]]
    return x

def update_secondary_confirm(master_data_dict, sql_column_df):
    x = master_data_dict["secondary_confirmation_test_result.csv"]["Data_Table"]
    x = correct_var_types(x, sql_column_df, "Secondary_Confirmatory_Test")

    if "Subaliquot_ID" in x.columns:
        x["BSI_Parent_ID"] = [i[:7] + " 0001" for i in x["Subaliquot_ID"].tolist()]
    x.rename(columns={"Comments": "Confirmatory_Clinical_Test_Comments"}, inplace=True)
    master_data_dict["secondary_confirmation_test_result.csv"]["Data_Table"] = x
    return master_data_dict


def get_bio_target(curr_table, conn):
    global error_msg
    curr_table.drop_duplicates(inplace=True)
    curr_cols = curr_table.columns.tolist()
    curr_cols = [i.replace("Target_biospecimen_is_", "") for i in curr_cols]
    curr_cols = [i.replace("/", "_") for i in curr_cols]
    curr_table.columns = curr_cols

    bio_type = pd.read_sql("Select * FROM Biospecimen_Type", conn)
    new_col_names = [i for i in bio_type["Biospecimen_Type"].tolist() if i in curr_table.columns]
    bio_table = curr_table[new_col_names].stack().to_frame()
    bio_table.reset_index(inplace=True)
    bio_table.columns = ["Assay_ID", "Target_Biospecimen_Type", "Is_Present"]
    try:
        bio_table["Assay_ID"] = curr_table["Assay_ID"].repeat(len(new_col_names)).tolist()
        bio_table = bio_table.query("Is_Present == 'Yes'")
        curr_table = curr_table.merge(bio_table)
    except Exception as e:
        error_msg.append(str(e))
        print(e)
    return curr_table


def get_bsi_files(s3_client, bucket, sub_folder, master_data_dict):
    if sub_folder == "Reference Pannel Submissions":
        curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_Reference_Panel_map.xlsx"
    elif sub_folder == "Vaccine Response Submissions":
        curr_file = "Serology_Data_Files/biorepository_id_map/Biorepository_ID_Vaccine_Response_map.xlsx"
    else:
        return master_data_dict

    print(" getting bsi parent data ")
    obj = s3_client.get_object(Bucket=bucket, Key= curr_file)
    parent_data = pd.read_excel(obj['Body'].read(), sheet_name="BSI_Parent_Aliquots", engine='openpyxl', na_filter=False,)
    #parent_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", format="xlsx", na_filter=False,
                                         #output_type="pandas", sheet_name="BSI_Parent_Aliquots")
    print(" gettting bsi child data ")
    obj_child = s3_client.get_object(Bucket=bucket, Key= curr_file)
    child_data = pd.read_excel(obj_child['Body'].read(), sheet_name="BSI_Child_Aliquots", engine='openpyxl', na_filter=False,)
    #child_data = pd_s3.get_df_from_keys(s3_client, bucket, curr_file, suffix="xlsx", format="xlsx", na_filter=False,
                                        #output_type="pandas", sheet_name="BSI_Child_Aliquots")

    master_data_dict["bsi_parent.csv"] = {"Data_Table": parent_data}
    master_data_dict["bsi_child.csv"] = {"Data_Table": child_data}
    return master_data_dict


def update_tables(conn, engine, primary_keys, update_table, sql_table):
    global error_msg
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)
    if "Sunday_Prior_To_First_Visit" in update_table:
        update_table = update_table[["Research_Participant_ID", "Age", "Sunday_Prior_To_First_Visit"]]
    else:
        update_table.drop("_merge", inplace=True, axis=1)

    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    if "Vaccination_Record" in update_table.columns:
        col_list.append("Vaccination_Record")

    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            curr_data = [str(i).replace("'", "") for i in curr_data]
            curr_data = [i.replace('', "NULL") if len(i) == 0 else i for i in curr_data]

            primary_value = update_table.loc[index, primary_keys].values.tolist()
            primary_value = [str(i).replace(".0", "") for i in primary_value]
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace("'nan'", "NULL")
            update_str = update_str.replace("'NULL'", "NULL")
            update_str = update_str.replace("'No Data'", "NULL")
            update_str = update_str.replace("'2000-01-01'", "NULL")
            update_str = update_str.replace("`Data_Release_Version` = NULL", "`Data_Release_Version` = '1.0.0'") # outdated, replace later
            update_str = update_str.replace("`Data_Release_Version` = '2'", "`Data_Release_Version` = '2.0.0'")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            #engine.execute(sql_query)
            conn.execute(sql_query)
        except Exception as e:
            error_msg.append(str(e))
            print(e)
        #finally:
            #conn.connection.commit()


def get_sql_dict_ref(s3_client, bucket):
    sql_table_dict = {}
    obj = s3_client.get_object(Bucket=bucket, Key= "SQL_Dict/sql_dict_ref.yaml")
    sql_table_dict = yaml.safe_load(obj['Body'])
    '''
    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]
    sql_table_dict["validation_assay_data.csv"] = ["Validation_Panel_Assays"]

    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]

    sql_table_dict["confirmatory_clinical_test.csv"] = ["Confirmatory_Clinical_Test"]
    sql_table_dict["demographic.csv"] = ["Participant"]  # , "Prior_Covid_Outcome", "Participant_Comorbidity_Reported", "Comorbidity"]
    sql_table_dict["prior_clinical_test.csv"] = ["Participant_Prior_SARS_CoV2_PCR"]  # , "Participant_Prior_Infection_Reported"]

    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]

    sql_table_dict["secondary_confirmation_test_result.csv"] = ["Secondary_Confirmatory_Test"]
    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    sql_table_dict["submission.csv"] = ["Submission"]
    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]
    sql_table_dict["CDC_Data.csv"] = ["CDC_Confrimation_Results"]

    sql_table_dict["Blinded_Evaluation_Panels.csv"] = ["Blinded_Validation_Test_Results"]
    '''
    return sql_table_dict


def get_sql_dict_vacc(s3_client, bucket):
    sql_table_dict = {}
    sql_table_dict = {}
    obj = s3_client.get_object(Bucket=bucket, Key= "SQL_Dict/sql_dict_vacc.yaml")
    sql_table_dict = yaml.safe_load(obj['Body'])

    '''
    visit_list = ["Participant_Visit_Info", "Participant", "Specimens_Collected", "Participant_Comorbidities", "Comorbidities_Names",
                  "Participant_Other_Conditions", "Participant_Other_Condition_Names", "Drugs_And_Alcohol_Use", "Non_SARS_Covid_2_Vaccination_Status"]

    sql_table_dict["baseline.csv"] = visit_list
    sql_table_dict["follow_up.csv"] = [i for i in visit_list if i != "Participant"]  # Participant table does not exist in follow up

    sql_table_dict["aliquot.csv"] = ["Tube", "Aliquot"]
    sql_table_dict["biospecimen.csv"] = ["Tube", "Biospecimen"]

    sql_table_dict["assay_data.csv"] = ["Assay_Metadata", "Assay_Calibration", "Assay_Bio_Target"]
    sql_table_dict["assay_target.csv"] = ["Assay_Target"]
    sql_table_dict["assay_qc.csv"] = ["Assay_Quality_Controls"]
    sql_table_dict["assay_conversion.csv"] = ["Assay_Organism_Conversion"]

    sql_table_dict["study_design.csv"] = ["Study_Design"]

    sql_table_dict["consumable.csv"] = ["Consumable", "Biospecimen_Consumable"]
    sql_table_dict["reagent.csv"] = ["Reagent", "Biospecimen_Reagent"]
    sql_table_dict["equipment.csv"] = ["Equipment", "Biospecimen_Equipment"]

    sql_table_dict["shipping_manifest.csv"] = ["Shipping_Manifest"]

    sql_table_dict["covid_history.csv"] = ["Covid_History"]
    sql_table_dict["covid_hist_sql.csv"] = ["Covid_History"]

    sql_table_dict["covid_vaccination_status.csv"] = ["Covid_Vaccination_Status"]
    sql_table_dict["vac_status_sql.csv"] = ["Covid_Vaccination_Status"]

    sql_table_dict["biospecimen_test_result.csv"] = ["Biospecimen_Test_Results"]
    sql_table_dict["test_results_sql.csv"] = ["Biospecimen_Test_Results"]

    sql_table_dict["treatment_history.csv"] = ["Treatment_History"]

    sql_table_dict["autoimmune_cohort.csv"] = ["AutoImmune_Cohort"]
    sql_table_dict["cancer_cohort.csv"] = ["Cancer_Cohort"]
    sql_table_dict["hiv_cohort.csv"] = ["HIV_Cohort"]
    sql_table_dict["organ_transplant_cohort.csv"] = ["Organ_Transplant_Cohort"]

    sql_table_dict["visit_info_sql.csv"] = ["Participant_Visit_Info"]
    sql_table_dict["submission.csv"] = ["Submission"]

    sql_table_dict["bsi_child.csv"] = ["BSI_Child_Aliquots"]
    sql_table_dict["bsi_parent.csv"] = ["BSI_Parent_Aliquots"]
    '''

    return sql_table_dict















#get box data function
def get_assay_data(s3_client, data_type, bucket_name):
    #file_sep = os.path.sep
    #box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"
    #assay_dir = box_dir + file_sep + "CBC_Folders"
    #assay_dir = "C:\\Data_Validation_CBC\Assay_Data_Folder"
    assay_dir = "CBC_Folders/"
    file_list = s3_client.list_objects(Bucket=bucket_name, Prefix=assay_dir)
    #print(file_list["Contents"][1])
    assay_paths = []
    path_dir = pd.DataFrame(columns=["Dir_Path", "File_Name", "Date_Created", "Date_Modified"])
    for file in file_list["Contents"]:
        if (file['Key'].endswith(".xlsx")) and ("20210402" not in file['Key']):
            file_path = file['Key']
            assay_paths.append(file_path)
            created = file['LastModified']
            modified =  file['LastModified']
            path_dir.loc[len(path_dir.index)] = [os.path.dirname(file['Key']), os.path.basename(file['Key']), created, modified]

    if data_type == "CBC_Data":
        all_assay_data = pd.DataFrame()
        all_target_data = pd.DataFrame()
        all_qc_data = pd.DataFrame()
        converion_file = pd.DataFrame()

        uni_path = list(set(path_dir["Dir_Path"]))
        for curr_path in uni_path:
            curr_folder = path_dir.query("Dir_Path == @curr_path")
            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: 'assay' in x and "assay_qc" not in x
                                                                    and "assay_target" not in x)]
            all_assay_data = populate_df(all_assay_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_qc" in x)]
            all_qc_data = populate_df(all_qc_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "assay_target_antigen" in x or
                                                                    "assay_target" in x)]
            all_target_data = populate_df(all_target_data, assay_file, bucket_name, s3_client)

            assay_file = curr_folder[curr_folder["File_Name"].apply(lambda x: "Assay_Target_Organism_Conversion.xlsx" in x)]
            converion_file = populate_df(converion_file, assay_file, bucket_name, s3_client)

        if len(all_assay_data) > 0:
            all_assay_data = box_clean_up_tables(all_assay_data, '[0-9]{2}[_]{1}[0-9]{3}$')
        if len(all_target_data) > 0:
            all_target_data = box_clean_up_tables(all_target_data, '[0-9]{2}[_]{1}[0-9]{3}$')
        if len(all_qc_data) > 0:
            all_qc_data = box_clean_up_tables(all_qc_data, '[0-9]{2}[_]{1}[0-9]{3}$')

        return all_assay_data, all_target_data, all_qc_data, converion_file
    elif data_type == "Validation":
        print("x")


def populate_df(curr_assay, assay_file, bucket_name, s3_client):
    if len(assay_file):
        curr_data = assay_file[assay_file["Date_Modified"] == max(assay_file["Date_Modified"])]
        file_path = os.path.join(curr_data["Dir_Path"].tolist()[0], curr_data["File_Name"].tolist()[0])
        #curr_data = pd.read_excel(file_path, na_filter=False, engine='openpyxl')
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        curr_data = pd.read_excel(obj['Body'].read(), na_filter=False, engine='openpyxl')
        curr_assay = pd.concat([curr_assay, curr_data])
    return curr_assay


def box_clean_up_tables(curr_table, ptrn_str):
    curr_table = curr_table[curr_table["Assay_ID"].apply(lambda x: re.compile(ptrn_str).match(str(x)) is not None)]
    curr_table = curr_table.dropna(axis=0, how="all", subset=None)
    if len(curr_table) > 0:
        missing_logic = curr_table.eq(curr_table.iloc[:, 0], axis=0).all(axis=1)
        curr_table = curr_table[[i is not True for i in missing_logic]]
        curr_table = curr_table.loc[:, ~curr_table .columns.str.startswith('Unnamed')]
        curr_table = curr_table.replace('–', '-')
        curr_table.columns = [i.replace("Assay_Target_Antigen", "Assay_Target") for i in curr_table.columns]
        curr_table.columns = [i.replace("lavage", "Lavage") for i in curr_table.columns]
    return curr_table


def get_study_design(s3_client, bucket_name):
    global success_msg
    # opens box and gets the study design.csv file directly from box
    #file_sep = os.path.sep
    #box_dir = "C:" + file_sep + "Users" + file_sep + os.getlogin() + file_sep + "Box"
    #design_dir = box_dir + file_sep + "CBC_Folders"
    design_dir = "CBC_Folders/"
    file_list = s3_client.list_objects(Bucket=bucket_name, Prefix=design_dir)
    study_paths = []

    study_data = pd.DataFrame(columns=["Cohort_Name", "Index_Date", "Notes", "CBC_Name"])
    for file in file_list["Contents"]:
        if (file['Key'].endswith(".csv")) and ("Vaccine_Respone_Study_Design" in file['Key']):
            file_path = file['Key']
            study_paths.append(file_path)

    for curr_path in study_paths:
        obj = s3_client.get_object(Bucket=bucket_name, Key= curr_path)
        curr_data = pd.read_csv(obj['Body'])
        r = os.path.dirname(curr_path)
        curr_data["CBC_Name"]= r.split("/")[1]
        study_data = pd.concat([study_data, curr_data])
    if len(study_data) > 0:
        print("## The study design data has been uploaded")
        success_msg.append("## The study design data has been updated")

    return study_data