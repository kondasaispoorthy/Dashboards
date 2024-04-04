# Importing the neccessary libraries
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from tableau_api_lib import TableauServerConnection
import numpy as np
import pandas as pd
from tableau_api_lib.utils import querying,flatten_dict_column
from tableau_api_lib.utils.querying import  get_views_dataframe
import snowflake.connector
import base64
from datetime import date
from datetime import timedelta
import boto3


symbol_mapping = {
    '▲': '&#9650;',  # Upwards arrow
    '▼': '&#9660;'   # Downwards arrow
}
# Python code for getting yesterday's code 
yesterday = str(date.today() - timedelta(days = 1))
# Email configuration
sender_email = "kondasaispoorthy@gmail.com"
receiver_email = "konda.saispoorthy@bizacuity.com"
password = "tlbr ytui wgtn lxoy"
sender_display_name  = "BI Support"
# create a client object
client = boto3.client('ssm', region_name='eu-central-1')
# Retriving gmail_user from AWS SSM
parameter = client.get_parameter(Name='/users/email/account/user', WithDecryption=True)
gmail_user = parameter.get("Parameter").get("Value")
# Retriving app_password from AWS SSM
parameter = client.get_parameter(Name='/users/email/account/password', WithDecryption=True)
gmail_app_password = parameter.get("Parameter").get("Value")
# Create a multipart message
msg = MIMEMultipart()
msg["From"] = f"{sender_display_name} <{sender_email}>"
#msg['To'] = "pavan.muppa@bizacuity.com,harshavardhan.raju.com"
msg['To'] = "kondasaispoorthy@gmail.com,konda.saispoorthy@bizacuity.com"
msg["Subject"] = "PLAYER RETENTION DASHBOARD"

def lambda_handler(event, context):
    try:
        parameter = client.get_parameter(Name='/tableau/token/name', WithDecryption=True)
        tableau_user = parameter.get("Parameter").get("Value")
        parameter = client.get_parameter(Name='/tableau/token/secret', WithDecryption=True)
        tableau_secret = parameter.get("Parameter").get("Value")
        # Connecting to tableau
        tableau_server_config = {
                'tableau_prod': {
                        'server': 'https://prod-uk-a.online.tableau.com/',
                        'api_version': '3.22',
                        'personal_access_token_name': tableau_user,
                        'personal_access_token_secret': tableau_secret,
                        'site_name': 'bitslerreportsportal',
                        'site_url' : ''
                }
        }
        # code from tableau
        conn = TableauServerConnection(tableau_server_config)
        response = conn.sign_in()
        print(response)
        print("Connection Estabished")
        views_df = querying.get_views_dataframe(conn)
        views_df = views_df[views_df["project"].notnull()]
        views_df =flatten_dict_column(views_df,keys=["name","id"],col_name="project")
        views_df =flatten_dict_column(views_df,keys=["name","id"],col_name="workbook")
        views_df = views_df[views_df["project_name"] == "Prod"]
        views_df = views_df[views_df["workbook_name"] == "PLAYER RETENTION DASHBOARD"]
        visual_ids = views_df["id"].to_list()
        visual_id1,visual_id2 = visual_ids[0],visual_ids[1]
        view_png_image1 = conn.query_view_image(view_id=visual_id1)
        view_png_image2 = conn.query_view_image(view_id=visual_id2)
        print(view_png_image1.status_code)
        print(view_png_image2.status_code)
        conn.sign_out()
        # Connecting to snowflake
        parameter = client.get_parameter(Name='/users/snowflake/account', WithDecryption=True)
        snf_account = parameter.get("Parameter").get("Value")
        
        parameter = client.get_parameter(Name='/users/snowflake/account/user', WithDecryption=True)
        snf_user = parameter.get("Parameter").get("Value")
        
        parameter = client.get_parameter(Name='/users/snowflake/account/password', WithDecryption=True)
        snf_password = parameter.get("Parameter").get("Value")
        snowflake_conn = snowflake.connector.connect(
        user= snf_user,
        password=snf_password,
        account=snf_account,
        warehouse='BIT_AWS_PROD_LOAD_WH',
        database='DEV_RZ',
        schema='BDD_BITSLER',
        role='SYSADMIN'
        )
        cur = snowflake_conn.cursor()
        cur.execute("CALL PROD_RZ.BDD_BITSLER.PLAYER_RETENTION_DASHBOARD()")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[i[0] for i in cur.description])
        df1 = df.iloc[0:3]
        df2 = df[df["MEASURE"].isin(["GGR%","NGR%"])]
        df1['dev15'] =  df1['DEVIATION15%']
        df1['DEVIATION15%'] = df1['DEVIATION15%'].astype(str)
        df1['dev30'] =  df1['DEVIATION30%']
        df1['DEVIATION30%'] = df1['DEVIATION30%'].astype(str)
        df2['dev15'] =  df2['DEVIATION15%']
        df2['DEVIATION15%'] = df2['DEVIATION15%'].astype(str)
        df2['dev30'] =  df2['DEVIATION30%']
        df2['DEVIATION30%'] = df2['DEVIATION30%'].astype(str)
        # For Converting values in first table
        for i in df1.index:
            if df1.at[i,'dev15'] < 0:
                df1.at[i,'DEVIATION15%'] =  '▼' + str(abs(df1.at[i,'dev15']))
            elif df1.at[i,'dev15'] > 0:
                df1.at[i,'DEVIATION15%'] =  '▲' +  str(abs(df1.at[i,'dev15']))
        for i in df1.index:
            if df1.at[i,'dev30'] < 0:
                df1.at[i,'DEVIATION30%'] =  '▼' + str(abs(df1.at[i,'dev30']))
            elif df1.at[i,'dev30'] > 0:
                df1.at[i,'DEVIATION30%'] =  '▲' +  str(abs(df1.at[i,'dev30']))
         # For Converting values in first table
        for i in df2.index:
            if df2.at[i,'dev15'] < 0:
                df2.at[i,'DEVIATION15%'] =  '▼' + str(abs(df2.at[i,'dev15']))
            elif df2.at[i,'dev15'] > 0:
                df2.at[i,'DEVIATION15%'] =  '▲' +  str(abs(df2.at[i,'dev15']))
        for i in df2.index:
            if df2.at[i,'dev30'] < 0:
                df2.at[i,'DEVIATION30%'] =  '▼' + str(abs(df2.at[i,'dev30']))
            elif df2.at[i,'dev30'] > 0:
                df2.at[i,'DEVIATION30%'] =  '▲' +  str(abs(df2.at[i,'dev30']))
        # Drop multiple columns
        df1.drop(columns=['dev15', 'dev30'], inplace=True)
        df2.drop(columns=['dev15', 'dev30'], inplace=True)
        df1 = df1.astype({"CURRENTVALUE" : int, "AVG15" : int, "AVG30" : int})
        df1 = df1.astype({"CURRENTVALUE" : str, "AVG15" :str,"AVG30" : str,"DEVIATION15%" :str, "DEVIATION30%":str})
        df2 = df2.astype({"CURRENTVALUE" : str, "AVG15" :str,"AVG30" : str,"DEVIATION15%" :str, "DEVIATION30%":str})
        for value in ["CURRENTVALUE","AVG15","AVG30"]:
            df1[value] = "€ " + df1[value] 
        for value in ["DEVIATION15%","DEVIATION30%"]:
            df1[value] = df1[value] + "%"
        for value in ["DEVIATION15%","DEVIATION30%","CURRENTVALUE","AVG15","AVG30"]:
            df2[value] = df2[value] + "%"
        # Renaming the measure column in both dataframes
        df1 = df1.rename(columns={'MEASURE': 'KPI'})
        df2 = df2.rename(columns={'MEASURE': 'KPI'})
        column_widths = {'KPI':75}
        # Attach some Insights into email
        html_table1 = df1.to_html(justify='center',index=False,col_space=column_widths)
        html_table2 =  df2.to_html(justify='center',index=False,col_space=column_widths)
        #Aligning the values in First Table
        for measure_value in df1['KPI'].tolist():  # Iterate through MEASURE values
            replacement_string = f'<td>{measure_value}</td>'
            left_aligned_string = f'<td style="text-align: left;">{measure_value}</td>'
            html_table1 = html_table1.replace(replacement_string, left_aligned_string)
        # Replace '▲' with green color and '▼' with red color in the HTML table
        html_table1 = html_table1.replace('▲', '<span style="color:green">&#9650;</span>').replace('▼', '<span style="color:red">&#9660;</span>')
        #html_table1 = html_table1.replace('<th>MEASURE</th>','<th>KPI</th>')
        html_table1 = html_table1.replace('<th>AVG15</th>','<th>AVG VALUE FOR<br>LAST 15 DAYS</th>')
        html_table1 = html_table1.replace('<th>AVG30</th>','<th>AVG VALUE FOR<br>LAST 30 DAYS</th>')
        html_table1 = html_table1.replace('<th>DEVIATION15%</th>','<th>DEVIATION FROM<br>15 DAYS AVG</th>')
        html_table1 = html_table1.replace('<th>DEVIATION30%</th>','<th>DEVIATION FROM<br>30 DAYS AVG</th>')
        html_table1 = html_table1.replace('<th>CURRENTVALUE</th>',f'<th>VALUE<br>ON {yesterday}</th>')
        html_table1 = html_table1.replace('<td>','<td style="text-align:center;">')
        #Aligning the values in second Table
        for measure_value in df2['KPI'].tolist():  # Iterate through MEASURE values
            replacement_string = f'<td>{measure_value}</td>'
            left_aligned_string = f'<td style="text-align: left;">{measure_value}</td>'
            html_table2 = html_table2.replace(replacement_string, left_aligned_string)
        # Replace '▲' with green color and '▼' with red color in the HTML table
        html_table2 = html_table2.replace('▲', '<span style="color:green">&#9650;</span>').replace('▼', '<span style="color:red">&#9660;</span>')
        #html_table2 = html_table2.replace('<th>MEASURE</th>','<th>KPI</th>')
        html_table2 = html_table2.replace('<th>AVG15</th>','<th> AVG VALUE FOR<br>LAST 15 DAYS</th>')
        html_table2 = html_table2.replace('<th>AVG30</th>','<th> AVG VALUE FOR<br>LAST 30 DAYS</th>')
        html_table2 = html_table2.replace('<th>DEVIATION15%</th>','<th>DEVIATION FROM<br>15 DAYS AVG</th>')
        html_table2 = html_table2.replace('<th>DEVIATION30%</th>','<th>DEVIATION FROM<br>30 DAYS AVG</th>')
        html_table2 = html_table2.replace('<th>CURRENTVALUE</th>',f'<th>VALUE<br> ON {yesterday}</th>')
        html_table2 = html_table2.replace('<td>','<td style="text-align:center;">')
        msg.attach(MIMEText(html_table1, "html"))
        msg.attach(MIMEText('\n'))
        msg.attach(MIMEText(html_table2, "html"))
        msg.attach(MIMEText('\n'))
        image_parts = []
        image_data = [(view_png_image1.content, "Retention Insights.png"),(view_png_image2.content, "Retention Management.png")]
        for idx, (content,name) in enumerate(image_data,start=1):
            image_part = MIMEImage(content, name=f"Player Retention {idx}")
            # Set appropriate content type based on image format (e.g., JPEG, PNG)
            image_part.add_header("Content-Type", "PNG")
            image_part.add_header("Content-ID", f"<image{idx}>")
            msg.attach(image_part)
            image_parts.append(image_part)
        # Create the HTML body with image tags referencing the Content-IDs
        html_body = "<html>"
        html_body += '''
        <head>
        <style>
        .grid-container {
        display: grid;
        grid-template-columns: auto;
        gap: 10x;
        # background-color: #2196F3;
        }
        </style>
        </head>
        '''
        html_body += '<body>'
        html_body += '<div class="grid-container">'
        for idx, image_part in enumerate(image_parts, start=1):
            if idx == 1:
                html_body += '<div>'
                html_body += f'<b>Retention Insights</b>'
                html_body += '</div>'
                
            elif idx == 2:
                html_body += '<div>'
                html_body += f'<b>Retention Management</b>'
                html_body += '</div>'
            html_body += '<div>'
            html_body += f'<img src="cid:image{idx}" alt="Embedded Image {idx}" width="950" height="600">'
            html_body += '</div>'
        html_body += '</div>'
        html_body+="<p>This View Contains Data till yesterday</p>"
        html_body+='<p>Access DashBoard Here <a href="https://prod-uk-a.online.tableau.com/#/site/bitslerreportsportal/views/PLAYER RETENTIONDASHBOARD_17010710460960/RetentionInsights?:iid=1">PLAYER RETENTION DASHBOARD</a></p>'
        html_body += "</body></html>"
        html_body = MIMEText(html_body, "html")
        msg.attach(html_body)
        print(html_body)
        # Connect to Gmail's SMTP server
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        
        # Login to Gmail
        server.login(gmail_user,gmail_app_password)
        
        server.sendmail(gmail_user,msg['To'].split(','), msg.as_string())
        
        # Quit the server
        server.quit()
        print("Email with image sent successfully!")
            
        # TODO implement
        return {
            'statusCode': 200,

        }
    except Exception as e:
        print(e)
