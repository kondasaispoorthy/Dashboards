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
import pandas as pd
from datetime import date
from datetime import timedelta
import boto3
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    # Python code for getting yesterday's code 
    yesterday = str(date.today() - timedelta(days = 1))
    client = boto3.client('ssm', region_name='eu-central-1')
    # Convert symbols to HTML entities for displaying in HTML
    symbol_mapping = {
        '▲': '&#9650;',  # Upwards arrow
        '▼': '&#9660;'   # Downwards arrow
    }
    # Retriving gmail user from AWS SSM
    parameter = client.get_parameter(Name='/users/email/account/user', WithDecryption=True)
    gmail_user = parameter.get("Parameter").get("Value")
    #Retriving gmail app password from AWS SSM
    parameter = client.get_parameter(Name='/users/email/account/password', WithDecryption=True)
    gmail_app_password = parameter.get("Parameter").get("Value")
    # Email configuration
    sender_email = "kondasaispoorthy@gmail.com"
    receiver_email = "konda.saispoorthy@bizacuity.com"
    sender_display_name  = "BI Support"
    password = "tlbr ytui wgtn lxoy"
    # Create a multipart message
    msg = MIMEMultipart()
    msg["From"] = f"{sender_display_name} <{sender_email}>"
    #msg["To"] = receiver_email
    msg['To'] = "pavan.muppa@bizacuity.com,konda.saispoorthy@bizacuity.com"
    #msg['To'] = "kondasaispoorthy@gmail.com,konda.saispoorthy@bizacuity.com"
    msg["Subject"] = "PLAYER INSIGHTS DASHBOARD"
    try:
        parameter = client.get_parameter(Name='/tableau/token/name', WithDecryption=True)
        tableau_user = parameter.get("Parameter").get("Value")
        parameter = client.get_parameter(Name='/tableau/token/secret', WithDecryption=True)
        tableau_secret = parameter.get("Parameter").get("Value")
        # Code from tableau
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
        # Connecting to tableau server and fetching view images
        conn = TableauServerConnection(tableau_server_config)
        response = conn.sign_in()
        print(response)
        print("Connection Estabished")
        views_df = querying.get_views_dataframe(conn)
        views_df = views_df[views_df["project"].notnull()]
        #df = pd.json_normalize(views_df["project"])
        views_df =flatten_dict_column(views_df,keys=["name","id"],col_name="project")
        views_df =flatten_dict_column(views_df,keys=["name","id"],col_name="workbook")
        views_df = views_df[views_df["project_name"] == "Prod"]
        views_df =  views_df[views_df["workbook_name"] == "PLAYER INSIGHTS DASHBOARD"]
        # Demography view
        visual_id = "8be84b00-c320-423b-a39d-87d978f8e23b"
        view_png_image = conn.query_view_image(view_id=visual_id)
        print(view_png_image.status_code)
        # System Engagement view
        visual_id1 = "0d32b7b6-419e-463a-bd3c-6f191baa4d86"
        view_png_image1 = conn.query_view_image(view_id=visual_id1)
        print(view_png_image1.status_code)
        # Product Engagement view
        visual_id2 = "58476100-9d04-4ef8-9811-8a97bfebabbb"
        view_png_image2 = conn.query_view_image(view_id=visual_id2)
        print(view_png_image2.status_code)
        conn.sign_out()
        parameter = client.get_parameter(Name='/users/snowflake/account', WithDecryption=True)
        snf_account = parameter.get("Parameter").get("Value")
        
        parameter = client.get_parameter(Name='/users/snowflake/account/user', WithDecryption=True)
        snf_user = parameter.get("Parameter").get("Value")
        
        parameter = client.get_parameter(Name='/users/snowflake/account/password', WithDecryption=True)
        snf_password = parameter.get("Parameter").get("Value")
        # Code from SnowFlake
        snowflake_conn = snowflake.connector.connect(
        	user=snf_user,
        	password=snf_password,
        	account=snf_account,
        	warehouse='BIT_AWS_PROD_LOAD_WH',
        	database='DEV_RZ',
        	schema='BDD_BITSLER',
        	role='SYSADMIN'
        )
        cur = snowflake_conn.cursor()
        cur.execute("CALL PROD_RZ.BDD_BITSLER.PLAYER_INSIGHTS_DASHBOARD()")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[i[0] for i in cur.description])
        df['dev15'] =  df['DEVIATION15%']
        df['DEVIATION15%'] = df['DEVIATION15%'].astype(str)
        df['dev30'] =  df['DEVIATION30%']
        df['DEVIATION30%'] = df['DEVIATION30%'].astype(str)
        for i in df.index:
            if df.at[i,'dev15'] < 0:
                df.at[i,'DEVIATION15%'] =  '▼' + str(abs(df.at[i,'dev15']))
            elif df.at[i,'dev15'] > 0:
                df.at[i,'DEVIATION15%'] =  '▲' +  str(abs(df.at[i,'dev15']))
        for i in df.index:
            if df.at[i,'dev30'] < 0:
                df.at[i,'DEVIATION30%'] =  '▼'  +  str(abs(df.at[i,'dev30']))
            elif df.at[i,'dev15'] > 0:
                df.at[i,'DEVIATION30%'] = '▲'   +  str(df.at[i,'dev30']) 
        # Drop multiple columns
        df.drop(columns=['dev15', 'dev30'], inplace=True)
        # Converting all the columns into String Type
        df = df.astype({"CURRENTVALUE" : str, "AVG15" :str,"AVG30" : str,"DEVIATION15%" :str, "DEVIATION30%":str})
        # Define a function to append a specific string based on values in Column MEASURE
        def append_string(row, col_name):
            if row['MEASURE'] == 'Unique Actives':
                return row[col_name] 
            elif row['MEASURE'] == 'XP Points Per Player':
                return row[col_name] 
            else:
                return "€ " + row[col_name]  
        # List of columns to be modified
        columns_to_modify = ['CURRENTVALUE', 'AVG15', 'AVG30']  
        for col in columns_to_modify:
            df[col] = df.apply(append_string,axis=1,args=(col,))
        for value in ["DEVIATION15%","DEVIATION30%"]:
            df[value] = df[value] + "%"
        # Attach some Insights into email
        html_table = df.to_html(justify='center',index=False)
        #Aligning the values in df
        for measure_value in df['MEASURE'].tolist():  # Iterate through MEASURE values
        	replacement_string = f'<td>{measure_value}</td>'
        	left_aligned_string = f'<td style="text-align: left;">{measure_value}</td>'
        	html_table = html_table.replace(replacement_string, left_aligned_string)
        # Replace '▲' with green color and '▼' with red color in the HTML table
        html_table = html_table.replace('▲', '<span style="color:green">&#9650;</span>').replace('▼', '<span style="color:red">&#9660;</span>')
        html_table = html_table.replace('<th>MEASURE</th>','<th>KPI</th>')
        html_table = html_table.replace('<th>AVG15</th>','<th>AVG VALUE FOR<br>LAST 15 DAYS</th>')
        html_table = html_table.replace('<th>AVG30</th>','<th>AVG VALUE FOR<br>LAST 30 DAYS</th>')
        html_table = html_table.replace('<th>DEVIATION15%</th>','<th>DEVIATION FROM <br>15 DAYS AVG </th>')
        html_table = html_table.replace('<th>DEVIATION30%</th>','<th>DEVIATION FROM <br>30 DAYS AVG </th>')
        html_table = html_table.replace('<th>CURRENTVALUE</th>',f'<th>VALUE<br> ON {yesterday}</th>')
        html_table = html_table.replace('<td>','<td style="text-align:center;">')
        msg.attach(MIMEText(html_table, "html"))
        msg.attach(MIMEText('\n'))
        image_parts = []
        image_data = [(view_png_image.content, "Demography.png"), (view_png_image1.content, "SystemEngagement.png"), (view_png_image2.content, "ProductEngagement.png")]
        for idx, (content, name) in enumerate(image_data, start=1):
            image_part = MIMEImage(content, name=f"Player Insights {idx}")
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
                html_body += f'<b>Demography</b>'
                html_body += '</div>'
            elif idx == 2:
                html_body += '<div>'
                html_body += f'<b>System Engagement</b>'
                html_body += '</div>'
            elif idx == 3:
                html_body += '<div>'
                html_body += f'<b>Product Engagement</b>'
                html_body += '</div>'
            html_body += '<div>'
            html_body += f'<img src="cid:image{idx}" alt="Embedded Image {idx}" width="950" height="600">'
            html_body += '</div>'
        html_body += '</div>'
        html_body+="<p>This View Contains Data till yesterday</p>"
        html_body+='<p>Access DashBoard Here <a href="https://prod-uk-a.online.tableau.com/#/site/bitslerreportsportal/views/PlayerInsights_16819860746690/Demography?:iid=1">PLAYER INSIGHTS DASHBOARD</a></p>'
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
        return {
        'statusCode': 200,

        }
    except Exception as e:
        print(e)
