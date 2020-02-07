from __future__ import unicode_literals
import base64
from os import path
import io
from io import StringIO
import pymysql
import imaplib
import json
import smtplib,ssl
import urllib.parse
import urllib.request
import lxml.html
from datetime import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
from email import charset
from email.charset import Charset, BASE64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.nonmultipart import MIMENonMultipart
from IPython.display import HTML
from premailer import transform

def sender(event, context):
    utc=pytz.UTC
    etl_start = datetime.now(pytz.utc)
    success=0
    message="Process started"
    try:
        GOOGLE_ACCOUNTS_BASE_URL = 'https://accounts.google.com'
        REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
        GOOGLE_CLIENT_ID = '58394777221-e6vc12ln4vb41gjpv29k87t97um89o2u.apps.googleusercontent.com'
        GOOGLE_CLIENT_SECRET = '7Zr8iR-7h2WwhquBPjASnqWs'
        GOOGLE_REFRESH_TOKEN = '1//0gBCgpkttWcjgCgYIARAAGBASNwF-L9Ir5hQ-8c5o1uCZKwIT418rqxeIQL-GAnYjW9gjJJt_RHSfFsybfRT0T_YkIrNTWnZRIb8'

        today = date.today()
        db_yest = today - timedelta(days = 2)
        yest = today - timedelta(days = 1)
        start = yest.replace(day= 1)

        def command_to_url(command):
            return '%s/%s' % (GOOGLE_ACCOUNTS_BASE_URL, command)

        def url_escape(text):
            return urllib.parse.quote(text, safe='~-._')

        def url_unescape(text):
            return urllib.parse.unquote(text)

        def url_format_params(params):
            param_fragments = []
            for param in sorted(params.items(), key=lambda x: x[0]):
                param_fragments.append('%s=%s' % (param[0], url_escape(param[1])))
            return '&'.join(param_fragments)

        def generate_permission_url(client_id, scope='https://mail.google.com/'):
            params = {}
            params['client_id'] = client_id
            params['redirect_uri'] = REDIRECT_URI
            params['scope'] = scope
            params['response_type'] = 'code'
            return '%s?%s' % (command_to_url('o/oauth2/auth'), url_format_params(params))

        def call_authorize_tokens(client_id, client_secret, authorization_code):
            params = {}
            params['client_id'] = client_id
            params['client_secret'] = client_secret
            params['code'] = authorization_code
            params['redirect_uri'] = REDIRECT_URI
            params['grant_type'] = 'authorization_code'
            request_url = command_to_url('o/oauth2/token')
            response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
            return json.loads(response)

        def call_refresh_token(client_id, client_secret, refresh_token):
            params = {}
            params['client_id'] = client_id
            params['client_secret'] = client_secret
            params['refresh_token'] = refresh_token
            params['grant_type'] = 'refresh_token'
            request_url = command_to_url('o/oauth2/token')
            response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
            return json.loads(response)

        def generate_oauth2_string(username, access_token, as_base64=False):
            auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
            if as_base64:
                auth_string = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
            return auth_string

        def test_imap(user, auth_string):
            imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
            imap_conn.debug = 4
            imap_conn.authenticate('XOAUTH2', lambda x: auth_string)
            imap_conn.select('INBOX')

        def test_smpt(user, base64_auth_string):
            smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
            smtp_conn.set_debuglevel(True)
            smtp_conn.ehlo('test')
            smtp_conn.starttls()
            smtp_conn.docmd('AUTH', 'XOAUTH2 ' + base64_auth_string)

        def get_authorization(google_client_id, google_client_secret):
            scope = "https://mail.google.com/"
            print('Navigate to the following URL to auth:', generate_permission_url(google_client_id, scope))
            authorization_code = input('Enter verification code: ')
            response = call_authorize_tokens(google_client_id, google_client_secret, authorization_code)
            return response['refresh_token'], response['access_token'], response['expires_in']

        def refresh_authorization(google_client_id, google_client_secret, refresh_token):
            response = call_refresh_token(google_client_id, google_client_secret, refresh_token)
            return response['access_token'], response['expires_in']

        engine = create_engine("mysql+pymysql://mcaff_user:mcopal123@mcaff-dwh.couzazfir9sh.ap-south-1.rds.amazonaws.com/mcaff_dwh")
        con_mysql = engine.connect()

        #dump_attachment_query_link
        attach = pd.read_sql("SELECT * from ETL_status order by 2 desc limit 100", con_mysql)

        #actual_grouped_data
        df=pd.read_sql("SELECT `Date`, `Channel`, `Campaign Type`, FORMAT(`Sessions`,0) as `Sessions`, FORMAT(`Impressions`,0) as `Impressions`, FORMAT(`Clicks`, 0) as `Clicks`, FORMAT(`Cost`,0) as `Cost`, FORMAT(`Transactions`,0) as `Transactions`, FORMAT(`Revenue`,0) as `Revenue`,  FORMAT(`Add_To_Cart`,0) as `Add To Cart`, coalesce(`Bounce Rate`, 0) as `Bounce Rate`, FORMAT(coalesce(`CPT`, 0),2) as CPT, FORMAT(coalesce(CPC, 0), 2) as CPC, coalesce(CR,0) as CR, FORMAT(coalesce(ROAS, 0), 2) as ROAS, FORMAT(coalesce(AOV, 0),0) as AOV, coalesce(CTR, 0) as CTR FROM (SELECT `Date`, `T3`.`Channel`,`T2`.`Campaign_type` as `Campaign Type`, SUM(`Sessions`) as `Sessions`, SUM(`Impressions`) as `Impressions`, SUM(`Clicks`) as `Clicks`, SUM(`Cost`) as `Cost`, SUM(`transaction`) as `Transactions`, SUM(`revenue`) as `Revenue`,  SUM(`add_To_Cart`) as `Add_To_Cart`,SUM(`bounce`)/SUM(`Sessions`) as `Bounce Rate`, SUM(`Cost`)/SUM(`transaction`) as CPT, SUM(`Cost`)/SUM(`Clicks`) as CPC, SUM(`transaction`)/SUM(`Sessions`) as CR, SUM(`revenue`)/SUM(`Cost`) as ROAS, SUM(`revenue`)/SUM(`transaction`) as AOV, SUM(`Clicks`)/SUM(`Impressions`) as CTR FROM GA_report T1 LEFT JOIN (SELECT distinct `Campaign`, `Campaign_type` FROM Campaign_Type_Master) T2 on `T1`.Campaign = `T2`.Campaign LEFT JOIN Source_Channel_Master T3 on `T1`.`Source-Medium` = `T3`.`Source-Medium` WHERE Date = (select curdate() - interval 1 DAY) GROUP BY `T3`.`Channel`, `T2`.`Campaign_type`, `Date`)A ",con_mysql)

        df1 = df.groupby(['Date', 'Channel', 'Campaign Type'], as_index= False).agg({"Sessions":sum, "Impressions":sum, "Clicks":sum, "Cost":sum, "Transactions":sum, "Revenue":sum,  "Add To Cart":sum, "Bounce Rate":sum, "CPT":sum, "CPC":sum, "CR":sum, "ROAS":sum, "AOV":sum, "CTR":sum})

        #actual_totals_data
        df_total = pd.read_sql("SELECT `Date`, `Channel`, `Campaign Type`, FORMAT(`Sessions`,0) as `Sessions`, FORMAT(`Impressions`,0) as `Impressions`, FORMAT(`Clicks`, 0) as `Clicks`, FORMAT(`Cost`,0) as `Cost`, FORMAT(`Transactions`,0) as `Transactions`, FORMAT(`Revenue`,0) as `Revenue`,  FORMAT(`Add_To_Cart`,0) as `Add To Cart`, FORMAT(coalesce(`Bounce Rate`, 0),2) as `Bounce Rate`, FORMAT(coalesce(`CPT`, 0),2) as CPT, FORMAT(coalesce(CPC, 0), 2) as CPC, FORMAT(coalesce(CR,0), 2) as CR, FORMAT(coalesce(ROAS, 0), 2) as ROAS, FORMAT(coalesce(AOV, 0),0) as AOV, FORMAT(coalesce(CTR, 0), 2) as CTR FROM (SELECT `Date`, `T3`.`Channel`,`T2`.`Campaign_type` as `Campaign Type`, SUM(`Sessions`) as `Sessions`, SUM(`Impressions`) as `Impressions`, SUM(`Clicks`) as `Clicks`, SUM(`Cost`) as `Cost`, SUM(`transaction`) as `Transactions`, SUM(`revenue`) as `Revenue`,  SUM(`add_To_Cart`) as `Add_To_Cart`,SUM(`bounce`)/SUM(`Sessions`) as `Bounce Rate`, SUM(`Cost`)/SUM(`transaction`) as CPT, SUM(`Cost`)/SUM(`Clicks`) as CPC, SUM(`transaction`)/SUM(`Sessions`) as CR, SUM(`revenue`)/SUM(`Cost`) as ROAS, SUM(`revenue`)/SUM(`transaction`) as AOV, SUM(`Clicks`)/SUM(`Impressions`) as CTR FROM GA_report  T1 LEFT JOIN (SELECT distinct `Campaign`, `Campaign_type` FROM Campaign_Type_Master) T2 on `T1`.Campaign = `T2`.Campaign LEFT JOIN Source_Channel_Master T3 on `T1`.`Source-Medium` = `T3`.`Source-Medium` WHERE Date = (select curdate() - interval 1 DAY) AND (`Channel` = 'facebook' or `Channel` = 'google') GROUP BY `T3`.`Channel`, `Date`)A", con_mysql)
        df_total['Campaign Type'] = "Total"

        df_GT = pd.read_sql("SELECT `Date`, `Channel`, `Campaign Type`, FORMAT(`Sessions`,0) as `Sessions`, FORMAT(`Impressions`,0) as `Impressions`, FORMAT(`Clicks`, 0) as `Clicks`, FORMAT(`Cost`,0) as `Cost`, FORMAT(`Transactions`,0) as `Transactions`, FORMAT(`Revenue`,0) as `Revenue`,  FORMAT(`Add To Cart`,0) as `Add To Cart`, FORMAT(coalesce(`Bounce Rate`, 0),2) as `Bounce Rate`, FORMAT(coalesce(`CPT`, 0),2) as CPT, FORMAT(coalesce(CPC, 0), 2) as CPC, FORMAT(coalesce(CR,0), 2) as CR, FORMAT(coalesce(ROAS, 0), 2) as ROAS, FORMAT(coalesce(AOV, 0),0) as AOV, FORMAT(coalesce(CTR, 0), 2) as CTR FROM (SELECT `Date`, `Campaign` as `Channel`,`campaign_id` as `Campaign Type`, SUM(`Sessions`) as `Sessions`, SUM(`Impressions`) as `Impressions`, SUM(`Clicks`) as `Clicks`, SUM(`Cost`) as `Cost`, SUM(`transaction`) as `Transactions`, SUM(`revenue`) as `Revenue`,  SUM(`add_To_Cart`) as `Add To Cart`,SUM(`bounce`)/SUM(`Sessions`) as `Bounce Rate`, SUM(`Cost`)/SUM(`transaction`) as CPT, SUM(`Cost`)/SUM(`Clicks`) as CPC, SUM(`transaction`)/SUM(`Sessions`) as CR, SUM(`revenue`)/SUM(`Cost`) as ROAS, SUM(`revenue`)/SUM(`transaction`) as AOV, SUM(`Clicks`)/SUM(`Impressions`) as CTR FROM GA_report WHERE Date = (select curdate() - interval 1 DAY) GROUP BY `Date`)A",con = con_mysql)
        df_GT = df_GT.replace("(not set)", "Grand Total")

        #actual+totals
        frames = [df_total, df1]
        df_1 = pd.concat(frames)

        #sort_case_insensitive
        b = df_1.sort_values(by = ['Channel', 'Campaign Type'], ascending = [True, False])

        #actual+totals+GrandTotals
        frames2 = [b,df_GT]
        df_whole = pd.concat(frames2)

        #commas_rounding_decimals
        df_whole = df_whole.round({"Sessions":0, "Impressions":0, "Clicks":0,"Cost":0, "Transactions":0, "Revenue":0, "Add To Cart":0, "CPT":2, "CPC":2, "ROAS":2, "AOV":2})
        df_whole['Bounce Rate'] = df_whole['Bounce Rate'].astype(float).map(lambda n: '{:.2%}'.format(n))
        df_whole['CR'] = df_whole['CR'].astype(float).map(lambda n: '{:.2%}'.format(n))
        df_whole['CTR'] = df_whole['CTR'].astype(float).map(lambda n: '{:.2%}'.format(n))

        mask = (df_whole['Channel'] != "Facebook") & (df_whole['Channel'] != "Google") & (df_whole['Channel'] != "Grand Total")
        df_whole['Campaign Type'][mask] = " "

        data = df_whole.drop([df_whole.columns[0]], axis = 1)

        df_index = data.set_index(['Channel', 'Campaign Type'])

        #Highliting_Totals_in_DataFrame

        def highlight_total(s):
                  is_total = s.index.get_level_values(1).str.contains(pat = "Total")
                  return ['background-color: #00A699' if v else 'background-color: #FBFCFC' for v in is_total]

        def bold_total(s):
                  is_total = s.index.get_level_values(1).str.contains(pat = "Total")
                  return ['font-weight: bolder' if v else 'font-weight: normal' for v in is_total]
                  
        x = (((df_index.style.apply(highlight_total)).apply(bold_total)).set_properties(**{'border-style': 'solid'}, **{'border-width':'0.5px'}, **{'text-align': 'Left'})).set_table_styles([{'selector': 'th','props': [('background', '#D3D3D3'), ('border-style', 'solid'), ('border-width', '0.5px')]}])
        Y = transform(x.render(), pretty_print = True)

        #FILENAME-SETUP
        def filenames(start, yest):
          if start == yest:
            return "GA_Dump_{}.csv".format(yest)
          else:
            return "GA_Dump_{}_to_{}.csv".format(start, yest)

        attach.reset_index(drop = True, inplace = True)

        path= r'/tmp/{}'.format(filenames(start,yest))
        attach.to_csv(path, sep = str(','))
        footer = "PFA the file - {}".format(filenames(start, yest))
        files = filenames(start, yest)

        def send_mail(fromaddr, subject, message):
            access_token, expires_in = refresh_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
            auth_string = generate_oauth2_string(fromaddr, access_token, as_base64=True)

            msg = MIMEMultipart('related')
            msg['Subject'] = subject + ": %s" %yest 
            msg['From'] = fromaddr
            msg['To'] = "indra@neenopal.com"
            #msg['To'] = "shubham.g@mcaffeine.com,kanchan@mcaffeine.com,vaishali@mcaffeine.com,siddhika@mcaffeine.com"
            msg['Cc'] = "indra@neenopal.com"
            msg.preamble = 'This is a multi-part message in MIME format.'
            msg_alternative = MIMEMultipart('alternative')
            msg.attach(msg_alternative)
            part_text = MIMEText(lxml.html.fromstring(message).text_content().encode('utf-8'), 'plain', _charset='utf-8')
            part_html = MIMEText(message.encode('utf-8'), 'html', _charset='utf-8')
            msg_alternative.attach(part_text)
            msg_alternative.attach(part_html)

            toupload = open(path, "rb")
            part = MIMEBase('application', "octet-stream")
            part.set_payload(toupload.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename = {}'.format(files))
            msg.attach(part)

            server = smtplib.SMTP('smtp.gmail.com:587')
            server.ehlo(GOOGLE_CLIENT_ID)
            server.starttls()
            server.docmd('AUTH', 'XOAUTH2 ' + auth_string)
            server.sendmail(fromaddr,msg['To'].split(",") + msg['Cc'].split(","), msg.as_string())
            server.quit()

        send_mail('dj@mcaffeine.com','GA - Daily Report','<h3> <b><u>GA Report for the Date of {}: </u></b></h3> {}''<h3><i>{}</i></h3>'''.format(yest, Y, footer))
                
        success=1 
        message="Mail Sent for the date of {}".format(yest)   
    except Exception as e:
        print(e)
        success=0 
        message=str(e)  
    finally:
        engine = create_engine("mysql+pymysql://mcaff_user:mcopal123@mcaff-dwh.couzazfir9sh.ap-south-1.rds.amazonaws.com/mcaff_dwh")
        con_mysql = engine.connect()
        etl_end= datetime.now()
        d = {'table_name': ['GA_DailyMail'],'etl_start': [etl_start],'etl_end':[etl_end],'success':[success],'message':[message]}
        df = pd.DataFrame(data=d)
        df.to_sql(name='ETL_status',con=con_mysql,if_exists='append',index=False)
