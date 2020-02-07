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


GOOGLE_ACCOUNTS_BASE_URL = 'https://accounts.google.com'
REDIRECT_URL = ***REDIRECT_URL***
GOOGLE_CLIENT_ID = ***GOOGLE_CLIENT_ID***
GOOGLE_CLIENT_SECRET = ***GOOGLE_CLIENT_SECRET***
GOOGLE_REFRESH_TOKEN = ***GOOGLE_REFRESH_TOKEN***

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

engine = create_engine("mysql+pymysql://user:pass@db_link/db_name")
con_mysql = engine.connect()

#dump_attachment_query_link
Query1 = """  """
attach = pd.read_sql(Query1, con_mysql)


df_index = data.set_index(['Column1', 'Column2'])

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
    return "Attach_{}.csv".format(yest)
  else:
    return "Attach_{}_to_{}.csv".format(start, yest)

attach.reset_index(drop = True, inplace = True)

path= r'/tmp/{}'.format(filenames(start,yest))
attach.to_csv(path, sep = str(','))
footer = "PFA the file - {}".format(filenames(start, yest))
files = filenames(start, yest)

def send_mail(fromaddr, subject, message):
    access_token, expires_in = refresh_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    auth_string = generate_oauth2_string(fromaddr, access_token, as_base64=True)

    msg = MIMEMultipart('related')
    msg['Subject'] = subject 
    msg['From'] = fromaddr
    msg['To'] = "indra@gmail.com,indra1@gmail.com,indra2@gmail.com"
    msg['Cc'] = "indra3@neenopal.com,indra4@gmail.com,indra5@gmail.com,indra6@gmail.com"
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

send_mail('indra0@gmail.com','Daily Report','<h3> <b><u>Report for the Date of {}: </u></b></h3> {}''<h3><i>{}</i></h3>'''.format(yest, Y, footer))
