#!/usr/bin/python

to_cpu_addr = ['yhan222@wisc.edu', 'ckoch5@wisc.edu', 'gthain@cs.wisc.edu', 'bbockelman@morgridge.org', 'jcpatton@wisc.edu']
to_gpu_addr = ['yhan222@wisc.edu', 'ckoch5@wisc.edu', 'gthain@cs.wisc.edu', 'bbockelman@morgridge.org', 'jcpatton@wisc.edu', 'AGitter@morgridge.org']
# to_cpu_addr = ['yhan222@wisc.edu']
# to_gpu_addr = ['hym980321@gmail.com']

def convertFile(input_Filename, output_Filename):
    import json 
    import csv 
    import pandas as pd
    with open(input_Filename) as json_file: 
        input_data = json.load(json_file) 
    output_file = open("./data/" + output_Filename, 'w') 
    csv_writer = csv.writer(output_file)
    count = 0
    if len(input_data) == 0:
        if (input_Filename == 'scheddStats.json' or input_Filename == 'scheddGpuStats.json' or input_Filename == "scheddPrintStats.json" or input_Filename == "scheddGpuPrintStats.json"):
            headline = ["Schedd","Completed Hours","Used Hours","Uniq Job Ids","Request Mem","Used Mem","Max Mem","Request Cpus","Short Jobs","All Jobs","NumShadowStarts","Request Gpus","Gpus Usage","Min","25%","Median","75%","Max","Mean","Std"]
        else:
            headline = ["User","Completed Hours","Used Hours","Uniq Job Ids","Request Mem","Used Mem","Max Mem","Request Cpus","Short Jobs","All Jobs", "NumShadowStarts","Request Gpus","Gpus Usage","Min","25%","Median","75%","Max","Mean","Std", "ScheddName","Schedd"]
        csv_writer.writerow(headline) 
    for currObs in input_data: 
        if count == 0: 
            header = list(input_data[currObs].keys())
            if (input_Filename == 'scheddStats.json' or input_Filename == 'scheddGpuStats.json' or input_Filename == "scheddPrintStats.json" or input_Filename == "scheddGpuPrintStats.json"):
                header.insert(0, "Schedd")
            else:
                header.insert(0, "User")
            csv_writer.writerow(header) 
            count += 1
        content = list(input_data[currObs].values())
        content.insert(0, currObs)
        csv_writer.writerow(content) 
    output_file.close()
    # df = pd.read_csv("./data/" + output_Filename)
    # df = df.sort_values(by=['Completed Hours'], ascending=False)
    # df.to_csv("./data/" + output_Filename, index=False) 


def color_tr(s):
    import re
    n = [1]
    def repl(m):
        n[0] += 1
        return '<tr>' if n[0] % 2 else '<tr bgcolor="MistyRose">'
    return re.sub(r'<tr>', repl, s)


def send_user_email(current_time, userFileName, scheddFileName, userPrintFile, scheedPrintFile):
  from email.mime.multipart import MIMEMultipart
  from email.mime.text  import MIMEText
  from email.mime.base import MIMEBase
  from email import encoders
  import smtplib
  import csv
  from tabulate import tabulate

  with open(userPrintFile) as input_file:
    reader = csv.reader(input_file)
    userdata = list(reader)

  with open(scheedPrintFile) as input_file:
    reader = csv.reader(input_file)
    schedddata = list(reader)

  html = """
    <html>
    <head>
    <style> """
    
  if (len(userdata) == 1 or len(schedddata) == 1): 
    html = html + """
      table, th, td {{ border: 1px solid black; border-collapse: separate; text-align: center}}
      td {{ padding: 5px; text-align: right; font-weight: bold;}}
      td,th {{background-color: #D3D3D3}}"""
  else:
    html = html + """
      table, th, td {{ border: 1px solid black; border-collapse: separate; text-align: center}}
      td {{ padding: 5px; text-align: right}}
      th {{background-color: #D3D3D3}}"""
  html = html + """  
    </style>
    </head>
    <body>
    <p style="text-align: center; display: block; margin: auto">CHTC per user usage for """  + str(current_time) +  """   
    </p>
    {usertable}
    <p style="text-align: center; display: block; margin: auto">CHTC per submit machine usage for """  + str(current_time) +  """   
    </p>
    {scheedtable}
    <p>Legend:<br>
        <b>Completed Hours:</b> Total hours per user for execution attempts that ran to completion <br>
        <b>Used Hours:</b> Total Hours per user for all execution attempts, including preemption and removal <br>
        <b>Uniq Job IDs:</b> Number of unique job ids across all execution attempts <br>
        <b>Short Job Starts:</b> Number of execution attempts that completed in less than 60 seconds <br>
        <b>All Starts:</b> Total number of execution attempts <br>
        <b>72 Hour:</b> Number of execution attempts that were preempted after 72 hours <br>
        <b>Min/25%/Median/Max/Mean/Std:</b> Statistics per completed execution attemps longer than 60 seconds</p>
    </body></html>
    """



  if (len(userdata) == 1 or len(schedddata) == 1):
    html = html.format(usertable=tabulate(userdata, tablefmt="html"),
                     scheedtable=tabulate(schedddata, tablefmt="html"))
  else:
    html = html.format(usertable=tabulate(userdata, headers="firstrow", tablefmt="html", showindex="always"),
                     scheedtable=tabulate(schedddata, headers="firstrow", tablefmt="html", showindex="always"))



  html = color_tr(html)



  fromaddr = "chtc.memory@gmail.com"
  msg = MIMEMultipart()
  msg['From'] = 'UW Madison CHTC Usage Report'
  msg['To'] = ", ".join(to_cpu_addr)
  msg['Subject'] = "CHTC Usage Report for " + current_time

#   body = "Attachment are the csv file for CHTC Usage and GPU Report <b>(Test Only)</b> <br> The reports traverse for the last three day's indices with the completion date equal to " + current_time
#   msg.attach(MIMEText(str(body), 'html'))
    # html_body = MIMEText(html, 'html');
  
  msg.attach(MIMEText(html, 'html'))

  part = MIMEBase('application', "octet-stream")
  part.set_payload(open(userFileName, "rb").read())
  encoders.encode_base64(part)
  part.add_header('Content-Disposition', 'attachment', filename = userFileName)
  msg.attach(part)
  
  part = MIMEBase('application', "octet-stream")
  part.set_payload(open(scheddFileName, "rb").read())
  encoders.encode_base64(part)
  part.add_header('Content-Disposition', 'attachment', filename = scheddFileName)
  msg.attach(part)

  try:
    
      server = smtplib.SMTP('smtp.gmail.com', 587)
      
      server.ehlo()
      server.starttls()
      
      server.ehlo()
      
     
      server.login("chtc.memory", "uwmadison_chtc")
      
      text = msg.as_string()
      server.sendmail(fromaddr, to_cpu_addr, text)

  except Exception as e:
      print(e)
  finally:
      server.quit() 

def send_gpu_email(current_time, userGpuFileName, scheddGpuFileName, userGpuPrintFile, scheedGpuPrintFile):
  from email.mime.multipart import MIMEMultipart
  from email.mime.text  import MIMEText
  from email.mime.base import MIMEBase
  from email import encoders
  import smtplib
  import csv
  from tabulate import tabulate
  
  with open(userGpuPrintFile) as input_file:
    reader = csv.reader(input_file)
    userdata = list(reader)
  
  with open(scheedGpuPrintFile) as input_file:
    reader = csv.reader(input_file)
    schedddata = list(reader)
 

  html = """
    <html>
    <head>
    <style> """
  
  if (len(userdata) == 1 or len(schedddata) == 1): 
    html = html + """
      table, th, td {{ border: 1px solid black; border-collapse: separate; text-align: center}}
      td {{ padding: 5px; text-align: right; font-weight: bold;}}
      td,th {{background-color: #D3D3D3}}"""
  else:
    html = html + """
      table, th, td {{ border: 1px solid black; border-collapse: separate; text-align: center}}
      td {{ padding: 5px; text-align: right}}
      th {{background-color: #D3D3D3}}"""
  html = html + """  
    </style>
    </head>
    <body>
    <p style="text-align: center; display: block; margin: auto">CHTC per user usage for """  + str(current_time) +  """   
    </p>
    {usertable}
    <p style="text-align: center; display: block; margin: auto">CHTC per submit machine usage for """  + str(current_time) +  """   
    </p>
    {scheedtable}
    <p>Legend:<br>
        <b>Completed Hours:</b> Total hours per user for execution attempts that ran to completion <br>
        <b>Used Hours:</b> Total Hours per user for all execution attempts, including preemption and removal <br>
        <b>Uniq Job IDs:</b> Number of unique job ids across all execution attempts <br>
        <b>Short Job Starts:</b> Number of execution attempts that completed in less than 60 seconds <br>
        <b>All Starts:</b> Total number of execution attempts <br>
        <b>72 Hour:</b> Number of execution attempts that were preempted after 72 hours <br>
        <b>Min/25%/Median/Max/Mean/Std:</b> Statistics per completed execution attemps longer than 60 seconds</p>
    </body></html>
    """


  if (len(userdata) == 1 or len(schedddata) == 1):
    html = html.format(usertable=tabulate(userdata, tablefmt="html"),
                     scheedtable=tabulate(schedddata, tablefmt="html"))
  else:
    html = html.format(usertable=tabulate(userdata, headers="firstrow", tablefmt="html", showindex="always"),
                     scheedtable=tabulate(schedddata, headers="firstrow", tablefmt="html", showindex="always"))

  html = color_tr(html)

  fromaddr = "chtc.memory@gmail.com"
  msg = MIMEMultipart()
  msg['From'] = 'UW Madison CHTC Usage Report'
  msg['To'] = ", ".join(to_gpu_addr)
  msg['Subject'] = "CHTC GPU Usage Report for " + current_time

#   body = "Attachment are the csv file for CHTC Usage and GPU Report <b>(Test Only)</b> <br> The reports traverse for the last three day's indices with the completion date equal to " + current_time
#   msg.attach(MIMEText(str(body), 'html'))
    # html_body = MIMEText(html, 'html');
  msg.attach(MIMEText(html, 'html'))

  part = MIMEBase('application', "octet-stream")
  part.set_payload(open(userGpuFileName, "rb").read())
  encoders.encode_base64(part)
  part.add_header('Content-Disposition', 'attachment', filename = userGpuFileName)
  msg.attach(part)

  part = MIMEBase('application', "octet-stream")
  part.set_payload(open(scheddGpuFileName, "rb").read())
  encoders.encode_base64(part)
  part.add_header('Content-Disposition', 'attachment', filename = scheddGpuFileName)
  msg.attach(part)

  try:
    
      server = smtplib.SMTP('smtp.gmail.com', 587)
      
      server.ehlo()
      server.starttls()
      
      server.ehlo()
      
     
      server.login("chtc.memory", "uwmadison_chtc")
      
      text = msg.as_string()
      server.sendmail(fromaddr, to_gpu_addr, text)

  except Exception as e:
      print(e)
  finally:
      server.quit() 


if __name__ == '__main__':
    import time
    from datetime import datetime, timedelta
    import os 
    yest_date = datetime.now() - timedelta(days = 1)
    current_time = yest_date.strftime("%m-%d-%Y")
    os.chdir("/home/yhan222/nodejs-elasticsearch/server") 
    os.system("~/bin/node --max-old-space-size=8192 searchGpu.js")
    time.sleep(1)
    os.system("~/bin/node --max-old-space-size=8192 getData.js")
    time.sleep(1)
    userFileName = "userStats_" + str(current_time) +".csv"
    scheddFileName = "scheddStats_" + str(current_time) +  ".csv"
    convertFile("scheddStats.json", scheddFileName)
    time.sleep(1)
    convertFile("userStats.json", userFileName)
    userGpuFileName = "userGpuStats_" + str(current_time) +".csv"
    scheddGpuFileName = "scheddGpuStats_" + str(current_time) +  ".csv"
    convertFile("userGpuStats.json", userGpuFileName)
    time.sleep(1)
    convertFile("scheddGpuStats.json", scheddGpuFileName)

    time.sleep(1)
    userPrintFileName = "userPrintStats.csv"
    scheddPrintFileName = "scheddPrintStats.csv"
    convertFile("userPrintStats.json", userPrintFileName)
    time.sleep(1)
    convertFile("scheddPrintStats.json", scheddPrintFileName)
    userGpuPrintFileName = "userGpuPrintStats.csv"
    scheddGpuPrintFileName = "scheddGpuPrintStats.csv"
    convertFile("userGpuPrintStats.json", userGpuPrintFileName)
    time.sleep(1)
    convertFile("scheddGpuPrintStats.json", scheddGpuPrintFileName)

    os.chdir("/home/yhan222/nodejs-elasticsearch/server/data") 
    send_user_email(current_time, userFileName, scheddFileName, userPrintFileName, scheddPrintFileName)
    time.sleep(2)
    send_gpu_email(current_time, userGpuFileName, scheddGpuFileName, userGpuPrintFileName, scheddGpuPrintFileName)