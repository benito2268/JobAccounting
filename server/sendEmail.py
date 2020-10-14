#!/usr/bin/python

# The following is the email address list for CPU stats
to_cpu_addr = [
    'bbockelman@morgridge.org',
    'ckoch5@wisc.edu',
    'gthain@cs.wisc.edu',
    'jcpatton@wisc.edu',
    'miron@cs.wisc.edu'
]
# The following is the email address list for GPU stats
to_gpu_addr = [
    'AGitter@morgridge.org',
    'bbockelman@morgridge.org',
    'ckoch5@wisc.edu',
    'gthain@cs.wisc.edu',
    'jcpatton@wisc.edu',
    'miron@cs.wisc.edu'
]

# Convert the json file to csv
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

# Shade differnt color for every other row 
def color_tr(s):
    import re
    n = [1]
    def repl(m):
        n[0] += 1
        return '<tr>' if n[0] % 2 else '<tr bgcolor="MistyRose">'
    return re.sub(r'<tr>', repl, s)

# Function to send email for CPU stats
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



  fromaddr = "accounting@chtc.wisc.edu"
  msg = MIMEMultipart()
  msg['From'] = 'accounting@chtc.wisc.edu'
  msg['To'] = ", ".join(to_cpu_addr)
  msg['Subject'] = "CHTC Usage Report for " + current_time

  
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

  for toaddr in to_cpu_addr:
    sendMail(fromaddr, toaddr, msg)  

# Function to send email for GPU stats
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

  fromaddr = "accounting@chtc.wisc.edu"
  msg = MIMEMultipart()
  msg['From'] = 'accounting@chtc.wisc.edu'
  msg['To'] = ", ".join(to_gpu_addr)
  msg['Subject'] = "CHTC GPU Usage Report for " + current_time


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

  for toaddr in to_gpu_addr:
    sendMail(fromaddr, toaddr, msg)
  

def sendMail(sender, recipient, message):
    import dns.resolver
    import smtplib
    
    address = recipient.split('@')
    domain = address[1]
    sent = False
    result = "Error"
    # Try to send mail using each server on recipient domain until one works
    for domain in dns.resolver.query(domain, 'MX'):
        serverDomain = str(domain).split()[1]
        serverDomain = serverDomain[:-1]
        try:
            server = smtplib.SMTP(serverDomain)
            result = server.sendmail(sender, recipient, message.as_string())
            server.quit
            sent = True
            break
        except (BaseException, Exception):
            continue
    if (not sent):
        print("Failed to send to " + recipient)
        print("Error code: " + result)
    else:
        print("Job Accounting report sent to " + recipient)

# Function to remove the json or csv file
def remove_file(file_name):
  import os
  if os.path.exists(file_name):
    os.remove(file_name)
  else:
    print("The file does not exist")

if __name__ == '__main__':
    import time
    from datetime import datetime, timedelta
    import os 
    import sys
    
    
    yest_date = datetime.now() - timedelta(days = 1)

    # Check the Argument
    if len(sys.argv) == 2:
      current_time = sys.argv[1]
      str_arr = current_time.split("-")
      if len(str_arr) == 3:
        if int(str_arr[0]) < 1900 or int(str_arr[0]) > 2200 or int(str_arr[1]) < 1 or int(str_arr[1]) > 12 or int(str_arr[2]) < 1 or int(str_arr[2]) > 31:
          sys.stderr.write("""\n\033[1mERROR: Please check your argument\033[0m\n\n""")
          sys.exit(0)
      else:
        sys.stderr.write("""\n\033[1mERROR: Please check your argument\033[0m\n\n""")
        sys.exit(0)
    elif len(sys.argv) == 1:
      current_time = yest_date.strftime("%Y-%m-%d")
    else:
      sys.stderr.write("""\n\033[1mERROR: Please check your argument\033[0m\n\n""")
      sys.exit(0)
    
    # Run the js script to generate json file from elasticsearch server
    if not os.path.exists("/opt/scripts/JobAccounting/server/data"):
      os.mkdir("/opt/scripts/JobAccounting/server/data")
    os.chdir("/opt/scripts/JobAccounting/server") 
    os.system("/usr/bin/node --max-old-space-size=8192 searchGpu.js " + current_time)
    time.sleep(1)
    os.system("/usr/bin/node --max-old-space-size=8192 searchCpu.js " + current_time)
    time.sleep(1)
    
    # Convert json file to csv
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

    # Send email to the user in email list
    os.chdir("/opt/scripts/JobAccounting/server/data") 
    send_user_email(current_time, userFileName, scheddFileName, userPrintFileName, scheddPrintFileName)
    time.sleep(2)
    send_gpu_email(current_time, userGpuFileName, scheddGpuFileName, userGpuPrintFileName, scheddGpuPrintFileName)
    time.sleep(2)
    
    # Remove unnecessary file
    remove_file(userFileName)
    time.sleep(2)
    remove_file(scheddFileName)
    time.sleep(2)
    remove_file(scheddGpuFileName)
    time.sleep(2)
    remove_file(userGpuFileName)
    time.sleep(2)

    remove_file(userPrintFileName)
    time.sleep(2)
    remove_file(scheddPrintFileName)
    time.sleep(2)
    remove_file(userGpuPrintFileName)
    time.sleep(2)
    remove_file(scheddGpuPrintFileName)

    os.chdir("/opt/scripts/JobAccounting/server") 
    remove_file("scheddGpuPrintStats.json")
    time.sleep(1)
    remove_file("scheddGpuStats.json")
    time.sleep(1)
    remove_file("scheddPrintStats.json")
    time.sleep(1)
    remove_file("scheddStats.json")
    time.sleep(1)

    remove_file("userGpuPrintStats.json")
    time.sleep(1)
    remove_file("userPrintStats.json")
    time.sleep(1)
    remove_file("userGpuStats.json")
    time.sleep(1)
    remove_file("userStats.json")
    time.sleep(1)

    if os.path.exists("/opt/scripts/JobAccounting/server/data"):
      os.rmdir("/opt/scripts/JobAccounting/server/data")
