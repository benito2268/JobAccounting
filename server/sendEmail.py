#!/usr/bin/python

def convertFile(input_Filename, output_Filename):
    
    import json 
    import csv 
    with open(input_Filename) as json_file: 
        input_data = json.load(json_file) 
    output_file = open(output_Filename, 'w') 
    csv_writer = csv.writer(output_file)
    count = 0
    for currObs in input_data: 
        if count == 0: 
            header = list(input_data[currObs].keys())
            if (input_Filename == 'scheddStats.json'):
                header.insert(0, "scheedName")
            else:
                header.insert(0, "userName")
            csv_writer.writerow(header) 
            count += 1
        content = list(input_data[currObs].values())
        content.insert(0, currObs)
        csv_writer.writerow(content) 
    output_file.close() 

def send_email(current_time, userFileName, scheddFileName):
  from email.mime.multipart import MIMEMultipart
  from email.mime.text  import MIMEText
  from email.mime.base import MIMEBase
  from email import encoders
  import smtplib
  



  fromaddr = "chtc.memory@gmail.com"
  toaddr = ['yhan222@wisc.edu', 'hym980321@gmail.com']
  msg = MIMEMultipart()
  msg['From'] = 'UW Madison CHTC Usage Report'
  msg['To'] = ", ".join(toaddr)
  msg['Subject'] = "Test - CHTC Usage Report " + current_time

  body = "Attachment is the csv file for CHTC Usage Report (Test Only)"
  msg.attach(MIMEText(body, 'plain'))

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
      server.sendmail(fromaddr, toaddr, text)

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
    os.system("node searchGpu.js")
    time.sleep(2)
    userFileName = "userStats_" + str(current_time) +".csv"
    scheddFileName = "scheddStats_" + str(current_time) +  ".csv"
    convertFile("scheddStats.json", scheddFileName)
    time.sleep(1)
    convertFile("userStats.json", userFileName)
    send_email(current_time, userFileName, scheddFileName)