#!/usr/bin/python

def convertFile(input_Filename, output_Filename):
    import json 
    import csv 
    with open(input_Filename) as json_file: 
        input_data = json.load(json_file) 
    output_file = open("./data/" + output_Filename, 'w') 
    csv_writer = csv.writer(output_file)
    count = 0
    if len(input_data) == 0:
        if (input_Filename == 'scheddStats.json' or input_Filename == 'scheddGpuStats.json'):
            headline = ["Schedd","Completed Hours","Used Hours","Uniq Job Ids","Request Mem","Used Mem","Max Mem","Request Cpus","ShortJobStarts","All Starts","Request Gpus","Gpus Usage","Min","25%","Median","75%","Max","Mean","Std"]
        else:
            headline = ["User","Completed Hours","Used Hours","Uniq Job Ids","Request Mem","Used Mem","Max Mem","Request Cpus","ShortJobStarts","All Starts","Request Gpus","Gpus Usage","Min","25%","Median","75%","Max","Mean","Std", "ScheddName","Schedd"]
        csv_writer.writerow(headline) 
    for currObs in input_data: 
        if count == 0: 
            header = list(input_data[currObs].keys())
            if (input_Filename == 'scheddStats.json' or input_Filename == 'scheddGpuStats.json'):
                header.insert(0, "Schedd")
            else:
                header.insert(0, "User")
            csv_writer.writerow(header) 
            count += 1
        content = list(input_data[currObs].values())
        content.insert(0, currObs)
        csv_writer.writerow(content) 
    output_file.close() 

def send_email(current_time, userFileName, scheddFileName, userGpuFileName, scheddGpuFileName):
  from email.mime.multipart import MIMEMultipart
  from email.mime.text  import MIMEText
  from email.mime.base import MIMEBase
  from email import encoders
  import smtplib
  



  fromaddr = "chtc.memory@gmail.com"
  toaddr = ['yhan222@wisc.edu', 'ckoch5@wisc.edu', 'gthain@cs.wisc.edu', 'bbockelman@morgridge.org']
#   toaddr = ['yhan222@wisc.edu']
  msg = MIMEMultipart()
  msg['From'] = 'UW Madison CHTC Usage Report'
  msg['To'] = ", ".join(toaddr)
  msg['Subject'] = "Test - CHTC Usage and GPU Report " + current_time

  body = "Attachment are the csv file for CHTC Usage and GPU Report <b>(Test Only)</b> <br> The reports traverse for the last three day's indices with the completion date equal to " + current_time
  msg.attach(MIMEText(str(body), 'html'))

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
    os.system("~/bin/node searchGpu.js")
    time.sleep(1)
    os.system("~/bin/node getData.js")
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
    os.chdir("/home/yhan222/nodejs-elasticsearch/server/data") 
    send_email(current_time, userFileName, scheddFileName, userGpuFileName, scheddGpuFileName)