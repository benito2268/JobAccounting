# Elastic Search Job Accounting

This is a simple problem using Nodejs and python to generate the report for jobs running on CHTC server.

## Installation

In order to run the program, you need to install the latest version of Nodejs and NPM. The following links are the reference for installation:
 -  https://www.tecmint.com/install-nodejs-npm-in-centos-ubuntu/
 -  https://docs.npmjs.com/downloading-and-installing-node-js-and-npm

In addition, you should have a Python version 3.6 or higher


## Usage

In order to generate the reports for a certain date, we could call the python scripts `sendEmail.py` with an additional argument:

**Note**: The argument must be a date format in *YYYY-MM-DD* 

```bash
python sendEmail.py 2020-03-21
```

You can also get the report for the day before by having no additional argument:

```bash
python sendEmail.py
```

## Editing Email List
If you would like to add or remove a user from the email list, please edit the python script `sendEmail.py` at line 3 and line 4
 - to_cpu_addr is the email address list for regular stats
 - to_gpu_addr is the email address list for GPU stats
 

```python
to_cpu_addr = ['chtc@cs.wisc.edu', 'chtc2@cs.wisc.edu',]
to_gpu_addr = ['chtc@cs.wisc.edu', 'chtc2@cs.wisc.edu',]
```

## Schedule Job Running Regularly
In order to schedule the job running regularly, we may need to use the [crontab](https://tecadmin.net/crontab-in-linux-with-20-examples-of-cron-schedule/):

The following is an example of the scripts running every day at 6:58 am.
```bash
crontab -l
58 06 * * * /home/yhan222/miniconda3/bin/python /home/yhan222/nodejs-elasticsearch/server/sendEmail.py
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.