const esClient = require('./client');
var cal = require('mathjs');
let indexList;
let jobList = [];
let userList = {};
let scheddList = {};
let userHourList = {};
let userMemoryList = {};
let scheddHourList = {};
let scheddMemoryList = {};
let finalUserList = {};
let finalScheddList = {};
let removelist = {}

// Check the memory size
    const v8 = require('v8');
    const totalHeapSize = v8.getHeapStatistics().total_available_size;
    const totalHeapSizeGb = (totalHeapSize / 1024 / 1024 / 1024).toFixed(2);
    console.log('totalHeapSizeGb: ', totalHeapSizeGb);

async function search(indexName) {
    let response =  await esClient.search({
        index: indexName,
        scroll: "10s",
        size: 1000,
        body: {
            // 'query': {
            //     "match_all": {}
            // } 
            
            'query': {
                    range : {
                        "CompletionDate" : {
                            "gte" : (new Date(new Date().setDate(new Date().getDate()-1)).setHours(0,0,0,0)) / 1000 ,
                            "lte" : (new Date(new Date().setDate(new Date().getDate()-1)).setHours(23,59,59,00)) / 1000,
                            // "boost" : 1.0
                        }
                    }
                } 
        }
    })
    let tempJobList = response.hits.hits;
    let jobListLength = jobList.length + response.hits.total.value;
    for (let curr of tempJobList) {
        jobList.push(curr);
    }
    //jobListLength
    // console.log(jobListLength)
    while (jobList.length < jobListLength) {
        response = await esClient.scroll({
            scrollId: response._scroll_id,
            scroll: '10s',
        })
        tempJobList = response.hits.hits;
        // jobList = jobList.concat(tempJobList)
        for (let curr of tempJobList) {
            jobList.push(curr);
        }
        // console.log("hello3");
        // tempLength += 1000;
        // console.log("Current Size: " + jobList.length);
    }
    console.log(indexName,jobList.length)

};

async function runPass() {
    await search('chtc-' + new Date(new Date().setDate(new Date().getDate()-1)).toISOString().slice(0,10));
    // console.log(new Date(new Date().setDate(new Date().getDate()-1)).toISOString().slice(0,10));
    // await search('chtc-2020-03-05')
    await processResult(jobList);
    await exportResult()
}
runPass()

function getHours(input_hours) {
    let decimalTime = input_hours * 60 * 60;
    let hours = Math.floor((decimalTime / (60 * 60)));
    decimalTime = decimalTime - (hours * 60 * 60);
    let minutes = Math.floor((decimalTime / 60));
    decimalTime = decimalTime - (minutes * 60);
    let seconds = Math.round(decimalTime);
    if(hours < 10)
    {
        hours = "0" + hours;
    }
    if(minutes < 10)
    {
        minutes = "0" + minutes;
    }
    if(seconds < 10)
    {
        seconds = "0" + seconds;
    }
    return ("" + hours + ":" + minutes);
} 

async function processResult(jobList){
    
    jobList.forEach(element => {
        let currObs = element._source;

        if (typeof currObs.Requestgpus !== 'undefined' && currObs.Requestgpus != 0) {
        // if (true) {
            if (typeof userList[currObs.User] === 'undefined') {
                let content = {};
                let currHour = [];
                let currMemory = [];
                content.CommittedCoreHr = currObs.CommittedCoreHr;
                content.CoreHr = currObs.CoreHr;
                content.Jobs = 1;
                content.RequestMemory = typeof currObs.RequestMemory === 'undefined' ? 0 : currObs.RequestMemory;
                content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
                if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                    content.ShortJobStarts = 1;
                } else {
                    content.ShortJobStarts = 0;
                }
                content.NumJobStarts = currObs.NumJobStarts;
                content.Requestgpus = currObs.Requestgpus;
                content.NumShadowStarts = typeof currObs.NumShadowStarts === 'undefined' ? 0 : currObs.NumShadowStarts;
                
                content.JobGpus =  typeof currObs.JobGpus === 'undefined' ? 0 : currObs.JobGpus;
                // content.GPUsProvisioned = currObs.GPUsProvisioned;
                content.ScheddName = currObs.ScheddName;
                content.Schedd = currObs.ScheddName.split('.')[1];
                
                
                if (typeof currObs.WallClockHr !== 'undefined') {
                    content.WallClockHr = currObs.WallClockHr ;
                    currHour.push(currObs.WallClockHr  );
                    
                }
                if (typeof currObs.MemoryUsage !== 'undefined') {
                    currMemory.push(currObs.MemoryUsage);
                }
                userMemoryList[currObs.User] = currMemory;
                userHourList[currObs.User] = currHour;
                userList[currObs.User] = content;

            } else {
                let content = userList[currObs.User];
                let currHour = userHourList[currObs.User];
                let currMemory = userMemoryList[currObs.User];
                content.Jobs += 1;
                content.NumJobStarts += currObs.NumJobStarts;
                content.CoreHr += currObs.CoreHr;
                content.CommittedCoreHr += currObs.CommittedCoreHr;
                content.Requestgpus = Math.max(content.Requestgpus, currObs.Requestgpus);
                content.JobGpus = Math.max(content.JobGpus, typeof currObs.JobGpus === 'undefined' ? 0 : currObs.JobGpus);
                content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
                content.RequestMemory = Math.max(content.RequestMemory, typeof currObs.RequestMemory === 'undefined' ? 0 : currObs.RequestMemory);
                if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                    content.ShortJobStarts ++;
                } 
                content.NumShadowStarts += typeof currObs.NumShadowStarts === 'undefined' ? 0 : currObs.NumShadowStarts;
                
                if (typeof currObs.WallClockHr !== 'undefined') {
                    content.WallClockHr += currObs.WallClockHr ;
                    currHour.push(currObs.WallClockHr );
                    userHourList[currObs.User] = currHour;
                }
                if (typeof currObs.MemoryUsage !== 'undefined') {
                    currMemory.push(currObs.MemoryUsage);
                    userMemoryList[currObs.User] = currMemory;
                }
                userList[currObs.User] = content;
            }
            if (typeof scheddList[currObs.ScheddName] === 'undefined') {
                let content = {};
                let currHour = [];
                let currMemory = [];
                content.CommittedCoreHr = currObs.CommittedCoreHr;
                content.CoreHr = currObs.CoreHr;
                content.Jobs = 1;
                content.RequestMemory = typeof currObs.RequestMemory === 'undefined' ? 0 : currObs.RequestMemory;
                content.Requestgpus = currObs.Requestgpus;
                content.JobGpus =  typeof currObs.JobGpus === 'undefined' ? 0 : currObs.JobGpus;
                content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
                if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                    content.ShortJobStarts = 1;
                } else {
                    content.ShortJobStarts = 0;
                }
                content.NumJobStarts = currObs.NumJobStarts;
                content.NumShadowStarts = typeof currObs.NumShadowStarts === 'undefined' ? 0 : currObs.NumShadowStarts;
                
                
                if (typeof currObs.WallClockHr !== 'undefined') {
                    content.WallClockHr = currObs.WallClockHr ;
                    currHour.push(currObs.WallClockHr );
                }
                scheddHourList[currObs.ScheddName] = currHour;
                if (typeof currObs.MemoryUsage !== 'undefined') {
                    currMemory.push(currObs.MemoryUsage);
                }
                scheddMemoryList[currObs.ScheddName] = currMemory;
                scheddList[currObs.ScheddName] = content;

            } else {
                let content = scheddList[currObs.ScheddName];
                let currHour = scheddHourList[currObs.ScheddName];
                let currMemory = scheddMemoryList[currObs.ScheddName];
                content.Jobs += 1;
                content.NumJobStarts += currObs.NumJobStarts;
                content.CoreHr += currObs.CoreHr;
                content.CommittedCoreHr += currObs.CommittedCoreHr;
                content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
                content.Requestgpus = Math.max(content.Requestgpus, currObs.Requestgpus);
                content.JobGpus = Math.max(content.JobGpus, typeof currObs.JobGpus === 'undefined' ? 0 : currObs.JobGpus);
                content.RequestMemory = Math.max(content.RequestMemory, typeof currObs.RequestMemory === 'undefined' ? 0 : currObs.RequestMemory);
                if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                    content.ShortJobStarts ++;
                } 
                content.NumShadowStarts  += typeof currObs.NumShadowStarts === 'undefined' ? 0 : currObs.NumShadowStarts;
                
                if (typeof currObs.WallClockHr !== 'undefined') {
                    content.WallClockHr += currObs.WallClockHr ;
                    currHour.push(currObs.WallClockHr );
                    scheddHourList[currObs.ScheddName] = currHour;
                }  
                if (typeof currObs.MemoryUsage !== 'undefined') {
                    currMemory.push(currObs.MemoryUsage);
                    scheddMemoryList[currObs.User] = currMemory;
                }
                scheddList[currObs.ScheddName] = content;
            }
        }
            


    });



    Object.entries(userList).forEach(([key, value]) => {
        let currUser = {};
        // currUser["Completed Hours"] = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        // currUser["Used Hours"] = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        currUser["Completed Hours"] = Math.round(value.CommittedCoreHr);
        currUser["Used Hours"] = Math.round(value.CoreHr);
        currUser["Uniq Job Ids"] = value.Jobs;
        currUser["Request Mem"] = Math.round(value.RequestMemory);
        let currMemory = userMemoryList[key];
        currMemory.sort(function(a,b){return a - b});
        let median_index = Math.floor(currMemory.length / 2);
        // currUser["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2) * 100) / 100;
        // currUser["Max Mem"] = Math.round(currMemory[currMemory.length - 1] * 100) / 100;
        currUser["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2));
        currUser["Max Mem"] = Math.round(currMemory[currMemory.length - 1]);

        currUser["Request Cpus"] = value.RequestCpus;
        currUser["Request Gpus"] = value.Requestgpus;
        currUser["Short Jobs"] = value.ShortJobStarts;
        currUser["All Jobs"] = value.NumJobStarts;
        currUser["NumShadowStarts"] = value.NumShadowStarts;
        let currHour = userHourList[key];
        currHour.sort(function(a,b){return a - b});
        median_index = Math.floor(currHour.length / 2);
        // currUser["Min"]  = Math.round(currHour[0] * 100)/ 100;
        currUser["Min"]  = getHours(currHour[0]);
        let per25 = (Math.floor(currHour.length*.25) - 1) >= 0 ? Math.floor(currHour.length*.25) - 1 : 0;
        // currUser["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        // currUser["Median"] = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        currUser["25%"] = getHours(currHour[per25]);
        currUser["Median"] = getHours((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2));


        let per75 = (Math.floor(currHour.length*.75) - 1) >= 0 ? Math.floor(currHour.length*.75) - 1 : 0;
        // currUser["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        // currUser["Max"] = Math.round(currHour[currHour.length - 1] * 100) / 100;
        // currUser["Mean"] = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
        // currUser["Std"] = Math.round(cal.std(currHour) * 100) / 100;
        currUser["75%"] = getHours(currHour[per75]);
        currUser["Max"] = getHours(currHour[currHour.length - 1]);
        currUser["Mean"] = getHours((value.WallClockHr / currHour.length));
        currUser["Std"] = getHours(cal.std(currHour));
        currUser["ScheddName"] = value.ScheddName;
        currUser["Schedd"] = value.Schedd;

        finalUserList[key] = currUser;
    })
    Object.entries(scheddList).forEach(([key, value]) => {
        let currSchedd = {};
        // currSchedd["Completed Hours"] = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        // currSchedd["Used Hours"] = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        currSchedd["Completed Hours"] = Math.round(value.CommittedCoreHr);
        currSchedd["Used Hours"] = Math.round(value.CoreHr);
        currSchedd["Uniq Job Ids"] = value.Jobs;
        currSchedd["Request Mem"] = Math.round(value.RequestMemory);
        let currMemory = scheddMemoryList[key];
        currMemory.sort(function(a,b){return a - b});
        let median_index = Math.floor(currMemory.length / 2);
        // currSchedd["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2) * 100) / 100;
        // currSchedd["Max Mem"] = Math.round(currMemory[currMemory.length - 1] * 100) / 100;
        currSchedd["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2));
        currSchedd["Max Mem"] = Math.round(currMemory[currMemory.length - 1]);

        currSchedd["Request Cpus"] = value.RequestCpus;
        currSchedd["Short Jobs"] = value.ShortJobStarts;
        currSchedd["All Jobs"] = value.NumJobStarts;
        currSchedd["NumShadowStarts"] = value.NumShadowStarts;
        currSchedd["Request Gpus"] = value.Requestgpus;
        // currSchedd["Gpus Usage"] = value.JobGpus;
        
        let currHour = scheddHourList[key];
        currHour.sort(function(a,b){return a - b});
        median_index = Math.floor(currHour.length / 2);
        // currSchedd["Min"]  = Math.round(currHour[0] * 100)/ 100;
        currSchedd["Min"]  = getHours(currHour[0]);
        let per25 = (Math.floor(currHour.length*.25) - 1) >= 0 ? Math.floor(currHour.length*.25) - 1 : 0;
        // currSchedd["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        // currSchedd["Median"] = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        currSchedd["25%"] = getHours(currHour[per25]);
        currSchedd["Median"] = getHours((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2));
        let per75 = (Math.floor(currHour.length*.75) - 1) >= 0 ? Math.floor(currHour.length*.75) - 1 : 0;
        // currSchedd["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        // currSchedd["Max"] = Math.round(currHour[currHour.length - 1] * 100) / 100;
        // currSchedd["Mean"] = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
        // currSchedd["Std"] = Math.round(cal.std(currHour) * 100) / 100;
        currSchedd["75%"] = getHours(currHour[per75]);
        currSchedd["Max"] = getHours(currHour[currHour.length - 1]);
        currSchedd["Mean"] = getHours((value.WallClockHr / currHour.length));
        currSchedd["Std"] = getHours(cal.std(currHour));

        finalScheddList[key] = currSchedd;
    })

   
}

async function exportResult() {
    var fs = require('fs');
    let userFile = JSON.stringify(finalUserList);
    // fs.writeFileSync('userGpuStats.json', userFile);
    fs.writeFile('userGpuStats.json', userFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });
    let scheddFile = JSON.stringify(finalScheddList);
    fs.writeFile('scheddGpuStats.json', scheddFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });
    // let testfile = JSON.stringify(removelist);
    // fs.writeFile('userList.json', testfile, 'utf8', (err) => {
    //     if (err) {
    //         console.error(err);
    //         return;
    //     };
    //     console.log("File has been created");
    // });

}

//Get all the indices
async function indices() {
    indexList = await esClient.indices.stats({
        index: 'chtc-2020-02-18', 
        format: 'json'
    })
    // .then(reuslt => console.log(reuslt))
    // .catch(err => console.error(`Error connecting to the es client: ${err}`));
    console.log(indexList);
};
// indices();




  

