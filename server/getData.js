const esClient = require('./client');
var ss = require('simple-statistics');
var cal = require('mathjs');

let testList;
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
// Check the memory size
const v8 = require('v8');
const totalHeapSize = v8.getHeapStatistics().total_available_size;
const totalHeapSizeGb = (totalHeapSize / 1024 / 1024 / 1024).toFixed(2);
console.log('totalHeapSizeGb: ', totalHeapSizeGb);

async function search(indexName) {
    let response =  await esClient.search({
        index: indexName,
        scroll: "10s",
        size: 10000,
        body: {
            'query': {
                    range : {
                        "CompletionDate" : {
                            "gte" : (new Date(new Date().setDate(new Date().getDate()-1)).setHours(0,0,0,0)) / 1000 ,
                            "lte" : (new Date(new Date().setDate(new Date().getDate()-1)).setHours(23,59,59,00)) / 1000,
                            "boost" : 1.0
                        }
                    }
                } 
        }
    })
    let tempJobList = response.hits.hits;
    // testList = response.hits.hits;
    // console.log(tempJobList);
    let jobListLength = jobList.length + response.hits.total.value;
    for (let curr of tempJobList) {
        jobList.push(curr);
    }
    //jobListLength
    while (jobList.length < jobListLength) {
        
        response = await esClient.scroll({
            scrollId: response._scroll_id,
            scroll: '10s'
        })
        tempJobList = response.hits.hits;
        for (let curr of tempJobList) {
            jobList.push(curr);
        }
       
    }

    
    console.log(indexName,jobList.length)
};

async function runPass() {
    await search('chtc-' + new Date(new Date().setDate(new Date().getDate() - 1)).toISOString().slice(0,10));
    await processResult(jobList);
    await exportResult()
}
runPass()

async function processResult(jobList){
    jobList.forEach(element => {
        let currObs = element._source;
        if (typeof userList[currObs.User] === 'undefined') {
            let content = {};
            let currHour = [];
            let currMemory = [];
            content.CommittedCoreHr = currObs.CommittedCoreHr;
            content.CoreHr = currObs.CoreHr;
            content.Jobs = 1;
            content.MemoryMB = typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB;
            // content.MemoryUsage = typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage;
            content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
            if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                content.ShortJobStarts = 1;
            } else {
                content.ShortJobStarts = 0;
            }
            content.NumJobStarts = currObs.NumJobStarts;
            
            
            content.ScheddName = currObs.ScheddName;
            content.Schedd = currObs.ScheddName.split('.')[1];
            
            userList[currObs.User] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(content.CommittedCoreHr);
            }
            if (typeof currObs.MemoryUsage !== 'undefined') {
                currMemory.push(currObs.MemoryUsage);
            }
            userMemoryList[currObs.User] = currMemory;
            userHourList[currObs.User] = currHour;
        } else {
            let content = userList[currObs.User];
            let currHour = userHourList[currObs.User];
            let currMemory = userMemoryList[currObs.User];
            content.Jobs += 1;
            content.NumJobStarts += currObs.NumJobStarts;
            content.CoreHr += currObs.CoreHr;
            content.CommittedCoreHr += currObs.CommittedCoreHr;
            content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
            // content.MemoryUsage = Math.max(content.MemoryUsage, typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage);
            content.MemoryMB = Math.max(content.MemoryMB, typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB);
            if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                content.ShortJobStarts ++;
            } 
            userList[currObs.User] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(currObs.CommittedCoreHr);
                userHourList[currObs.User] = currHour;
            }
            if (typeof currObs.MemoryUsage !== 'undefined') {
                currMemory.push(currObs.MemoryUsage);
                userMemoryList[currObs.User] = currMemory;
            }


        }
        if (typeof scheddList[currObs.ScheddName] === 'undefined') {
            let content = {};
            let currHour = [];
            let currMemory = [];
            content.CommittedCoreHr = currObs.CommittedCoreHr;
            content.CoreHr = currObs.CoreHr;
            content.Jobs = 1;
            content.MemoryMB = typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB;
            // content.MemoryUsage = typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage;
            content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
            if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                content.ShortJobStarts = 1;
            } else {
                content.ShortJobStarts = 0;
            }
            content.NumJobStarts = currObs.NumJobStarts;
            
            
            scheddList[currObs.ScheddName] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(currObs.CommittedCoreHr);
            }
            scheddHourList[currObs.ScheddName] = currHour;
            if (typeof currObs.MemoryUsage !== 'undefined') {
                currMemory.push(currObs.MemoryUsage);
            }
            scheddMemoryList[currObs.ScheddName] = currMemory;
        } else {
            let content = scheddList[currObs.ScheddName];
            let currHour = scheddHourList[currObs.ScheddName];
            let currMemory = scheddMemoryList[currObs.ScheddName];
            content.Jobs += 1;
            content.NumJobStarts += currObs.NumJobStarts;
            content.CoreHr += currObs.CoreHr;
            content.CommittedCoreHr += currObs.CommittedCoreHr;
            content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
            // content.MemoryUsage = Math.max(content.MemoryUsage, typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage);
            content.MemoryMB = Math.max(content.MemoryMB, typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB);
            if (currObs.CompletionDate - currObs.JobCurrentStartDate < 60) {
                content.ShortJobStarts ++;
            } 
            scheddList[currObs.ScheddName] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(currObs.CommittedCoreHr);
                scheddHourList[currObs.ScheddName] = currHour;
            }  
            if (typeof currObs.MemoryUsage !== 'undefined') {
                currMemory.push(currObs.MemoryUsage);
                scheddMemoryList[currObs.User] = currMemory;
            }
        }
    });
    // Reorder
    
    Object.entries(userList).forEach(([key, value]) => {
        let currUser = {};
        currUser["Completed Hours"] = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        currUser["Used Hours"] = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        currUser["Uniq Job Ids"] = value.Jobs;
        currUser["Request Mem"] = Math.round(value.MemoryMB);
        let currMemory = userMemoryList[key];
        currMemory.sort(function(a,b){return a - b});
        let median_index = Math.floor(currMemory.length / 2);
        currUser["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2) * 100) / 100;
        currUser["Max Mem"] = Math.round(currMemory[currMemory.length - 1] * 100) / 100;
        currUser["Request Cpus"] = value.RequestCpus;
        currUser["ShortJobStarts"] = value.ShortJobStarts;
        currUser["All Starts"] = value.NumJobStarts;
        
        let currHour = userHourList[key];
        currHour.sort(function(a,b){return a - b});
        median_index = Math.floor(currHour.length / 2);
        currUser["Min"]  = Math.round(currHour[0] * 100)/ 100;
        let per25 = (Math.floor(currHour.length*.25) - 1) >= 0 ? Math.floor(currHour.length*.25) - 1 : 0;
        currUser["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        currUser["Median"] = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        
        let per75 = (Math.floor(currHour.length*.75) - 1) >= 0 ? Math.floor(currHour.length*.75) - 1 : 0;
        currUser["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        currUser["Max"] = Math.round(currHour[currHour.length - 1] * 100) / 100;
        currUser["Mean"] = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
        currUser["Std"] = Math.round(cal.std(currHour) * 100) / 100;
        currUser["ScheddName"] = value.ScheddName;
        currUser["Schedd"] = value.Schedd;

        finalUserList[key] = currUser;
    })
    Object.entries(scheddList).forEach(([key, value]) => {

        let currSchedd = {};
        currSchedd["Completed Hours"] = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        currSchedd["Used Hours"] = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        currSchedd["Uniq Job Ids"] = value.Jobs;
        currSchedd["Request Mem"] = Math.round(value.MemoryMB);
        let currMemory = scheddMemoryList[key];
        currMemory.sort(function(a,b){return a - b});
        let median_index = Math.floor(currMemory.length / 2);
        currSchedd["Used Mem"] = Math.round((currMemory.length % 2 !== 0  ? currMemory[median_index] :  (currMemory[median_index - 1] + currMemory[median_index]) / 2) * 100) / 100;
        currSchedd["Max Mem"] = Math.round(currMemory[currMemory.length - 1] * 100) / 100;
        currSchedd["Request Cpus"] = value.RequestCpus;
        currSchedd["ShortJobStarts"] = value.ShortJobStarts;
        currSchedd["All Starts"] = value.NumJobStarts;
        
        let currHour = scheddHourList[key];
        currHour.sort(function(a,b){return a - b});
        median_index = Math.floor(currHour.length / 2);
        currSchedd["Min"]  = Math.round(currHour[0] * 100)/ 100;
        let per25 = (Math.floor(currHour.length*.25) - 1) >= 0 ? Math.floor(currHour.length*.25) - 1 : 0;
        currSchedd["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        currSchedd["Median"] = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        
        let per75 = (Math.floor(currHour.length*.75) - 1) >= 0 ? Math.floor(currHour.length*.75) - 1 : 0;
        currSchedd["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        currSchedd["Max"] = Math.round(currHour[currHour.length - 1] * 100) / 100;
        currSchedd["Mean"] = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
        currSchedd["Std"] = Math.round(cal.std(currHour) * 100) / 100;


        finalScheddList[key] = currSchedd;
    })
   
}

async function exportResult() {
    var fs = require('fs');
    let userFile = JSON.stringify(finalUserList);
    fs.writeFile('userStats.json', userFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });
    let scheddFile = JSON.stringify(finalScheddList);
    fs.writeFile('scheddStats.json', scheddFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });

    // let testfile = JSON.stringify(testList);
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




  

