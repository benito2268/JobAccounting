const esClient = require('./client');
var ss = require('simple-statistics')
let indexList;
let jobList = [];
let userList = {};
let scheddList = {};
let userHourList = {}
let scheddHourList = {}

async function search(indexName) {
    let response =  await esClient.search({
        index: indexName,
        scroll: "30s",
        size: 10000,
        body: {
            // 'query': {
            //     "match_all": {}
            // } 
            
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
    let jobListLength = response.hits.total.value;
    for (let curr of tempJobList) {
        jobList.push(curr);
    }
    //jobListLength
    while (jobList.length < jobListLength) {
        
        response = await esClient.scroll({
            scrollId: response._scroll_id,
            scroll: '30s'
        })
        tempJobList = response.hits.hits;
        for (let curr of tempJobList) {
            jobList.push(curr);
        }
        console.log(jobList.length)
    }

    processResult(jobList);
    exportResult()

};
search('chtc-' + new Date(new Date().setDate(new Date().getDate()-1)).toISOString().slice(0,10))

async function processResult(jobList){
    jobList.forEach(element => {
        let currObs = element._source;
        if (typeof userList[currObs.User] === 'undefined') {
            let content = {};
            let currHour = [];
            content.Jobs = 1;
            content.NumJobStarts = currObs.NumJobStarts;
            content.CoreHr = currObs.CoreHr;
            content.CommittedCoreHr = currObs.CommittedCoreHr;
            content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
            content.MemoryUsage = typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage;
            content.MemoryMB = typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB;
            content.ScheddName = currObs.ScheddName;
            content.Schedd = currObs.ScheddName.split('.')[1];
            userList[currObs.User] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(content.CommittedCoreHr);
            }
            userHourList[currObs.User] = currHour;
        } else {
            let content = userList[currObs.User];
            let currHour = userHourList[currObs.User];
            content.Jobs += 1;
            content.NumJobStarts += currObs.NumJobStarts;
            content.CoreHr += currObs.CoreHr;
            content.CommittedCoreHr += currObs.CommittedCoreHr;
            content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
            content.MemoryUsage = Math.max(content.MemoryUsage, typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage);
            content.MemoryMB = Math.max(content.MemoryMB, typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB);
            userList[currObs.User] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(currObs.CommittedCoreHr);
                userHourList[currObs.User] = currHour;
            }
        }
        if (typeof scheddList[currObs.ScheddName] === 'undefined') {
            let content = {};
            let currHour = [];
            content.Jobs = 1;
            content.NumJobStarts = currObs.NumJobStarts;
            content.CoreHr = currObs.CoreHr;
            content.CommittedCoreHr = currObs.CommittedCoreHr;
            content.RequestCpus = typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus;
            content.MemoryUsage = typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage;
            content.MemoryMB = typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB;
            scheddList[currObs.ScheddName] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(content.CommittedCoreHr);
            }
            scheddHourList[currObs.ScheddName] = currHour;
        } else {
            let content = scheddList[currObs.ScheddName];
            let currHour = scheddHourList[currObs.ScheddName];
            content.Jobs += 1;
            content.NumJobStarts += currObs.NumJobStarts;
            content.CoreHr += currObs.CoreHr;
            content.CommittedCoreHr += currObs.CommittedCoreHr;
            content.RequestCpus = Math.max(content.RequestCpus, typeof currObs.RequestCpus === 'undefined' ? 0 : currObs.RequestCpus);
            content.MemoryUsage = Math.max(content.MemoryUsage, typeof currObs.MemoryUsage === 'undefined' ? 0 : currObs.MemoryUsage);
            content.MemoryMB = Math.max(content.MemoryMB, typeof currObs.MemoryMB === 'undefined' ? 0 : currObs.MemoryMB);
            scheddList[currObs.ScheddName] = content;
            if (typeof currObs.CommittedCoreHr !== 'undefined') {
                currHour.push(currObs.CommittedCoreHr);
                scheddHourList[currObs.ScheddName] = currHour;
            }
           
        }

    });

    Object.entries(userList).forEach(([key, value]) => {
        value.CoreHr = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        value.CommittedCoreHr = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        value.MemoryMB = Math.round(value.MemoryMB);
        let currHour = userHourList[key];
        currHour.sort();
        let median_index = Math.floor(currHour.length / 2);
        // value.Min = ss.quantile(currHour, 0);;
        // value["25%"] = ss.quantile(currHour, 0.25);
        // value.Median = ss.quantile(currHour, 0.5);
        // value["75%"] = ss.quantile(currHour, 0.75);
        // value.Max = ss.quantile(currHour, 1);
        value.Min = Math.round(currHour[0] * 100)/ 100;
        let per25 =  Math.floor((currHour.length-1)*.25);
        value["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        value.Median = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        let per75 =  Math.floor((currHour.length-1)*.75);
        value["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        value.Max = Math.round(currHour[currHour.length - 1] * 100) / 100;
        value.Mean = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
    })
    Object.entries(scheddList).forEach(([key, value]) => {
        value.CoreHr = Math.round((value.CoreHr + Number.EPSILON) * 100) / 100;
        value.CommittedCoreHr = Math.round((value.CommittedCoreHr + Number.EPSILON) * 100) / 100
        value.MemoryMB = Math.round(value.MemoryMB);
        let currHour = scheddHourList[key];
        currHour.sort();
        let median_index = Math.floor(currHour.length / 2);
        value.Min = Math.round(currHour[0] * 100)/ 100;
        let per25 =  Math.floor((currHour.length-1)*.25);
        value["25%"] =  Math.round(currHour[per25] * 100)/ 100;
        value.Median = Math.round((currHour.length % 2 !== 0  ? currHour[median_index] :  (currHour[median_index - 1] + currHour[median_index]) / 2) * 100) / 100;
        let per75 =  Math.floor((currHour.length-1)*.75);
        value["75%"] =  Math.round(currHour[per75] * 100)/ 100;
        value.Max = Math.round(currHour[currHour.length - 1] * 100) / 100;
        value.Mean = Math.round((value.CommittedCoreHr / currHour.length) * 100 )/100;
    })
   
}

async function exportResult() {
    var fs = require('fs');
    let userFile = JSON.stringify(userList);
    fs.writeFile('userStats.json', userFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });
    let scheddFile = JSON.stringify(scheddList);
    fs.writeFile('scheddStats.json', scheddFile, 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        };
        console.log("File has been created");
    });

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




  

