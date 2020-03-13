const esClient = require('./client');
var ss = require('simple-statistics');
var cal = require('mathjs');

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
            // 'query': {
            //         User: {
            //             quote: 'bbee@bmrb.wisc.edu'
            //         },
            //         range : {
            //                         "CompletionDate" : {
            //                             "gte" : (new Date(new Date().setDate(new Date().getDate()-2)).setHours(0,0,0,0)) / 1000 ,
            //                             "lte" : (new Date(new Date().setDate(new Date().getDate()-2)).setHours(23,59,59,00)) / 1000,
            //                             "boost" : 1.0
            //                         }
            //                     }
            //     } 

            'query': {
                    range : {
                        "CompletionDate" : {
                            "gte" : (new Date(new Date().setDate('February 26, 2020')).setHours(0,0,0,0)) / 1000 ,
                            "lte" : (new Date(new Date().setDate('February 26, 2020')).setHours(23,59,59,00)) / 1000,
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

    

};

async function runPass() {
    await search('chtc-2020-02-26');
    // await search('chtc-2020-02-27');
    // await search('chtc-2020-02-28');
    // await search('chtc-' + new Date(new Date().setDate(new Date().getDate()-2)).toISOString().slice(0,10));
    // await search('chtc-' + new Date(new Date().setDate(new Date().getDate()-3)).toISOString().slice(0,10));
    // await processResult(jobList);
    // await exportResult()
}
runPass()



  

