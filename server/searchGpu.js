const esClient = require('./client');
let indexList;
let jobList = []
let userList = [];
let scheddList = [];

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

};
search('chtc-' + new Date(new Date().setDate(new Date().getDate()-1)).toISOString().slice(0,10))

async function processResult(jobList){
    
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




  

