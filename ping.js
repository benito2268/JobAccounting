// const esClient = require('./client');

// esClient.ping({
// // ping usually has a 3000ms timeout
//     requestTimeout: 1000
// }, function (error) {
//     if (error) {
//         console.trace('elasticsearch cluster is down!');
//     } else {
//         console.log('All is well');
//     }
// });

const Http = XMLHttpRequest();
const url='localhost:9200/chtc_testing-2020-02-04';
Http.open("GET", url);
Http.send();
let test = Http.responseText;

console.log(test);