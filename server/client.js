const es = require('elasticsearch');
const esClientHost = new es.Client({
    host: 'localhost:9200',
    // log: 'trace'
});


module.exports = esClientHost;