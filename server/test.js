var ss = require('simple-statistics')
let currHour = [1,3,5,7]
let b = ss.quantile(currHour, 0.25);
console.log(b)