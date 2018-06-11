const express = require('express');
const mysql = require('./db/database');
const bodyParser = require('body-parser');
const crawl = require('./crawler/crawl');
const algo = require('./algo/algo');
const database = require('./db/database');

const app = express();
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST', 'GET');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Access-Control,Allow-Headers, Authorization, X-Requested-With');
  next();
});

mysql.init();

app.use(bodyParser.json());

app.set('port', 9899);
app.listen(app.get('port'), () => console.log("Conneted " + app.get('port') + " port"));

app.use('/crawl', crawl);
app.use('/algo', algo);

process.on('exit', () => database.disconnect());