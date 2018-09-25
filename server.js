const config = require('./config.js');
const crawlFrom = require('./crawl.js');
const mariadb = require('mariadb');
const pool = mariadb.createPool({host: config.dbHost, user:config.dbUsername, password: config.dbPassword,
  connectionLimit: config.dbConnectionLimit, database:config.dbDatabaseName});

const HttpProvider = require('ethjs-provider-http');
const PollingBlockTracker = require('eth-block-tracker');

async function getLatestBlock(){
  // get latest block number from the database
  let queryBlockNumber = "SELECT number FROM blocks ORDER BY number DESC LIMIT 1";
  let conn = await pool.getConnection();
  let rows = await conn.query(queryBlockNumber);
  let latestBlockNumber = rows.length > 0 ? rows[0].number : 0;
  return latestBlockNumber;
}

async function runBlockListener(){
  console.log("Server is running...");
  let latestBlockNumber = await getLatestBlock();

  // crawl from that block number
  await crawlFrom(latestBlockNumber+1);

  // listen to the geth
  const provider = new HttpProvider(config.web3Provider);
  const blockTracker = new PollingBlockTracker({ provider })
  blockTracker.on('latest', async () => {
    let latestBlockNumber = await getLatestBlock();
    await crawlFrom(latestBlockNumber+1);
  });
}

runBlockListener();
