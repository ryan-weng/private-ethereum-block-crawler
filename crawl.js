const config = require('./config.js');
const Web3 = require('web3');
const web3 = new Web3(config.web3Provider);
const mariadb = require('mariadb');
const pool = mariadb.createPool({host: config.dbHost, user:config.dbUsername, password: config.dbPassword,
  connectionLimit: config.dbConnectionLimit, database:config.dbDatabaseName});

async function execute(startingBlockNumber = 1){
  let blockNumber = await web3.eth.getBlockNumber();
  if (startingBlockNumber < blockNumber){
    let conn = await pool.getConnection();
    let queryBlock = "INSERT INTO blocks (blockHash, difficulty, extraData, gasLimit, gasUsed, logsBloom, miner, mixHash, nonce,"+
        " number, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, timestamp, totalDifficulty, transactionsRoot)"+
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    let queryTransaction = "INSERT INTO transactions (blockHash, blockNumber, contractAddress, cumulativeGasUsed, gasUsed,"+
        " `from`, `to`, logsBloom, transactionHash, transactionIndex, gas, gasPrice, input, nonce, `value`, v, r, s, `status`, root)"+
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    for (var i = startingBlockNumber; i <= blockNumber; i++){
      // get block information
      let blockInformation = await web3.eth.getBlock(i,true);
      let { hash, difficulty, extraData, gasLimit, gasUsed, logsBloom, miner, mixHash, nonce, number, parentHash,
       receiptsRoot, sha3Uncles, size, stateRoot, timestamp, totalDifficulty, transactionsRoot } = blockInformation;
      await conn.query(queryBlock,[hash, difficulty, extraData, gasLimit, gasUsed, logsBloom, miner, mixHash, nonce, number,
        parentHash, receiptsRoot, sha3Uncles, size, stateRoot, timestamp, totalDifficulty, transactionsRoot]);
      console.log("Block No.",i," inserted");

      // get transactions count inside the block
      let transactionsList = blockInformation.transactions;

      // get each transaction information
      for (var j = 0; j < transactionsList.length; j++){
        let transactionDetail = transactionsList[j];
        let { hash, blockHash, blockNumber, gas, gasPrice, input, nonce, value, v, r, s } = transactionDetail;
        let transactionReceipt = await web3.eth.getTransactionReceipt(hash);

        let { contractAddress, cumulativeGasUsed, gasUsed, from, to, logsBloom, transactionHash,
          transactionIndex, status, root } = transactionReceipt;
        // check if transaction receipt has status or root
        status = status ? status : "null";
        root = root ? root : "null";

        await conn.query(queryTransaction, [blockHash, blockNumber, contractAddress, cumulativeGasUsed, gasUsed,
          from, to, logsBloom, transactionHash, transactionIndex, gas, gasPrice, input, nonce, value, v, r,
          s, status, root]);
        console.log("Transaction No.",j,"inserted");
      }
    }
  }
}

module.exports = execute;
