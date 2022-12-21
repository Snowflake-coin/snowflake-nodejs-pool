/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
var fs = require('fs');
var async = require('async');
const util = require('util');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
var notifications = require('./notifications.js');
var utils = require('./utils.js');

// Initialize log system
var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

// Initialize Payment ID and Priority
if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;


function openWallet(){
    if (config.wallet.api){
        params = {
          "daemonHost": config.daemon.host,
          "daemonPort": config.daemon.port,
          "filename": config.wallet.file,
          "password": config.wallet.secret
        };

        apiInterfaces.rpcWallet("/wallet/open", params, function(error, result) {
            if (error){
                log('error', logSystem, 'Error with %s API request to wallet-api %j', ["walletOpen", error]);
                cback(false);
                return;
            } else {
                log('info', logSystem, 'Pool Wallet is now Open');
            }
        });
    }
}

function runInterval(){
    async.waterfall([

        // Get worker keys
        function(callback){
            redisClient.keys(config.coin + ':workers:*', function(error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                callback(null, result);
            });
        },

        // Get worker balances
        function(keys, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'balance'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }

                var balances = {};
                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
                    var workerId = parts[parts.length - 1];

                    balances[workerId] = parseInt(replies[i]) || 0;
                }
                callback(null, keys, balances);
            });
        },

        // Get worker minimum payout
        function(keys, balances, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'minPayoutLevel'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting minimum payout from redis %j', [error]);
                    callback(true);
                    return;
                }

                var minPayoutLevel = {};

                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
                    var workerId = parts[parts.length - 1];

                    var minLevel = config.payments.minPayment;
                    var maxLevel = config.payments.maxPayment;
                    var defaultLevel = minLevel;

                    var payoutLevel = parseInt(replies[i]) || minLevel;
                    if (payoutLevel < minLevel) payoutLevel = minLevel;
                    if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
                    minPayoutLevel[workerId] = payoutLevel;


                    if (payoutLevel !== defaultLevel) {
                        log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [ utils.getReadableCoins(minPayoutLevel[workerId]), workerId, utils.getReadableCoins(defaultLevel) ]);
                    }
                }
                callback(null, balances, minPayoutLevel);
            });
        },

        // calculate the worker fees
        async function(balances, minPayoutLevel, callback){
            var workerFee = {};

            for (var worker in balances){
                if(config.payments.minerPayFee){
                    // Calculate the fee per worker
                    if (config.wallet.api) {
                        var address = worker;
                        var addr = address.split(config.poolServer.paymentId.addressSeparator);
                        if (addr.length >= 2) address = addr[0];
                        if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
                            var addr = address.split(config.poolServer.fixedDiff.addressSeparator);
                            if (addr.length >= 2) address = addr[0];
                        }

                        if (balances[worker] > config.payments.minPayment) {
                            rpcCommand = "/transactions/prepare/basic";
                            prepBasic = {
                                'destination': address,
                                'amount': balances[worker],
                                'paymentID': ''
                            };
                            const feeCalc = util.promisify(apiInterfaces.rpcWallet);
                            let result = await feeCalc(rpcCommand, prepBasic);
                                try {
                                    workerFee[worker] = result.fee;
//                                    // log('info', logSystem, '-- Amount before fee = %s', [balances[worker]]);
                                    balances[worker] -= workerFee[worker]
//                                    // log('info', logSystem, '-- Miner %s pays fee of %s', [worker, workerFee[worker]]);
//                                    // log('info', logSystem, '-- Amount after fee  = %s', [balances[worker]]);
                                } catch {
                                    log('error', logSystem, '** Error with transPrep request to wallet daemon %j error is %s', [prepBasic, error]);
                                    workerFee[worker] = config.payments.transferFee;
                                }
                        } else {
                            workerFee[worker] = 0;
                        }
                    } else {
                        // we want the miner to pay a static fee not dynamically calculated without wallet-api
                        workerFee[worker] = config.payments.transferFee;
                        balances[worker] -= config.payments.transferFee;
                    }
                } else {
                    workerFee[worker] = 0;
                }
            };
            callback(null, balances, minPayoutLevel, workerFee);
        },

        // Filter workers under balance threshold for payment
        function(balances, minPayoutLevel, workerFee, callback){
            var payments = {};

            for (var worker in balances){
                var balance = balances[worker];
                if (balance >= minPayoutLevel[worker]){
                    var remainder = balance % config.payments.denomination;
                    var payout = balance - remainder;

                    if (payout < 0) continue;

                    payments[worker] = payout;
                }
            }

            if (Object.keys(payments).length === 0){
                log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
                callback(true);
                return;
            }

            var transferCommands = [];
            var addresses = 0;
            var commandAmount = 0;
            var commandIndex = 0;

            for (var worker in payments){
                var amount = parseInt(payments[worker]);
                if(config.payments.maxTransactionAmount && amount + commandAmount > config.payments.maxTransactionAmount) {
                    amount = config.payments.maxTransactionAmount - commandAmount;
                }

                var address = worker;
                var payment_id = null;

                var with_payment_id = false;

                var addr = address.split(config.poolServer.paymentId.addressSeparator);
                if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2){
                    with_payment_id = true;
                    if (addr.length >= 2){
                        address = addr[0];
                        payment_id = addr[1];
                        payment_id = payment_id.replace(/[^A-Za-z0-9]/g,'');
                        if (payment_id.length !== 16 && payment_id.length !== 64) {
                            with_payment_id = false;
                            payment_id = null;
                        }
                    }
                    if (addresses > 0){
                        commandIndex++;
                        addresses = 0;
                        commandAmount = 0;
                    }
                }

                if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
                    var addr = address.split(config.poolServer.fixedDiff.addressSeparator);
                    if (addr.length >= 2) address = addr[0];
                }

                if(!transferCommands[commandIndex]) {
                    transferCommands[commandIndex] = {
                        redis: [],
                        amount : 0,
                        rpc: {
                            destinations: [],
                            fee: 0,
                            mixin: config.payments.mixin,
                            priority: config.payments.priority,
                            unlock_time: 0
                        }
                    };
                }

//                log('info', logSystem, '-- Values pushed to redis %j with fee of %s', [{address: address, amount: amount}, workerFee[worker]]);
                transferCommands[commandIndex].rpc.destinations.push({amount: amount, address: address});
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
                if (workerFee[worker] > 0) {
                    transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -workerFee[worker]]);
                }
                transferCommands[commandIndex].amount += amount;
                if (payment_id) transferCommands[commandIndex].rpc.payment_id = payment_id;

                addresses++;
                commandAmount += amount;

                if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
                    commandIndex++;
                    addresses = 0;
                    commandAmount = 0;
                }
            }

            var timeOffset = 0;
            var notify_miners = [];

            var daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";

            async.filter(transferCommands, function(transferCmd, cback){
                var rpcCommand = "transfer";
                var rpcRequest = transferCmd.rpc;

                if (daemonType === "bytecoin" || daemonType === "snowflake") {
                    if (config.wallet.api) {
                        rpcCommand = "/transactions/send/advanced";
                        rpcRequest = {
                            destinations: transferCmd.rpc.destinations,
                            mixin: transferCmd.rpc.mixin,
                            unlockTime: transferCmd.rpc.unlock_time
                        };
                    }
                    else {
                        rpcCommand = "sendTransaction";
                        rpcRequest = {
                            transfers: transferCmd.rpc.destinations,
                            anonymity: transferCmd.rpc.mixin,
                            unlockTime: transferCmd.rpc.unlock_time
                        };
                    }
                    if (transferCmd.rpc.payment_id) {
                        rpcRequest.paymentId = transferCmd.rpc.payment_id;
                    }
                }

                apiInterfaces.rpcWallet(rpcCommand, rpcRequest, function(error, result){
                    if (error){
                        log('error', logSystem, 'Error with %s RPC request to wallet daemon %j', [rpcCommand, error]);
                        log('error', logSystem, 'Payments failed to send to %j', transferCmd.rpc.destinations);
                        callback(false);
                        return;
                    }

                    var now = (timeOffset++) + Date.now() / 1000 | 0;
                    var txHash = (daemonType === "bytecoin" || daemonType === "snowflake") ? result.transactionHash : result.tx_hash;
                    txHash = txHash.replace('<', '').replace('>', '');
                    var txFeePaid = result.fee;

                    transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                        txHash,
                        transferCmd.amount,
                        txFeePaid,
                        transferCmd.rpc.mixin,
                        Object.keys(transferCmd.rpc.destinations).length
                    ].join(':')]);

                    var notify_miners_on_success = [];
                    for (var i = 0; i < transferCmd.rpc.destinations.length; i++){
                        var destination = transferCmd.rpc.destinations[i];
                        if (transferCmd.rpc.payment_id){
                            destination.address += config.poolServer.paymentId.addressSeparator + transferCmd.rpc.payment_id;
                        }
                        transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                            txHash,
                            destination.amount,
                            txFeePaid,
                            transferCmd.rpc.mixin
                        ].join(':')]);

                        notify_miners_on_success.push(destination);
                    }

                    log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                    redisClient.multi(transferCmd.redis).exec(function(error, replies){
                        if (error){
                            log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                            log('error', logSystem, 'Double payments likely to be sent to %j', transferCmd.rpc.destinations);
                            cback(false);
                            return;
                        }

                        for (var m in notify_miners_on_success) {
                            notify_miners.push(notify_miners_on_success[m]);
                        }

                        cback(true);
                    });
                });
            }, function(succeeded){
                var failedAmount = transferCommands.length - succeeded.length;

                for (var m in notify_miners) {
                    var notify = notify_miners[m];
                    log('info', logSystem, '- Miner - %s --> Payment Amount %s - Fee %s', [ notify.address, utils.getReadableCoins(notify.amount), utils.getReadableCoins(workerFee[notify.address]) ]);
                    notifications.sendToMiner(notify.address, 'payment', {
                        'ADDRESS': notify.address.substring(0,7)+'...'+notify.address.substring(notify.address.length-7),
                        'AMOUNT': utils.getReadableCoins(notify.amount),
		            });
                }
                log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);

                callback(null);
            });

        }

    ], function(error, result){
        setTimeout(runInterval, config.payments.interval * 1000);
    });
}

/**
 * Run payments processor
 **/

log('info', logSystem, 'Payment Processor Started');

// Open wallet-api wallet
openWallet();

// Run main Payment Processor after 5 second wait
setTimeout(runInterval, 5 * 1000);

