/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Market Exchanges
 **/

// Load required modules
var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet);

// Initialize log system
var logSystem = 'market';
require('./exceptionWriter.js')(logSystem);

/**
 * Get market prices
 **/
exports.get = function(exchange, tickers, callback) {
    if (!exchange) { 
        callback('No exchange specified', null);
    }
    exchange = exchange.toLowerCase();

    if (!tickers || tickers.length === 0) {
        callback('No tickers specified', null);
    }

    var marketPrices = [];
    var numTickers = tickers.length;
    var completedFetches = 0;

    getExchangeMarkets(exchange, function(error, marketData) {
        if (!marketData || marketData.length === 0) {
            callback({});
            return ;
        }

        for (var i in tickers) {
            (function(i){
                var pairName = tickers[i];
                var pairParts = pairName.split('-');
                var base = pairParts[0] || null;
                var target = pairParts[1] || null;

                if (!marketData[base]) {
                    completedFetches++;
                    if (completedFetches === numTickers) callback(marketPrices);
                } else {
                    var price = marketData[base][target] || null;
                    if (!price || price === 0) {
                        var cryptonatorBase;
                        if (marketData[base]['BTC']) cryptonatorBase = 'BTC';
                        else if (marketData[base]['ETH']) cryptonatorBase = 'ETH';
                        else if (marketData[base]['LTC']) cryptonatorBase = 'LTC';

                        if (!cryptonatorBase) {
                            completedFetches++;
                            if (completedFetches === numTickers) callback(marketPrices);
                        } else {
                            getExchangePrice("cryptonator", cryptonatorBase, target, function(error, tickerData) {
                                completedFetches++;
                                if (tickerData && tickerData.price) {
                                    marketPrices[i] = {
                                        ticker: pairName,
                                        price: tickerData.price * marketData[base][cryptonatorBase],
                                        source: tickerData.source
                                    };
                                }
                                if (completedFetches === numTickers) callback(marketPrices);
                            });
                        }
                    } else {
                        completedFetches++;
                        marketPrices[i] = { ticker: pairName, price: price, source: exchange };
                        if (completedFetches === numTickers) callback(marketPrices);
                    }
                }
            })(i);
        }
    });
}

/**
 * Get Exchange Market Prices
 **/

var marketRequestsCache = {};

function getExchangeMarkets(exchange, callback) {
    callback = callback || function(){};
    if (!exchange) { 
        callback('No exchange specified', null);
    }
    exchange = exchange.toLowerCase();

    // Return cache if available
    var cacheKey = exchange;
    var currentTimestamp = Date.now() / 1000;

    if (marketRequestsCache[cacheKey] && marketRequestsCache[cacheKey].ts > (currentTimestamp - 60)) {
        callback(null, marketRequestsCache[cacheKey].data);
        return ;
    }

    callback('Exchange not supported: ' + exchange);
}
exports.getExchangeMarkets = getExchangeMarkets;

/**
 * Get Exchange Market Price
 **/

var priceRequestsCache = {};

function getExchangePrice(exchange, base, target, callback) {
    callback = callback || function(){};

    if (!exchange) { 
        callback('No exchange specified');
    }
    else if (!base) {
        callback('No base specified');
    }
    else if (!target) {
        callback('No target specified');
    }

    exchange = exchange.toLowerCase();
    base = base.toUpperCase();
    target = target.toUpperCase();

    // Return cache if available
    var cacheKey = exchange + '-' + base + '-' + target;
    var currentTimestamp = Date.now() / 1000;

    if (priceRequestsCache[cacheKey] && priceRequestsCache[cacheKey].ts > (currentTimestamp - 60)) {
        callback(null, priceRequestsCache[cacheKey].data);
        return ;
    }

    callback('Exchange not supported: ' + exchange);
}
exports.getExchangePrice = getExchangePrice;
