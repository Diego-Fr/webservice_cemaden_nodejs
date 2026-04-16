const axios = require('axios');
const HttpsProxyAgent = require('https-proxy-agent');

const proxyAgent = new HttpsProxyAgent('http://10.1.6.20:80');

axios.defaults.httpAgent = proxyAgent;
axios.defaults.httpsAgent = proxyAgent;
axios.defaults.proxy = false; // importantíssimo

module.exports = axios;