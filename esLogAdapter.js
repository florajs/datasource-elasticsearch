/* Creates a prototype for a logger as used by elasticsearch. Directs logs to the provided bunyan logger */
module.exports = {

  createEsLogAdapter: function(log) {
    return function(config) {
      this.error = log.error.bind(log);
      this.warning = log.warn.bind(log);
      this.info = log.info.bind(log);
      this.debug = log.debug.bind(log);
      this.trace = function(method, requestUrl, body, responseBody, responseStatus) {
        return log.trace({
          method: method,
          requestUrl: requestUrl,
          body: body,
          responseBody: responseBody,
          responseStatus: responseStatus
        });
      };
      this.close = function() {};
      return this;
    }
  }
}
