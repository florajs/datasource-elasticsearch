'use strict';

var expect = require('chai').expect,
    _ = require('lodash'),
    FloraElasticsearch = require('../index'),
    nop = function() {},
    mockLog = {
        info: nop,
        debug: nop,
        trace: nop,
        error: nop,
        warn: nop
    };

describe('Flora Elasticsearch DataSource', function () {
    var dataSource;
    var api = {
        log: mockLog
    };

    beforeEach(function () {
        var cfg = {
            hosts: ['http://example.com/elasticsearch']
        };

        dataSource = new FloraElasticsearch(api, cfg);
    });

    describe('interface', function () {
        it('should export a query function', function () {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', function () {
            expect(dataSource.prepare).to.be.a('function');
        });
    });

    describe('request builder', function () {
        it('should use ids filter for retrieve by id', function() {
            var search = dataSource.createSearchConfig({
                esindex: 'fund',
                estype: 'fund',
                filter: [[
                    {
                        "attribute": "_id",
                        "operator": "equal",
                        "value": "119315"
                    }
                ]]
            });

            var expected = {
                "index": "fund",
                "type": "fund",
                "body": {
                    "query": {
                        "filtered": {
                            "filter": {
                                "ids": {
                                    "values": ["119315"]
                                }
                            }
                        }
                    },
                    "size": 1000000
                }
            };

            expect(search).to.deep.equal(expected);
        });

        it('should convert an aliased agg', function() {
            var search = dataSource.createSearchConfig({
                esindex: 'prod',
                estype: 'instrument',
                limit: 0,

                /* countByIssuer=count(limit:20,issuer.name) */
                aggregateTest: [
                   {
                      "options": {
                         "limit": "20"
                      },
                      "fields": [
                         "issuer.name"
                      ],
                      "aggregate": [],
                      "functionName": "count",
                      "alias": "countByIssuer"
                   }
                ]
            });

            var expected = {
                "body": {
                  "aggs": {
                    "countByIssuer": {
                      "terms": {
                        "field": "issuer.name",
                        "size": 20
                      }
                    }
                  },
                  "size": 0
                },
                "index": "prod",
                "search_type": "count",
                "type": "instrument"
            };

            expect(search).to.deep.equal(expected);
        })
    });

});
