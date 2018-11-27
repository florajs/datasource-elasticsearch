/* global describe, it, beforeEach */

'use strict';

const { expect } = require('chai');

const FloraElasticsearch = require('../index');
const createSearchConfig = require('../lib/create-search-config');

const nop = function() {};

const mockLog = {
    info: nop,
    debug: nop,
    trace: nop,
    error: nop,
    warn: nop,
    child: () => mockLog
};

describe('Flora Elasticsearch DataSource', function() {
    let dataSource;
    const api = { log: mockLog };

    beforeEach(function() {
        const cfg = {
            hosts: ['http://example.com/elasticsearch']
        };

        dataSource = new FloraElasticsearch(api, cfg);
    });

    describe('interface', function() {
        it('should export a query function', function() {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', function() {
            expect(dataSource.prepare).to.be.a('function');
        });
    });

    describe('request builder', function() {
        it('should use ids filter for retrieve by id', function() {
            const search = createSearchConfig({
                esindex: 'fund',
                estype: 'fund',
                filter: [
                    [
                        {
                            attribute: '_id',
                            operator: 'equal',
                            value: '119315'
                        }
                    ]
                ]
            });

            const expected = {
                index: 'fund',
                type: 'fund',
                body: {
                    query: {
                        filtered: {
                            filter: {
                                ids: {
                                    values: ['119315']
                                }
                            }
                        }
                    },
                    size: 1000000
                }
            };

            expect(search).to.deep.equal(expected);
        });

        it('should not nest id arrays for retrieve by multiple ids', function() {
            const search = createSearchConfig({
                esindex: 'fund',
                estype: 'fund',
                filter: [
                    [
                        {
                            attribute: '_id',
                            operator: 'equal',
                            value: ['133962', '133963']
                        }
                    ]
                ]
            });

            const expected = {
                index: 'fund',
                type: 'fund',
                body: {
                    query: {
                        filtered: {
                            filter: {
                                ids: {
                                    values: ['133962', '133963']
                                }
                            }
                        }
                    },
                    size: 1000000
                }
            };

            expect(search).to.deep.equal(expected);
        });

        it('should convert an aliased agg', function() {
            const search = createSearchConfig({
                esindex: 'prod',
                estype: 'instrument',
                limit: 0,

                /* countByIssuer=count(limit:20,issuer.name) */
                aggregateTest: [
                    {
                        options: {
                            limit: '20'
                        },
                        fields: ['issuer.name'],
                        aggregate: [],
                        functionName: 'count',
                        alias: 'countByIssuer'
                    }
                ]
            });

            const expected = {
                body: {
                    aggs: {
                        countByIssuer: {
                            terms: {
                                field: 'issuer.name',
                                size: 20
                            }
                        }
                    },
                    size: 0
                },
                index: 'prod',
                search_type: 'count',
                type: 'instrument'
            };

            expect(search).to.deep.equal(expected);
        });
    });
});
