'use strict';

const { expect } = require('chai');

const FloraElasticsearch = require('../index');
const createSearchConfig = require('../lib/create-search-config');

const nop = () => {};
const mockLog = {
    info: nop,
    debug: nop,
    trace: nop,
    error: nop,
    warn: nop,
    child: () => mockLog
};

describe('Flora Elasticsearch DataSource', () => {
    let dataSource;
    const api = { log: mockLog };

    beforeEach(() => {
        const cfg = {
            node: 'http://example.com/elasticsearch'
        };

        dataSource = new FloraElasticsearch(api, cfg);
    });

    describe('interface', () => {
        it('should export a query function', () => {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(dataSource.prepare).to.be.a('function');
        });
    });

    describe('request builder', () => {
        it('should use ids filter for retrieve by id', () => {
            const search = createSearchConfig({
                esindex: 'fund',
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

            expect(search).to.deep.equal({
                index: 'fund',
                body: {
                    query: {
                        ids: {
                            values: ['119315']
                        }
                    },
                    size: 1000000
                }
            });
        });

        it('should not nest id arrays for retrieve by multiple ids', () => {
            const search = createSearchConfig({
                esindex: 'fund',
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

            expect(search).to.deep.equal({
                index: 'fund',
                body: {
                    query: {
                        ids: {
                            values: ['133962', '133963']
                        }
                    },
                    size: 1000000
                }
            });
        });

        it('should convert an aliased agg', () => {
            const search = createSearchConfig({
                esindex: 'prod',
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

            expect(search).to.deep.equal({
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
                search_type: 'count'
            });
        });
    });
});
