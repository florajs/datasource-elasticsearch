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
        it('should handle limit', () => {
            const search = createSearchConfig({ esindex: 'fund', limit: 10 });
            expect(search.body).to.have.property('size', 10);
        });

        it('should set fallback if limit is not set', () => {
            const search = createSearchConfig({ esindex: 'fund' });
            expect(search.body).to.have.property('size', 1000000);
        });

        it('should handle unlimited limit', () => {
            const search = createSearchConfig({ esindex: 'fund', limit: 'unlimited' });
            expect(search.body).to.have.property('size', 1000000);
        });

        it('should handle page', () => {
            const search = createSearchConfig({ esindex: 'fund', limit: 10, page: 2 });
            expect(search.body).to.have.property('from', 10);
        });

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
