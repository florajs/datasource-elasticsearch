'use strict';

const createSearchConfig = require('../lib/create-search-config');
const { expect } = require('chai');
const { ImplementationError } = require('@florajs/errors');

describe('create-search-config', () => {
    describe('limit', () => {
        it('should handle numerical limit', () => {
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
    });

    it('should handle page', () => {
        const search = createSearchConfig({ esindex: 'fund', limit: 10, page: 2 });
        expect(search.body).to.have.property('from', 10);
    });

    describe('search', () => {
        it('should handle search terms', () => {
            const { body } = createSearchConfig({ esindex: 'fund', search: 'foo' });

            expect(body.query)
                .to.have.property('multi_match')
                .and.to.eql({
                    type: 'phrase_prefix',
                    query: 'foo',
                    fields: ['_all']
                });
        });

        it('should handle search boost query option', () => {
            const boost = ['name^5', 'seoDescription^4', 'interests.value'];
            const { body } = createSearchConfig({
                esindex: 'fund',
                search: 'foo',
                queryOptions: { boost }
            });

            expect(body.query).to.have.property('multi_match').and.to.eql({
                type: 'phrase_prefix',
                query: 'foo',
                fields: boost
            });
        });

        it('should handle search field_value_factor query option', () => {
            const field_value_factor = { field: 'searchPriority', modifier: 'log1p' };
            const { body } = createSearchConfig({
                esindex: 'fund',
                search: 'foo',
                queryOptions: { field_value_factor }
            });

            expect(body.query)
                .to.have.property('function_score')
                .and.to.eql({
                    query: {
                        multi_match: {
                            type: 'phrase_prefix',
                            query: 'foo',
                            fields: ['_all']
                        }
                    },
                    field_value_factor
                });
        });
    });

    it('should handle request specific Elasticsearch queries', () => {
        const elasticsearchQuery = {
            function_score: {
                functions: [
                    {
                        field_value_factor: {
                            field: 'assetClass.fulltextBoost',
                            missing: 0
                        },
                        weight: 12
                    }
                ]
            },
            boost_mode: 'multiply'
        };
        const { body } = createSearchConfig({ esindex: 'fund', elasticsearchQuery });

        expect(body.query).to.eql(elasticsearchQuery);
    });

    describe('order', () => {
        it('should handle single order criteria', () => {
            const { body } = createSearchConfig({
                esindex: 'fund',
                order: [{ attribute: 'name', direction: 'asc' }]
            });

            expect(body)
                .to.have.property('sort')
                .and.to.eql([{ name: 'asc' }, '_score']);
        });

        it('should handle multiple order criterias', () => {
            const { body } = createSearchConfig({
                esindex: 'fund',
                order: [
                    { attribute: 'name', direction: 'asc' },
                    { attribute: 'performance', direction: 'desc' }
                ]
            });

            expect(body)
                .to.have.property('sort')
                .and.to.eql([{ name: 'asc' }, { performance: 'desc' }, '_score']);
        });

        it('should handle sort maps', () => {
            const { body } = createSearchConfig({
                esindex: 'fund',
                order: [{ attribute: 'name', direction: 'asc' }],
                sortMap: '{"name":"name.raw"}'
            });

            expect(body)
                .to.have.property('sort')
                .and.to.eql([{ 'name.raw': 'asc' }, '_score']);
        });
    });

    it('should combine request specific Elasticsearch queries with filters', () => {
        const elasticsearchQuery = {
            function_score: {
                functions: [
                    {
                        field_value_factor: {
                            field: 'assetClass.fulltextBoost',
                            missing: 0
                        },
                        weight: 12
                    }
                ]
            },
            boost_mode: 'multiply'
        };
        const { body } = createSearchConfig({
            esindex: 'fund',
            filter: [
                [
                    {
                        attribute: '_id',
                        operator: 'equal',
                        value: '119315'
                    }
                ]
            ],
            elasticsearchQuery
        });

        expect(body.query).to.eql({
            bool: {
                must: [
                    {
                        ids: {
                            values: ['119315']
                        }
                    },
                    elasticsearchQuery
                ]
            }
        });
    });

    describe('filter', () => {
        describe('equal', () => {
            it('should use ids filter for retrieve by id', () => {
                const { body } = createSearchConfig({
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

                expect(body)
                    .to.have.property('query')
                    .and.to.eql({
                        ids: {
                            values: ['119315']
                        }
                    });
            });

            it('should not nest id arrays for retrieve by multiple ids', () => {
                const { body } = createSearchConfig({
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

                expect(body)
                    .to.have.property('query')
                    .and.to.eql({
                        ids: {
                            values: ['133962', '133963']
                        }
                    });
            });

            it('should search for single attribute value', () => {
                const { body } = createSearchConfig({
                    esindex: 'fund',
                    filter: [
                        [
                            {
                                attribute: 'attr',
                                operator: 'equal',
                                value: 'foo'
                            }
                        ]
                    ]
                });

                expect(body)
                    .to.have.property('query')
                    .and.to.eql({
                        term: {
                            attr: 'foo'
                        }
                    });
            });

            it('should search for multiple attribute values', () => {
                const { body } = createSearchConfig({
                    esindex: 'fund',
                    filter: [
                        [
                            {
                                attribute: 'attr',
                                operator: 'equal',
                                value: ['foo', 'bar']
                            }
                        ]
                    ]
                });

                expect(body)
                    .to.have.property('query')
                    .and.to.eql({
                        terms: {
                            attr: ['foo', 'bar']
                        }
                    });
            });
        });

        const floraOperators = {
            greater: 'gt',
            greaterOrEqual: 'gte',
            less: 'lt',
            lessOrEqual: 'lte'
        };

        Object.keys(floraOperators).forEach((operator) => {
            const elasticSearchFilterAttr = floraOperators[operator];

            it(`should handle ${operator} filter`, () => {
                const { body } = createSearchConfig({
                    esindex: 'fund',
                    filter: [
                        [
                            {
                                attribute: 'attr',
                                operator,
                                value: 1337
                            }
                        ]
                    ]
                });

                expect(body)
                    .to.have.property('query')
                    .and.to.eql({
                        range: {
                            attr: {
                                [elasticSearchFilterAttr]: 1337
                            }
                        }
                    });
            });
        });

        it('should handle "and" filters', () => {
            const { body } = createSearchConfig({
                esindex: 'fund',
                filter: [
                    [
                        {
                            attribute: '_id',
                            operator: 'equal',
                            value: ['133962', '133963']
                        },
                        {
                            attribute: 'attr',
                            operator: 'lessOrEqual',
                            value: 1
                        }
                    ]
                ]
            });

            expect(body)
                .to.have.property('query')
                .and.to.eql({
                    bool: {
                        must: [{ ids: { values: ['133962', '133963'] } }, { range: { attr: { lte: 1 } } }]
                    }
                });
        });

        it('should handle "or" filters', () => {
            const { body } = createSearchConfig({
                esindex: 'fund',
                filter: [
                    [
                        {
                            attribute: '_id',
                            operator: 'equal',
                            value: ['133962', '133963']
                        },
                        {
                            attribute: 'attr',
                            operator: 'lessOrEqual',
                            value: 1
                        }
                    ],
                    [
                        {
                            attribute: '_id',
                            operator: 'equal',
                            value: ['133964', '133965']
                        },
                        {
                            attribute: 'attr1',
                            operator: 'greater',
                            value: 1
                        }
                    ]
                ]
            });

            expect(body)
                .to.have.property('query')
                .and.to.eql({
                    bool: {
                        should: [
                            {
                                bool: {
                                    must: [{ ids: { values: ['133962', '133963'] } }, { range: { attr: { lte: 1 } } }]
                                }
                            },
                            {
                                bool: {
                                    must: [{ ids: { values: ['133964', '133965'] } }, { range: { attr1: { gt: 1 } } }]
                                }
                            }
                        ]
                    }
                });
        });

        it('should throw an error for unsupported Flora operators', () => {
            expect(() => {
                createSearchConfig({
                    esindex: 'fund',
                    filter: [
                        [
                            {
                                attribute: 'attr',
                                operator: 'between',
                                value: [1, 3]
                            }
                        ]
                    ]
                });
            }).to.throw(ImplementationError, `Operator "between" not implemented`);
        });
    });
});
