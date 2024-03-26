'use strict';

const assert = require('node:assert/strict');
const createSearchConfig = require('../lib/create-search-config');

describe('create-search-config', () => {
    const floraRequest = { esindex: 'fund', attributes: ['_id'] };

    describe('limit', () => {
        it('should handle numerical limit', () => {
            const search = createSearchConfig({ ...floraRequest, limit: 10 });

            assert.ok(Object.hasOwn(search.body, 'size'));
            assert.equal(search.body.size, 10);
        });

        it('should set fallback if limit is not set', () => {
            const search = createSearchConfig(floraRequest);

            assert.ok(Object.hasOwn(search.body, 'size'));
            assert.equal(search.body.size, 1000000);
        });

        it('should handle unlimited limit', () => {
            const search = createSearchConfig({ ...floraRequest, limit: 'unlimited' });

            assert.ok(Object.hasOwn(search.body, 'size'));
            assert.equal(search.body.size, 1000000);
        });
    });

    it('should handle page', () => {
        const search = createSearchConfig({ ...floraRequest, limit: 10, page: 2 });

        assert.ok(Object.hasOwn(search.body, 'from'));
        assert.equal(search.body.from, 10);
    });

    describe('search', () => {
        it('should handle search terms', () => {
            const { body } = createSearchConfig({ ...floraRequest, search: 'foo' });

            assert.ok(Object.hasOwn(body.query, 'multi_match'));
            assert.deepEqual(body.query.multi_match, {
                type: 'phrase_prefix',
                query: 'foo',
                fields: ['_all']
            });
        });

        it('should handle search boost query option', () => {
            const boost = ['name^5', 'seoDescription^4', 'interests.value'];
            const { body } = createSearchConfig({
                ...floraRequest,
                search: 'foo',
                queryOptions: { boost }
            });

            assert.ok(Object.hasOwn(body.query, 'multi_match'));
            assert.deepEqual(body.query.multi_match, {
                type: 'phrase_prefix',
                query: 'foo',
                fields: boost
            });
        });

        it('should handle search field_value_factor query option', () => {
            const field_value_factor = { field: 'searchPriority', modifier: 'log1p' };
            const { body } = createSearchConfig({
                ...floraRequest,
                search: 'foo',
                queryOptions: { field_value_factor }
            });

            assert.ok(Object.hasOwn(body.query, 'function_score'));
            assert.deepEqual(body.query.function_score, {
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
        const { body } = createSearchConfig({ ...floraRequest, elasticsearchQuery });

        assert.deepEqual(body.query, elasticsearchQuery);
    });

    describe('order', () => {
        it('should handle single order criteria', () => {
            const { body } = createSearchConfig({
                ...floraRequest,
                order: [{ attribute: 'name', direction: 'asc' }]
            });

            assert.ok(Object.hasOwn(body, 'sort'));
            assert.deepEqual(body.sort, [{ name: 'asc' }, '_score']);
        });

        it('should handle multiple order criterias', () => {
            const { body } = createSearchConfig({
                ...floraRequest,
                order: [
                    { attribute: 'name', direction: 'asc' },
                    { attribute: 'performance', direction: 'desc' }
                ]
            });

            assert.ok(Object.hasOwn(body, 'sort'));
            assert.deepEqual(body.sort, [{ name: 'asc' }, { performance: 'desc' }, '_score']);
        });

        it('should handle sort maps', () => {
            const { body } = createSearchConfig({
                ...floraRequest,
                order: [{ attribute: 'name', direction: 'asc' }],
                sortMap: '{"name":"name.raw"}'
            });

            assert.ok(Object.hasOwn(body, 'sort'));
            assert.deepEqual(body.sort, [{ 'name.raw': 'asc' }, '_score']);
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
            ...floraRequest,
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

        assert.deepEqual(body.query, {
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
                    ...floraRequest,
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

                assert.ok(Object.hasOwn(body, 'query'));
                assert.deepEqual(body.query, {
                    ids: {
                        values: ['119315']
                    }
                });
            });

            it('should not nest id arrays for retrieve by multiple ids', () => {
                const { body } = createSearchConfig({
                    ...floraRequest,
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

                assert.ok(Object.hasOwn(body, 'query'));
                assert.deepEqual(body.query, {
                    ids: {
                        values: ['133962', '133963']
                    }
                });
            });

            it('should search for single attribute value', () => {
                const { body } = createSearchConfig({
                    ...floraRequest,
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

                assert.ok(Object.hasOwn(body, 'query'));
                assert.deepEqual(body.query, {
                    term: {
                        attr: 'foo'
                    }
                });
            });

            it('should search for multiple attribute values', () => {
                const { body } = createSearchConfig({
                    ...floraRequest,
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

                assert.ok(Object.hasOwn(body, 'query'));
                assert.deepEqual(body.query, {
                    terms: {
                        attr: ['foo', 'bar']
                    }
                });
            });
        });

        Object.entries({
            greater: 'gt',
            greaterOrEqual: 'gte',
            less: 'lt',
            lessOrEqual: 'lte'
        }).forEach(([operator, elasticSearchFilterAttr]) => {
            it(`should handle ${operator} filter`, () => {
                const { body } = createSearchConfig({
                    ...floraRequest,
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

                assert.ok(Object.hasOwn(body, 'query'));
                assert.deepEqual(body.query, {
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
                ...floraRequest,
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

            assert.ok(Object.hasOwn(body, 'query'));
            assert.deepEqual(body.query, {
                bool: {
                    must: [{ ids: { values: ['133962', '133963'] } }, { range: { attr: { lte: 1 } } }]
                }
            });
        });

        it('should handle "or" filters', () => {
            const { body } = createSearchConfig({
                ...floraRequest,
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

            assert.ok(Object.hasOwn(body, 'query'));
            assert.deepEqual(body.query, {
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
            assert.throws(
                () => {
                    createSearchConfig({
                        ...floraRequest,
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
                },
                {
                    name: 'ImplementationError',
                    message: `Operator "between" not implemented`
                }
            );
        });
    });

    it('should request required fields only', () => {
        const search = createSearchConfig({ ...floraRequest, attributes: ['_id', 'assetClass.id'] });

        assert.ok(Object.hasOwn(search, '_source'));
        assert.deepEqual(search._source, ['_id', 'assetClass.*']);
    });
});
