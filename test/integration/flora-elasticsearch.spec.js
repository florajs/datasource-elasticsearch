'use strict';

const { describe, it, before, beforeEach, after } = require('node:test');
const assert = require('node:assert/strict');

const FloraElasticsearch = require('../../index');
const mockLog = require('../mock-log');

const api = { log: mockLog };

const NODE = process.env.ES_URL || 'http://localhost:9200';
const INDEX = 'marvel';
const DOCUMENT_COUNT = 15; // more than the limit tested in "caps the number of returned documents at the given limit"

describe(
    'Flora Elasticsearch DataSource (integration)',
    { skip: !URL.canParse(NODE) && `ES_URL is not a valid URL: "${NODE}"` },
    () => {
        let dataSource;

        before(async () => {
            dataSource = new FloraElasticsearch(api, { node: NODE });

            await fetch(`${NODE}/${INDEX}`, { method: 'PUT', signal: AbortSignal.timeout(5000) });

            const bulkResponse = await fetch(`${NODE}/${INDEX}/_bulk?refresh=true`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-ndjson' },
                body: [
                    { _id: '1', name: 'Captain America', id: 1, team: { id: 1, name: 'Avengers' } },
                    { _id: '2', name: 'Iron Man', id: 2, team: { id: 1, name: 'Avengers' } },
                    { _id: '3', name: 'Magneto', id: 3, team: { id: 2, name: 'Brotherhood' } },
                    { _id: '4', name: 'Thor', id: 4, team: { id: 1, name: 'Avengers' } },
                    { _id: '5', name: 'Black Widow', id: 5, team: { id: 1, name: 'Avengers' } },
                    { _id: '6', name: 'Hawkeye', id: 6, team: { id: 1, name: 'Avengers' } },
                    { _id: '7', name: 'Wolverine', id: 7, team: { id: 3, name: 'X-Men' } },
                    { _id: '8', name: 'Cyclops', id: 8, team: { id: 3, name: 'X-Men' } },
                    { _id: '9', name: 'Storm', id: 9, team: { id: 3, name: 'X-Men' } },
                    { _id: '10', name: 'Mister Fantastic', id: 10, team: { id: 4, name: 'Fantastic Four' } },
                    { _id: '11', name: 'Invisible Woman', id: 11, team: { id: 4, name: 'Fantastic Four' } },
                    { _id: '12', name: 'Human Torch', id: 12, team: { id: 4, name: 'Fantastic Four' } },
                    { _id: '13', name: 'Star-Lord', id: 13, team: { id: 5, name: 'Guardians of the Galaxy' } },
                    { _id: '14', name: 'Gamora', id: 14, team: { id: 5, name: 'Guardians of the Galaxy' } },
                    { _id: '15', name: 'Rocket Raccoon', id: 15, team: { id: 5, name: 'Guardians of the Galaxy' } }
                ]
                    .map(({ _id, ...doc }) => `${JSON.stringify({ index: { _id } })}\n${JSON.stringify(doc)}\n`)
                    .join(''),
                signal: AbortSignal.timeout(10000)
            });
            assert.ok(bulkResponse.ok, `seeding failed: ${await bulkResponse.text()}`);
        });

        after(async () => {
            await fetch(`${NODE}/${INDEX}`, { method: 'DELETE', signal: AbortSignal.timeout(5000) });
        });

        it('propagates a non-existent index as an unwrapped 404, not a RequestError', async () => {
            await assert.rejects(
                () => dataSource.process({ esindex: 'does-not-exist', attributes: ['_id'] }),
                (err) => {
                    assert.notEqual(err.name, 'RequestError');
                    assert.equal(err.meta.statusCode, 404);
                    return true;
                }
            );
        });

        it('caps the number of returned documents at the given limit', async () => {
            const { data, totalCount } = await dataSource.process({ esindex: INDEX, limit: 12, attributes: ['_id'] });

            assert.equal(totalCount, DOCUMENT_COUNT);
            assert.equal(data.length, 12);
        });

        describe('filters', () => {
            describe('operators', () => {
                Object.entries({
                    'equal (single)': { operator: 'equal', value: 2, expected: ['Iron Man'] },
                    'equal (multiple)': { operator: 'equal', value: [1, 2], expected: ['Captain America', 'Iron Man'] },
                    greater: { operator: 'greater', value: 13, expected: ['Gamora', 'Rocket Raccoon'] },
                    greaterOrEqual: {
                        operator: 'greaterOrEqual',
                        value: 13,
                        expected: ['Gamora', 'Rocket Raccoon', 'Star-Lord']
                    },
                    less: { operator: 'less', value: 3, expected: ['Captain America', 'Iron Man'] },
                    lessOrEqual: {
                        operator: 'lessOrEqual',
                        value: 3,
                        expected: ['Captain America', 'Iron Man', 'Magneto']
                    }
                }).forEach(([operatorDescription, { operator, value, expected: expectedNames }]) => {
                    it(`filters by ${operatorDescription}`, async () => {
                        const { data } = await dataSource.process({
                            esindex: INDEX,
                            attributes: ['_id', 'name'],
                            filter: [[{ attribute: 'id', operator, value }]]
                        });

                        const names = data.map((doc) => doc.name).sort();
                        assert.deepEqual(names, expectedNames);
                    });
                });
            });

            it('intersects multiple id filters', async () => {
                /* both conditions must be in the same AND-group (inner array) - two separate
                   OR-groups would union the id sets instead of intersecting them */
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id'],
                    filter: [
                        [
                            { attribute: '_id', operator: 'equal', value: [1, 2] },
                            { attribute: '_id', operator: 'equal', value: [2, 3] }
                        ]
                    ]
                });

                const ids = data.map((doc) => doc._id);
                assert.deepEqual(ids, ['2']);
            });

            it('returns an empty result when nothing matches', async () => {
                const { data, totalCount } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id'],
                    filter: [[{ attribute: '_id', operator: 'equal', value: 'does-not-exist' }]]
                });

                assert.deepEqual(data, []);
                assert.equal(totalCount, 0);
            });

            it('unions multiple filter groups', async () => {
                /* two separate OR-groups (outer array) - unlike two conditions in the same AND-group,
                   each group is independent and the results are the union, not the intersection */
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    filter: [
                        [{ attribute: '_id', operator: 'equal', value: '1' }],
                        [{ attribute: '_id', operator: 'equal', value: '3' }]
                    ]
                });

                const names = data.map((doc) => doc.name).sort();
                assert.deepEqual(names, ['Captain America', 'Magneto']);
            });

            it('combines "filter" and "search"', async () => {
                /* filter alone matches 5 docs (id > 10), search alone matches 3 docs (team.name contains
                   "Fantastic") - only their intersection (Human Torch, Invisible Woman) proves both are
                   combined with AND instead of one silently overriding the other */
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    filter: [[{ attribute: 'id', operator: 'greater', value: 10 }]],
                    search: 'Fantastic',
                    queryOptions: { boost: ['team.name'] }
                });

                const names = data.map((doc) => doc.name).sort();
                assert.deepEqual(names, ['Human Torch', 'Invisible Woman']);
            });

            it('combines "filter" and "elasticsearchQuery"', async () => {
                /* filter alone matches 8 docs (id < 9), elasticsearchQuery alone matches the whole X-Men
                   roster (Wolverine, Cyclops, Storm) - only their intersection (Wolverine, Cyclops) proves
                   both are combined with AND instead of one silently overriding the other */
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    filter: [[{ attribute: 'id', operator: 'less', value: 9 }]],
                    elasticsearchQuery: { match: { 'team.name': 'X-Men' } }
                });

                const names = data.map((doc) => doc.name).sort();
                assert.deepEqual(names, ['Cyclops', 'Wolverine']);
            });
        });

        Object.entries({
            asc: ['Captain America', 'Iron Man', 'Magneto'],
            desc: ['Rocket Raccoon', 'Gamora', 'Star-Lord']
        }).forEach(([direction, expectedNames]) => {
            it(`sorts by order (${direction})`, async () => {
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    limit: 3,
                    attributes: ['_id', 'name'],
                    order: [{ attribute: 'id', direction }]
                });

                const names = data.map((doc) => doc.name);
                assert.deepEqual(names, expectedNames);
            });
        });

        it('paginates results', async () => {
            const { data } = await dataSource.process({
                esindex: INDEX,
                limit: 5,
                page: 2,
                attributes: ['_id', 'name'],
                order: [{ attribute: 'id', direction: 'asc' }]
            });

            const names = data.map((doc) => doc.name);
            assert.deepEqual(names, ['Hawkeye', 'Wolverine', 'Cyclops', 'Storm', 'Mister Fantastic']);
        });

        it('flattens nested _source fields', async () => {
            const { data } = await dataSource.process({
                esindex: INDEX,
                attributes: ['_id', 'team.name'],
                filter: [[{ attribute: '_id', operator: 'equal', value: '1' }]]
            });

            assert.ok(data.length > 0);
            const [doc] = data;
            assert.ok(Object.hasOwn(doc, 'team.name'));
            assert.equal(doc['team.name'], 'Avengers');
        });

        it('rejects invalid requests from Elasticsearch as RequestError', async () => {
            /* sorting on a "text" field without fielddata enabled is a genuine 400 from Elasticsearch -
               a non-existent index only yields a 404, which the datasource does not wrap into a RequestError */
            await assert.rejects(
                () =>
                    dataSource.process({
                        esindex: INDEX,
                        attributes: ['_id'],
                        order: [{ attribute: 'name', direction: 'asc' }]
                    }),
                (err) => {
                    assert.equal(err.name, 'RequestError');
                    assert.ok(Object.hasOwn(err, 'info'));
                    assert.ok(Object.hasOwn(err.info, 'originalError'));
                    return true;
                }
            );
        });

        it('rejects full-text search without "boost"', async () => {
            await assert.rejects(
                () =>
                    dataSource.process({
                        esindex: INDEX,
                        attributes: ['_id'],
                        search: 'Captain'
                    }),
                {
                    name: 'ImplementationError',
                    message: '"boost" query option is required for full-text search'
                }
            );
        });

        describe('boost', () => {
            it('finds matches on the configured fields', async () => {
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    search: 'Captain',
                    queryOptions: { boost: ['name'] }
                });

                const names = data.map((doc) => doc.name);
                assert.deepEqual(names, ['Captain America']);
            });

            it('applies per-field weights (e.g. "name^10")', async () => {
                /* "Mister Fantastic" matches both "name" ("Mister Fantastic") and "team.name" ("Fantastic
                   Four"), "Invisible Woman"/"Human Torch" only match via "team.name" - weighting "name" far
                   above "team.name" must push "Mister Fantastic" to the top */
                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    search: 'Fantastic',
                    queryOptions: { boost: ['name^10', 'team.name'] }
                });

                const names = data.map((doc) => doc.name);
                assert.equal(names[0], 'Mister Fantastic');
                assert.deepEqual(names.slice(1).sort(), ['Human Torch', 'Invisible Woman']);
            });
        });

        it('ranks results by field_value_factor', async () => {
            /* all matched "team.name" values are the identical string "Avengers", so their base text
               relevance ties - any ordering difference must come from field_value_factor scaling the
               score by "id", ranking the highest id first */
            const { data } = await dataSource.process({
                esindex: INDEX,
                attributes: ['_id', 'name'],
                search: 'Avengers',
                queryOptions: {
                    boost: ['team.name'],
                    field_value_factor: { field: 'id', factor: 100 }
                }
            });

            const names = data.map((doc) => doc.name);
            assert.deepEqual(names, ['Hawkeye', 'Black Widow', 'Thor', 'Iron Man', 'Captain America']);
        });

        describe('prepare query options', () => {
            let dataSource;

            beforeEach(() => {
                dataSource = new FloraElasticsearch(api, { node: NODE });
            });

            it('finds matches using "boost" parsed from the resource config', async () => {
                const dsConfig = { boost: 'name,team.name' };
                dataSource.prepare(dsConfig);

                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    search: 'Captain',
                    queryOptions: dsConfig.queryOptions
                });

                const names = data.map((doc) => doc.name);
                assert.deepEqual(names, ['Captain America']);
            });

            it('ranks results using "field_value_factor" parsed from the resource config', async () => {
                const dsConfig = {
                    boost: 'team.name',
                    field_value_factor: '{"field":"id","factor":100}'
                };
                dataSource.prepare(dsConfig);

                const { data } = await dataSource.process({
                    esindex: INDEX,
                    attributes: ['_id', 'name'],
                    search: 'Avengers',
                    queryOptions: dsConfig.queryOptions
                });

                const names = data.map((doc) => doc.name);
                assert.deepEqual(names, ['Hawkeye', 'Black Widow', 'Thor', 'Iron Man', 'Captain America']);
            });
        });
    }
);
