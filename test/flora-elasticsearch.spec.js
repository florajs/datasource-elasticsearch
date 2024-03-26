'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { errors } = require('@elastic/elasticsearch');

const Mock = require('@elastic/elasticsearch-mock');

const FloraElasticsearch = require('../index');

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
    const floraRequest = { esindex: 'marvel', attributes: ['_id'] };
    const api = { log: mockLog };
    let dataSource;

    describe('interface', () => {
        dataSource = new FloraElasticsearch(api, { node: 'http://example.com/elasticsearch' });

        it('should export a query function', () => {
            assert.ok(typeof dataSource.process === 'function');
        });

        it('should export a prepare function', () => {
            assert.ok(typeof dataSource.prepare === 'function');
        });
    });

    describe('#process', () => {
        let mock;

        beforeEach(() => {
            mock = new Mock();

            mock.add({ method: 'GET', path: '/' }, () => ({
                name: 'mocked-es-instance',
                version: {
                    number: '7.17.1',
                    build_flavor: 'default'
                },
                tagline: 'You Know, for Search'
            }));

            dataSource = new FloraElasticsearch(api, {
                node: 'http://elasticsearch.example.com/',
                Connection: mock.getConnection()
            });
        });

        afterEach(() => mock.clearAll());

        it('should handle empty responses', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: { hits: [] }
            }));

            const { data } = await dataSource.process(floraRequest);

            assert.deepEqual(data, []);
        });

        it('should handle non-empty responses', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: {
                    hits: [
                        { _id: 1, _source: { id: 1, name: 'Captain America' } },
                        { _id: 2, _source: { id: 2, name: 'Iron Man' } }
                    ]
                }
            }));

            const { data } = await dataSource.process(floraRequest);

            assert.deepEqual(data, [
                { _id: 1, id: 1, name: 'Captain America' },
                { _id: 2, id: 2, name: 'Iron Man' }
            ]);
        });

        it('should handle nested responses', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: {
                    hits: [
                        { _id: 1, _source: { id: 1, name: 'Captain America', team: { id: 1 } } },
                        { _id: 2, _source: { id: 2, name: 'Iron Man', team: { id: 2 } } }
                    ]
                }
            }));

            const { data } = await dataSource.process(floraRequest);

            assert.deepEqual(data, [
                { _id: 1, id: 1, name: 'Captain America', 'team.id': 1 },
                { _id: 2, id: 2, name: 'Iron Man', 'team.id': 2 }
            ]);
        });

        it('should read number of found entries', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: { hits: [], total: { value: 1337, relation: 'eq' } }
            }));

            const { totalCount } = await dataSource.process(floraRequest);

            assert.equal(totalCount, 1337);
        });

        describe('error handling', () => {
            it('should re-throw client errors as Flora request errors w/ additional information', async () => {
                mock.add(
                    { method: 'POST', path: '/marvel/_search' },
                    () =>
                        new errors.ResponseError({
                            body: 'Test error handling',
                            statusCode: 400
                        })
                );

                await assert.rejects(
                    async () => await dataSource.process(floraRequest),
                    (err) => {
                        assert.equal(err.name, 'RequestError');
                        assert.equal(err.message, 'ResponseError from elasticsearch.');

                        assert.ok(Object.hasOwn(err, 'info'));
                        assert.ok(Object.hasOwn(err.info, 'originalError'));
                        assert.ok(Object.hasOwn(err.info.originalError, 'name'));
                        assert.equal(err.info.originalError.name, 'ResponseError');

                        return true;
                    }
                );
            });

            it('should re-throw non-client errors', async () => {
                mock.add(
                    { method: 'POST', path: '/marvel/_search' },
                    () => new errors.ConnectionError('Something bad happened!')
                );

                await assert.rejects(async () => await dataSource.process(floraRequest), {
                    name: 'ConnectionError',
                    message: 'Something bad happened!'
                });
            });
        });
    });
});
