'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { Connection, errors } = require('@elastic/elasticsearch');

const FloraElasticsearch = require('../../index');
const mockLog = require('../mock-log');

describe('Flora Elasticsearch DataSource', () => {
    const floraRequest = { esindex: 'marvel', attributes: ['_id'] };
    const api = { log: mockLog };

    describe('interface', () => {
        const dataSource = new FloraElasticsearch(api, { node: 'http://example.com/elasticsearch' });

        it('should export a query function', () => {
            assert.ok(typeof dataSource.process === 'function');
        });

        it('should export a prepare function', () => {
            assert.ok(typeof dataSource.prepare === 'function');
        });
    });

    describe('error handling', () => {
        it('should re-throw non-client errors', async () => {
            const dataSource = new FloraElasticsearch(api, {
                node: 'http://elasticsearch.example.com/',
                Connection: class extends Connection {
                    request(params, callback) {
                        callback(new errors.ConnectionError('Something bad happened!'), null);
                        return { abort() {} };
                    }
                }
            });

            await assert.rejects(() => dataSource.process(floraRequest), {
                name: 'ConnectionError',
                message: 'Something bad happened!'
            });
        });
    });
});
