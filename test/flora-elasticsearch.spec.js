'use strict';

const { expect } = require('chai');
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
    const api = { log: mockLog };
    let dataSource;

    describe('interface', () => {
        dataSource = new FloraElasticsearch(api, { node: 'http://example.com/elasticsearch' });

        it('should export a query function', () => {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(dataSource.prepare).to.be.a('function');
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

            const { data } = await dataSource.process({ esindex: 'marvel' });
            expect(data).to.eql([]);
        });

        it('should handle non-empty responses', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: {
                    hits: [
                        { _id: 1, _type: 'doc', _source: { id: 1, name: 'Captain America' } },
                        { _id: 2, _type: 'doc', _source: { id: 2, name: 'Iron Man' } }
                    ]
                }
            }));

            const { data } = await dataSource.process({ esindex: 'marvel' });
            expect(data).to.eql([
                { _id: 1, _type: 'doc', id: 1, name: 'Captain America' },
                { _id: 2, _type: 'doc', id: 2, name: 'Iron Man' }
            ]);
        });

        it('should handle nested responses', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: {
                    hits: [
                        { _id: 1, _type: 'doc', _source: { id: 1, name: 'Captain America', team: { id: 1 } } },
                        { _id: 2, _type: 'doc', _source: { id: 2, name: 'Iron Man', team: { id: 2 } } }
                    ]
                }
            }));

            const { data } = await dataSource.process({ esindex: 'marvel' });
            expect(data).to.eql([
                { _id: 1, _type: 'doc', id: 1, name: 'Captain America', 'team.id': 1 },
                { _id: 2, _type: 'doc', id: 2, name: 'Iron Man', 'team.id': 2 }
            ]);
        });

        it('should read number of found entries', async () => {
            mock.add({ method: 'POST', path: '/marvel/_search' }, () => ({
                hits: { hits: [], total: { value: 1337, relation: 'eq' } }
            }));

            const { totalCount } = await dataSource.process({ esindex: 'marvel' });
            expect(totalCount).to.equal(1337);
        });
    });
});
