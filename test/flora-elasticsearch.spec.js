'use strict';

const { expect } = require('chai');

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
});
