'use strict';

const { setTimeout } = require('node:timers/promises');
const { parseArgs } = require('node:util');

const MAX_RETRIES = 50;
const RETRY_DELAY_MS = 1000;

const {
    values: { url }
} = parseArgs({ options: { url: { type: 'string' } } });

if (!URL.canParse(url)) {
    console.error(`Not a valid URL: "${url}"`);
    process.exit(1);
}

(async () => {
    for (let retries = 0; retries <= MAX_RETRIES; retries++) {
        const isReachable = await fetch(url, { signal: AbortSignal.timeout(1000) })
            .then((res) => res.ok)
            .catch(() => false);

        if (isReachable) {
            console.error(`Connected to Elasticsearch (${url}) - running tests`);
            return;
        }

        console.error('Elasticsearch is unavailable - sleeping');
        await setTimeout(RETRY_DELAY_MS);
    }

    console.error('Tried multiple times - giving up');
    process.exit(1);
})();
