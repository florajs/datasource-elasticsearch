'use strict';

const nop = () => {};

module.exports = {
    info: nop,
    debug: nop,
    trace: nop,
    error: nop,
    warn: nop,
    child() {
        return module.exports;
    }
};
