"use strict";
exports.__esModule = true;
var TYPES = [0, 1, 2];
var CompressionTypesClass = /** @class */ (function () {
    function CompressionTypesClass() {
        this.NONE = 0 /* NONE */;
        this.GZIP = 1 /* GZIP */;
        this.SNAPPY = 2 /* SNAPPY */;
    }
    CompressionTypesClass.prototype.isValid = function (type) {
        if (typeof type !== "number") {
            return false;
        }
        return TYPES.indexOf(type) !== -1;
    };
    return CompressionTypesClass;
}());
exports.CompressionTypesClass = CompressionTypesClass;
exports.CompressionTypes = new CompressionTypesClass();
