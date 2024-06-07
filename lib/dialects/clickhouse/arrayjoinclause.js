const { assign } = require('lodash');
const assert = require('assert');

// ArrayJoinClause
// -------

// The "ArrayJoinClause" is an object holding any necessary info about a join,
// including the type, and any associated tables & columns being joined.
function ArrayJoinClause(table, type, schema) {
  this.schema = schema;
  this.table = table;
  this.joinType = type;
  this.and = this;
  this.clauses = [];
  this.grouping = 'arrayJoin';
}
ArrayJoinClause.prototype.left = ArrayJoinClause.prototype.left;
//Test for repo
module.exports = ArrayJoinClause;
