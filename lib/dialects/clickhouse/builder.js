// Clickhouse Query Builder
// ------
const assert = require('assert');
const inherits = require('inherits');
const QueryBuilder = require('../.././query/querybuilder');
const ArrayJoinClause = require('./arrayjoinclause');
const Raw = require('../../raw');
const {
    assign,
    isEmpty,
    isFunction,
    isObject,
    toArray,
  } = require('lodash');

  class QueryBuilder_Clickhouse extends QueryBuilder {
    constructor(client) {
      super(client);
  
      const { returning } = this._single;
      this._arrayJoinFlag = 'array';
      if (returning) {
        this.client.logger.warn(
          '.returning() is not supported by Clickhouse and will not have any effect.'
        );
      }
    }
  
  }
  
  
  assign(QueryBuilder_Clickhouse.prototype, {

    // The where function can be used in several ways:
    // The most basic is `where(key, value)`, which expands to
    // where key = value.
    
     // Adds a join clause to the query, allowing for advanced joins
    // with an anonymous function as the second argument.
    // function(table, first, operator, second)
    arrayJoin(table, first) {
        let join;
        const { schema } = this._single;
        const joinType = this._arrayJoinType();
        if (typeof first === 'function') {
            join = new ArrayJoinClause(table, joinType, schema);
            first.call(join, join);
        } else if (joinType === 'raw') {
            join = new ArrayJoinClause(this.client.raw(table, first), 'raw');
        } else {
            join = new ArrayJoinClause(
                table,
                joinType,
                table instanceof QueryBuilder ? undefined : schema
            );
            if (arguments.length > 1) {
                join.on.apply(join, toArray(arguments).slice(1));
            }
        }
        this._statements.push(join);
        return this;
    },
    
      // Helper to get or set the "joinFlag" value.
      _arrayJoinType(val) {
        if (arguments.length === 1) {
            this._joinFlag = val;
            return this;
        }
        const ret = this._arrayJoinFlag || 'array';
        this._joinFlag = 'array';
        return ret;
    },

    // JOIN blocks:
    leftArrayJoin() {
        return this._joinType('left array').join.apply(this, arguments);
    },

    preWhere(column, operator, value) {
        // Support "where true || where false"
        if (column === false || column === true) {
            return this.preWhere(1, '=', column ? 1 : 0);
        }

        // Check if the column is a function, in which case it's
        // a where statement wrapped in parens.
        if (typeof column === 'function') {
            return this.preWhereWrapped(column);
        }

        // Allow a raw statement to be passed along to the query.
        if (column instanceof Raw && arguments.length === 1)
            return this.preWhereRaw(column);

        // Allows `where({id: 2})` syntax.
        if (isObject(column) && !(column instanceof Raw))
            return this._objectPreWhere(column);

        // Enable the where('key', value) syntax, only when there
        // are explicitly two arguments passed, so it's not possible to
        // do where('key', '!=') and have that turn into where key != null
        if (arguments.length === 2) {
            value = operator;
            operator = '=';

            // If the value is null, and it's a two argument query,
            // we assume we're going for a `whereNull`.
            if (value === null) {
                return this.preWhereNull(column);
            }
        }

        // lower case the operator for comparison purposes
        const checkOperator = `${operator}`.toLowerCase().trim();

        // If there are 3 arguments, check whether 'in' is one of them.
        if (arguments.length === 3) {
            if (checkOperator === 'in' || checkOperator === 'not in') {
                return this._not(checkOperator === 'not in').preWhereIn(
                    arguments[0],
                    arguments[2]
                );
            }
            if (checkOperator === 'between' || checkOperator === 'not between') {
                return this._not(checkOperator === 'not between').preWhereBetween(
                arguments[0],
                arguments[2]
                );
            }
        }

        // If the value is still null, check whether they're meaning
        // where value is null
        if (value === null) {
            // Check for .where(key, 'is', null) or .where(key, 'is not', 'null');
            if (checkOperator === 'is' || checkOperator === 'is not') {
                return this._not(checkOperator === 'is not').preWhereNull(column);
            }
        }

        // Push onto the where statement stack.
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereBasic',
            column,
            operator,
            value,
            not: this._not(),
            bool: this._bool(),
            asColumn: this._asColumnFlag,
        });
        return this;
    },

    preWhereColumn(column, operator, rightColumn) {
        this._asColumnFlag = true;
        this.preWhere.apply(this, arguments);
        this._asColumnFlag = false;
        return this;
    },

        // Adds an `or where` clause to the query.
    orPreWhere() {
        this._bool('or');
        const obj = arguments[0];
        if (isObject(obj) && !isFunction(obj) && !(obj instanceof Raw)) {
            return this.preWhereWrapped(function() {
            for (const key in obj) {
                this.andPreWhere(key, obj[key]);
            }
            });
        }
        return this.where.apply(this, arguments);
    },

    orPreWhereColumn() {
        this._bool('or');
        const obj = arguments[0];
        if (isObject(obj) && !isFunction(obj) && !(obj instanceof Raw)) {
            return this.preWhereWrapped(function() {
            for (const key in obj) {
                this.andPreWhereColumn(key, '=', obj[key]);
            }
            });
        }
        return this.preWhereColumn.apply(this, arguments);
    },

    // Adds an `not where` clause to the query.
    preWhereNot() {
        return this._not(true).preWhere.apply(this, arguments);
    },

    preWhereNotColumn() {
        return this._not(true).preWhereColumn.apply(this, arguments);
    },

    // Adds an `or not where` clause to the query.
    orPreWhereNot() {
        return this._bool('or').preWhereNot.apply(this, arguments);
    },

    orPreWhereNotColumn() {
        return this._bool('or').preWhereNotColumn.apply(this, arguments);
    },

    // Processes an object literal provided in a "prewhere" clause.
    _objectPreWhere(obj) {
        const boolVal = this._bool();
        const notVal = this._not() ? 'Not' : '';
        for (const key in obj) {
            this[boolVal + 'prewhere' + notVal](key, obj[key]);
        }
        return this;
    },

    // Adds a raw `where` clause to the query.
    preWhereRaw(sql, bindings) {
        const raw = sql instanceof Raw ? sql : this.client.raw(sql, bindings);
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereRaw',
            value: raw,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    orPreWhereRaw(sql, bindings) {
        return this._bool('or').preWhereRaw(sql, bindings);
    },

    // Helper for compiling any advanced `where` queries.
    preWhereWrapped(callback) {
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereWrapped',
            value: callback,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    // Adds a `where exists` clause to the query.
    preWhereExists(callback) {
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereExists',
            value: callback,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    // Adds an `or where exists` clause to the query.
    orPreWhereExists(callback) {
        return this._bool('or').preWhereExists(callback);
    },

    // Adds a `where not exists` clause to the query.
    preWhereNotExists(callback) {
        return this._not(true).preWhereExists(callback);
    },

    // Adds a `or where not exists` clause to the query.
    orPreWhereNotExists(callback) {
        return this._bool('or').preWhereNotExists(callback);
    },

    // Adds a `where in` clause to the query.
    preWhereIn(column, values) {
        if (Array.isArray(values) && isEmpty(values))
            return this.preWhere(this._not());
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereIn',
            column,
            value: values,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    // Adds a `or where in` clause to the query.
    orPreWhereIn(column, values) {
        return this._bool('or').preWhereIn(column, values);
    },

    // Adds a `where not in` clause to the query.
    preWhereNotIn(column, values) {
        return this._not(true).preWhereIn(column, values);
    },

    // Adds a `or where not in` clause to the query.
    orPreWhereNotIn(column, values) {
        return this._bool('or')
            ._not(true)
            .preWhereIn(column, values);
    },

    // Adds a `where null` clause to the query.
    preWhereNull(column) {
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereNull',
            column,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    // Adds a `or where null` clause to the query.
    orPreWhereNull(column) {
        return this._bool('or').preWhereNull(column);
    },

    // Adds a `where not null` clause to the query.
    preWhereNotNull(column) {
        return this._not(true).preWhereNull(column);
    },

    // Adds a `or where not null` clause to the query.
    orPreWhereNotNull(column) {
        return this._bool('or').preWhereNotNull(column);
    },

    // Adds a `where between` clause to the query.
    preWhereBetween(column, values) {
        assert(
            Array.isArray(values),
            'The second argument to whereBetween must be an array.'
        );
        assert(
            values.length === 2,
            'You must specify 2 values for the whereBetween clause'
        );
        this._statements.push({
            grouping: 'prewhere',
            type: 'preWhereBetween',
            column,
            value: values,
            not: this._not(),
            bool: this._bool(),
        });
        return this;
    },

    // Adds a `where not between` clause to the query.
    preWhereNotBetween(column, values) {
        return this._not(true).preWhereBetween(column, values);
    },

    // Adds a `or where between` clause to the query.
    orPreWhereBetween(column, values) {
        return this._bool('or').preWhereBetween(column, values);
    },

    // Adds a `or where not between` clause to the query.
    orPreWhereNotBetween(column, values) {
        return this._bool('or').preWhereNotBetween(column, values);
    },
    // Helper to get or set the "joinFlag" value.
  });

// Set the QueryBuilder & QueryCompiler on the client object,
// in case anyone wants to modify things to suit their own purposes.
module.exports = QueryBuilder_Clickhouse;