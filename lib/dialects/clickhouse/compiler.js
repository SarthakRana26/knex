// Clickhouse Query Compiler
// ------
const inherits = require('inherits');
const QueryCompiler = require('../../query/querycompiler');
const helpers = require('../../util/helpers');
const {
  columnize: columnize_,
  direction: direction_,
  operator: operator_,
  wrap: wrap_,
  unwrapRaw: unwrapRaw_,
  rawOrFn: rawOrFn_,
  parameter:parameter_
} = require('../.././formatter/wrappingFormatter');
const {
  assign,
  bind,
  map,
} = require('lodash');
const { stat } = require('fs');

class QueryCompiler_Clickhouse extends QueryCompiler {
  constructor(client, builder,formatter) {
    super(client, builder,formatter);
    const { returning } = this.single;

    if (returning) {
      this.client.logger.warn(
        '.returning() is not supported by Clickhouse and will not have any effect.'
      );
    }
  }
  arrayJoin() {
    let sql = '';
    let i = -1;
    const joins = this.grouped.arrayJoin;
    if (!joins) return '';
    while (++i < joins.length) {
      const join = joins[i];
      const table = join.schema ? `${join.schema}.${join.table}` : join.table;
      if (i > 0) sql += ' ';
      if (join.joinType === 'raw') {
        sql += unwrapRaw_(join.table);
      } else {
        sql += join.joinType + ' join ' + this.formatter.wrap(table);
        let ii = -1;
        while (++ii < join.clauses.length) {
          const clause = join.clauses[ii];
          if (ii > 0) {
            sql += ` ${clause.bool} `;
          } else {
            sql += ` ${clause.type === 'onUsing' ? 'using' : 'on'} `;
          }
          const val = this[clause.type].call(this, clause);
          if (val) {
            sql += val;
          }
        }
      }
    }
    return sql;
  }

  // Update method, including joins, wheres, order & limits.
  update() {
    const join = this.arrayJoin();
    const updates = this._prepUpdate(this.single.update);
    const where = this.where();
    const order = this.order();
    const limit = this.limit();
    return (
      `update ${this.tableName}` +
      (join ? ` ${join}` : '') +
      ' set ' +
      updates.join(', ') +
      (where ? ` ${where}` : '') +
      (order ? ` ${order}` : '') +
      (limit ? ` ${limit}` : '')
    );
  }

  forUpdate() {
    return 'for update';
  }

  forShare() {
    return 'lock in share mode';
  }

  // Compiles a `columnInfo` query.
  columnInfo() {
    const column = this.single.columnInfo;

    // The user may have specified a custom wrapIdentifier function in the config. We
    // need to run the identifiers through that function, but not format them as
    // identifiers otherwise.
    const table = this.client.customWrapIdentifier(this.single.table, identity);

    return {
      sql:
        'select * from information_schema.columns where table_name = ? and table_schema = ?',
      bindings: [table, this.client.database()],
      output(resp) {
        const out = resp.reduce(function(columns, val) {
          columns[val.COLUMN_NAME] = {
            defaultValue: val.COLUMN_DEFAULT,
            type: val.DATA_TYPE,
            maxLength: val.CHARACTER_MAXIMUM_LENGTH,
            nullable: val.IS_NULLABLE === 'YES',
          };
          return columns;
        }, {});
        return (column && out[column]) || out;
      },
    };
  }

  limit() {
    const noLimit = !this.single.limit && this.single.limit !== 0;
    if (noLimit && !this.single.offset) return '';

    // Workaround for offset only.
    // see: http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
    const limit =
    this.single.offset && noLimit
    ? '18446744073709551615'
    : this._getValueOrParameterFromAttribute('limit');
return `limit ${limit}`;
  }

  // PreWhere Clause
  // ------

  // Compiles all `where` statements on the query.
  prewhere() {
    const wheres = this.grouped.prewhere;
    if (!wheres) return;
    const sql = [];
    let i = -1;
    while (++i < wheres.length) {
      const stmt = wheres[i];
      if (
        stmt.hasOwnProperty('value') &&
        helpers.containsUndefined(stmt.value)
      ) {
        this._undefinedInWhereClause = true;
      }
      const val = this[stmt.type](stmt);
      if (val) {
        if (sql.length === 0) {
          sql[0] = 'prewhere';
        } else {
          sql.push(stmt.bool);
        }
        sql.push(val);
      }
    }
    return sql.length > 1 ? sql.join(' ') : '';
  }

  preWhereIn(statement) {
    let columns = null;
    if (Array.isArray(statement.column)) {
      columns = `(${this.formatter.columnize(statement.column)})`;
    } else {
      columns = this.formatter.wrap(statement.column);
    }

    const values = this.formatter.values(statement.value);
    return `${columns} ${this._not(statement, 'in ')}${values}`;
  }

  preWhereNull(statement) {
    return (
      this.formatter.wrap(statement.column) +
      ' is ' +
      this._not(statement, 'null')
    );
  }

  // Compiles a basic "where" clause.
  preWhereBasic(statement) {
    return (
      this._not(statement, '') +
      wrap_(
        statement.column,
        undefined,
        this.builder,
        this.client,
        this.bindingsHolder
      ) +
      ' ' +
      operator_(
        statement.operator,
        this.builder,
        this.client,
        this.bindingsHolder
      ) +
      ' ' +
      this._valueClause(statement)
    );
  }

  preWhereExists(statement) {
    return (
      this._not(statement, 'exists') +
      ' (' +
      this.formatter.rawOrFn(statement.value) +
      ')'
    );
  }

  preWhereWrapped(statement) {
    const val = this.formatter.rawOrFn(statement.value, 'where');
    return (val && this._not(statement, '') + '(' + val.slice(6) + ')') || '';
  }

  preWhereBetween(statement) {
   return (
      wrap_(
        statement.column,
        undefined,
        this.builder,
        this.client,
        this.bindingsHolder
      ) +
      ' ' +
      this._not(statement, 'between') +
      ' ' +
      statement.value
        .map((value) =>
          this.client.parameter(value, this.builder, this.bindingsHolder)
        )
        .join(' and ')
    );
  }

  // Compiles a "whereRaw" query.
  preWhereRaw(statement) {
    return this._not(statement, '') + unwrapRaw_(statement.value,undefined,
      this.builder,
      this.client,
      this.bindingsHolder);
  }
  

  // Compiles all each of the `join` clauses on the query,
  // including any nested join queries.
  

}
// Set the QueryBuilder & QueryCompiler on the client object,
// in case anyone wants to modify things to suit their own purposes.
module.exports = QueryCompiler_Clickhouse;
