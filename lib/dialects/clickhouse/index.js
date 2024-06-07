// MySQL Client
// -------
const inherits = require('inherits');

const Client = require('../../client');
const Bluebird = require('bluebird');

const Transaction = require('../../execution/transaction');//
const QueryCompiler = require('./compiler');
const SchemaCompiler = require('../../schema/compiler');
const TableCompiler = require('../../schema/tablecompiler');
const ColumnCompiler = require('../../schema/columncompiler');
const QueryBuilder = require('./builder');
const { assign, map } = require('lodash');
const { makeEscape } = require('../../util/string');//

const sqlString = require('sqlstring');

// Always initialize with the "QueryBuilder" and "QueryCompiler"
// objects, which extend the base 'lib/query/builder' and
// 'lib/query/compiler', respectively.
class Client_Clickhouse extends Client {
  constructor(config) {
    super(config);
    if (config.returning) {
      this.defaultReturning = config.returning;
    }

    if (config.searchPath) {
      this.searchPath = config.searchPath;
    }
  }

  queryCompiler(builder, formatter) {
    return new QueryCompiler(this, builder, formatter);
  }

  queryBuilder() {
    return new QueryBuilder(this);
  }

  schemaCompiler() {
    return new SchemaCompiler(this, ...arguments);
  }

  tableCompiler() {
    return new TableCompiler(this, ...arguments);
  }

  columnCompiler() {
    return new ColumnCompiler(this, ...arguments);
  }

  transaction() {
    return new Transaction(this, ...arguments);
  }

  _escapeBinding(value) {
    return makeEscape()(value);
  }

  wrapIdentifierImpl(value) {
    return value !== '*' ? `\`${value.replace(/`/g, '``')}\`` : '*';
  }

  acquireRawConnection() {
    return new Bluebird((resolver, rejecter) => {
      this.driver.state = 'connected';
      resolver(this.driver);
    });
  }

  async destroyRawConnection(connection) {
    return Bluebird.fromCallback(connection.end.bind(connection))
      .catch((err) => {
        connection.__knex__disposed = err;
      })
      .finally(() => connection.removeAllListeners());
  }

  validateConnection(connection) {
    if (
      connection.state === 'connected' ||
      connection.state === 'authenticated'
    ) {
      return true;
    }
    return false;
  }

  _stream(connection, obj, stream, options) {
    options = options || {};
    const queryOptions = assign({ sql: obj.sql }, obj.options);
    return new Bluebird((resolver, rejecter) => {
      stream.on('error', rejecter);
      stream.on('end', resolver);
      const queryStream = connection
        .query(queryOptions, obj.bindings)
        .stream(options);

      queryStream.on('error', (err) => {
        rejecter(err);
        stream.emit('error', err);
      });

      queryStream.pipe(stream);
    });
  }

  _query(connection, obj) {
    if (!obj || typeof obj === 'string') obj = { sql: obj };
    return new Bluebird((resolver, rejecter) => {
      if (!obj.sql) {
        resolver();
        return;
      }

      const queryOptions = assign({ sql: obj.sql }, obj.options);
      const query = sqlString.format(queryOptions.sql, obj.bindings);
      connection.query(query, function (err, rows, fields) {
        if (err) return rejecter(err);
        obj.response = [rows, fields];
        resolver(obj);
      });
    });
  }

  processResponse(obj, runner) {
    if (obj == null) return;
    const { response } = obj;
    const { method } = obj;
    const rows = response[0];
    const fields = response[1];
    if (obj.output) return obj.output.call(runner, rows, fields);
    switch (method) {
      case 'select':
      case 'pluck':
      case 'first': {
        if (method === 'pluck') return map(rows, obj.pluck);
        return method === 'first' ? rows[0] : rows;
      }
      case 'insert':
        return [rows.insertId];
      case 'del':
      case 'update':
      case 'counter':
        return rows.affectedRows;
      default:
        return response;
    }
  }

  cancelQuery(connectionToKill) {
    const acquiringConn = this.acquireConnection();

    return acquiringConn
      .timeout(100)
      .then((conn) =>
        this.query(conn, {
          method: 'raw',
          sql: 'KILL QUERY ?',
          bindings: [connectionToKill.threadId],
          options: {},
        })
      )
      .finally(() => {
        acquiringConn.then((conn) => this.releaseConnection(conn));
      });
  }
}

assign(Client_Clickhouse.prototype, {
  dialect: 'clickhouse',

  driverName: 'clickhouse',

  _driver() {
    const { ClickHouse } = require('clickhouse');
    const clickhouse = new ClickHouse({
      url: this.config.connection.host,
      port: this.config.connection.port,
      debug: false,
      basicAuth: null,
      isUseGzip: false,
      user: this.config.connection.user,
      password: this.config.connection.password,
      database: this.config.connection.database,
      config: {
        session_timeout: 60,
        output_format_json_quote_64bit_integers: 0,
        enable_http_compression: 0,
      },
    });
    return clickhouse;
  },
});

module.exports = Client_Clickhouse;