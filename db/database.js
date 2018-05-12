const { from } = require('rxjs');
const { map, mergeMap } = require('rxjs/operators');
const mysql = require('mysql2/promise');
let pool;

module.exports = {
  init: () => {
    this.pool = mysql.createPool({
      host: 'localhost',
      port: '3306',
      user: 'root',
      password: 'guswhdrla1!',
      // password: 'grapgrap',
      database: 'sourgrape',
      connectionLimit: 20,
      waitForConnection: false,
      multipleStatements: true
    });
  },
  query: (sql) => {
    return from(this.pool.getConnection()).pipe(
      map(
        (conn) => {
          console.log(`=============== Run Query : ${sql}`);
          const res = from(conn.query(sql));
          conn.release();
          return res;
        },
        (err) => err
      )
    )
  },
  disconnect: () => {
    if (this.pool) {
      this.pool.end();
    }
  }
};