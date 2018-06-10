const { from } = require('rxjs');
const { map, tap, mergeMap } = require('rxjs/operators');
const mysql = require('mysql2/promise');
let conn;

module.exports = {
  init: () => {
    this.pool = mysql.createPool({
      host: 'localhost',
      port: '3306',
      user: 'root',
      // password: 'guswhdrla1!',
      password: 'grapgrap',
      database: 'sourgrape',
      connectionLimit: 490,
      waitForConnection: false,
      multipleStatements: true
    });
  },
  query: (sql) => {
    return from(this.pool.getConnection()).pipe(
      mergeMap(
        (conn) => {
          // console.log(`=============== Run Query : ${sql}`);
          const res = from(conn.query(sql)).pipe(
            map(res => res[0]) // only data part
          );
          conn.release();
          return res;
        }
      ),
    )
  },
  disconnect: () => {
    if (this.pool) {
      this.pool.end();
    }
  }
};