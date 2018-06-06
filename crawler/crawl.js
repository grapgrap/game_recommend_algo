const { from, interval } = require('rxjs');
const fs = require('fs-extra');
const { map, take, mergeMap, tap } = require('rxjs/operators');

const express = require('express');
const router = express.Router();
const request = require('request-promise');
const cheerio = require('cheerio');
const database = require('../db/database');
const mysql = require('mysql2');
const moment = require('moment');

router.get('/games', (req, res, next) => {
  const base = 'http://www.metacritic.com/browse/games/score/metascore/all/pc?sort=desc';
  const end = 0;
  let count = 0;
  interval(5000)
    .pipe(
      take(end),
      map(i => base + `&page=${i}`),
      tap(() => console.log(`======================================= START Page ${count} =======================================`)),
      map(url => ({
        method: 'GET',
        uri: url,
        headers: { 'USER-Agent': 'sourgrape' },
        transform: body => cheerio.load(body)
      })),
      mergeMap(option => from(request(option))),
      map(
        $ => $,
        err => {
          res.json({
            result: 'fail',
            msg: `page ${count}`,
            data: err
          });
        }
      ),
      map($ =>
        $('.product_item.product_title > a')
          .map((i, el) => ({
            title: $(el).text().split('\n')[1].trim(),
            url: $(el).attr('href').split('/')[3]
          }))
          .toArray()
      ),
      tap(() => console.log(`======================================= END Page ${count++} =======================================`)),
      mergeMap(results => from(results)),
      mergeMap(item => {
        const q = 'INSERT INTO `game` (`title`, `url`) VALUES ("' + mysql.escape(item.title) + '","' + item.url + '")';
        return from(database.query(q));
      }),
      map(
        result => {
        },
        err => {
          res.json({
            result: 'fail',
            data: err
          });
        }
      )
    )
    .subscribe(
      () => {
      },
      () => {
      },
      () => {
        res.json({
          result: 'success',
          msg: 'Finish to crawl successfully about game title and url.'
        });
      }
    );
});

router.get('/games/detail', (req, res, next) => {
  const base = `http://www.metacritic.com/game/pc/`;
  const q = `SELECT * FROM game`;
  from(database.query(q))
    .pipe(
      mergeMap(res => res), // subscribe query
      map(res => res[0].slice(3701)), // get query result
      mergeMap(list =>
        interval(10000).pipe(
          // make stream about result
          take(list.length),
          map(i => list[i])
        )
      ),
      map(item => ({
        ...item,
        url: base + item.url
      })),
      mergeMap(item => {
        const option = {
          method: 'GET',
          uri: item.url,
          headers: { 'USER-Agent': 'sourgrape' },
          transform: body => cheerio.load(body)
        };
        return from(request(option)).pipe(
          map($ => $, err => res.json({ result: 'fail', msg: item.title, data: err })),
          tap(() => console.log(`============================= START ${item.title} ==============================`)),
          mergeMap($ => {
            const release_date = moment($('.summary_detail.release_data .data').text(), 'MMM D, YYYY').format('YYYY-MM-DD');
            const developer = $('.summary_detail.developer .data').text().split('\n')[1].trim();
            const q = `UPDATE game SET release_date = ${mysql.escape(release_date)}, developer = ${mysql.escape(developer)} WHERE id = ${
              item.id
              }`;
            return from(database.query(q));
          }),
          map(
            result => {
              console.log(`============================= SUCCESS ${item.title} ==============================`);
            },
            err => {
              res.json({
                result: 'fail',
                data: err
              });
            }
          )
        );
      })
    )
    .subscribe(
      () => {
      },
      () => {
      },
      () => {
        res.json({
          result: 'success',
          msg: 'Finish to crawl successfully about game release date and developer.'
        });
      }
    );
});

router.get('/games/tag', (req, res, next) => {
  const base = `http://www.metacritic.com/game/pc/`;
  const q = `SELECT * FROM game`;
  from(database.query(q))
    .pipe(
      mergeMap(res => res), // subscribe query
      map(res => res[0].slice(3783)), // get query result
      mergeMap(list =>
        interval(10000).pipe(
          // make stream about result
          take(list.length),
          map(i => list[i])
        )
      ),
      map(item => ({
        ...item,
        url: base + item.url
      })),
      mergeMap(item => {
        const option = {
          method: 'GET',
          uri: item.url,
          headers: { 'USER-Agent': 'sourgrape' },
          transform: body => cheerio.load(body)
        };
        return from(request(option)).pipe(
          map($ => $, err => res.json({ result: 'fail', msg: item.title, data: err })),
          tap(() => console.log(`============================= START ${item.title} ==============================`)),
          mergeMap($ => {
            const tags = $('.summary_detail.product_genre .data').map((i, el) => $(el).text()).toArray().filter((item, index, arr) => arr.indexOf(item) === index); // unique tags.
            return from(tags).pipe(
              mergeMap(tag => {
                const q = `INSERT INTO tag (name) SELECT ${mysql.escape(tag)} FROM DUAL WHERE NOT EXISTS (SELECT name FROM tag WHERE name = ${mysql.escape(tag)});
                           SELECT * FROM tag WHERE name = ${mysql.escape(tag)};`;
                return from(database.query(q));
              })
            );
          }),
          mergeMap(res => res),
          map(res => res[0][1][0]),
          tap(row => console.log(`====================================== INSERT ${row.name} ================`)),
          mergeMap(row => {
            const q = `INSERT INTO game_tag (game_id, tag_id) VALUE (${item.id}, ${row.id})`;
            return from(database.query(q));
          }),
          mergeMap(res => res),
          map(res => res[0]),
          tap(() => console.log(`============================= SUCCESS ${item.title} ==============================`))
        );
      })
    )
    .subscribe(
      () => {
      },
      () => {
      },
      () => {
        res.json({
          result: 'success',
          msg: 'Finish to crawl successfully about game release date and developer.'
        });
      }
    );
});

router.get('/ratings/user', (req, res, next) => {
  const baseFront = `http://www.metacritic.com/game/pc/`;
  const baseBack = `/user-reviews`;
  const q = `SELECT * FROM game`;
  from(database.query(q))
    .pipe(
      mergeMap(res => res),
      map(res => res[0].slice(3704)),
      mergeMap(list => interval(5000).pipe(take(list.length), map(i => list[i]))),
      map(item => ({
        ...item,
        url: baseFront + item.url + baseBack
      })),
      mergeMap(item => {
        const option = {
          method: 'GET',
          uri: item.url,
          headers: { 'USER-Agent': 'Mozilla/5.0' },
          transform: body => cheerio.load(body)
        };
        return from(request(option)).pipe(
          tap(() => console.log(`============================= START TITLE: ${item.title} // ID: ${item.id} ==============================`)),
          map($ => $, err => console.log(err)),
          mergeMap($ => {
            const lastPageNum = $('.page.last_page > .page_num').text() === '' ? 1 : $('.page.last_page > .page_num').text();
            return interval(1000).pipe(
              take(lastPageNum),
              tap(i => console.log(`==================== START ${item.title} PAGE ${i} =======`)),
              map(i => ({
                method: 'GET',
                uri: item.url + `?page=${i}`,
                headers: { 'USER-Agent': 'Mozilla/5.0' },
                transform: body => cheerio.load(body)
              })),
              mergeMap(option => from(request(option))),
              mergeMap($ => {
                const datas = $('.user_reviews .review_section .review_stats').map((i, elem) => {
                  const name = $(elem).children('.review_critic').children('.name').children('*').text() || '';
                  const date = moment($(elem).children('.review_critic').children('.date').text(), 'MMM D, YYYY').format('YYYY-MM-DD') || '';
                  const rate = +$(elem).children('.review_grade').children('.user').text() || 0;
                  return { name: name, date: date, rate: (rate / 2) };
                }).toArray();
                return interval(5).pipe(
                  take(datas.length),
                  map(i => datas[i]),
                  mergeMap(data => {
                    const q = `INSERT INTO user (email, nickname, is_metacritic) SELECT ${mysql.escape(data.name)}, ${mysql.escape(data.name)}, ${mysql.escape(1)} FROM DUAL WHERE NOT EXISTS (SELECT email FROM user WHERE email = ${mysql.escape(data.name)});
                             SELECT * FROM user WHERE email = ${mysql.escape(data.name)};`;
                    return from(database.query(q)).pipe(
                      mergeMap(res => res),
                      map(res => res[0][1][0]),
                      mergeMap(row => {
                        const q = `INSERT INTO game_rate (game_id, user_id, rate, regi_date) 
                        VALUE (${mysql.escape(item.id)}, ${mysql.escape(row.id)}, ${mysql.escape(data.rate)}, ${mysql.escape(data.date)})
                        ON DUPLICATE KEY UPDATE game_id = ${mysql.escape(item.id)}, user_id = ${mysql.escape(row.id)}`;
                        return from(database.query(q));
                      }),
                      mergeMap(res => res),
                      map(res => res[0])
                    );
                  })
                );
              }),
            );
          })
        );
      })
    )
    .subscribe(() => {
    }, (err) => {
      console.log(err);
      console.log("\007");

    });
});

router.get('/ratings/critic', (req, res, next) => {
  const baseFront = `http://www.metacritic.com/game/pc/`;
  const baseBack = `/critic-reviews`;
  const q = `SELECT * FROM game`;
  from(database.query(q))
    .pipe(
      mergeMap(list =>
        interval(3000).pipe(
          take(list.length - 3622),
          map(i => list[i + 3622])
        )
      ),
      map(item => ({
        ...item,
        url: baseFront + item.url + baseBack
      })),
      mergeMap(item => {
        const option = {
          method: 'GET',
          uri: item.url,
          headers: { 'USER-Agent': 'Mozilla/5.0' },
          transform: body => cheerio.load(body)
        };
        return from(request(option)).pipe(
          tap(() => console.log(`============================= START TITLE: ${item.title} // ID: ${item.id} ==============================`)),
          map($ => $, err => console.log(err)),
          mergeMap($ => {
            const datas = $('.critic_reviews .review_section .review_stats').map((i, elem) => {
              const name = $(elem).children('.review_critic').children('.source').children('a').text() || '';
              const unFormattedDate = $(elem).children('.review_critic').children('.date').text();
              let date;
              if (unFormattedDate !== '') {
                date = moment(unFormattedDate, 'MMM D, YYYY').format('YYYY-MM-DD');
              } else {
                date = unFormattedDate;
              }
              const rate = +$(elem).children('.review_grade').children('.game').text() || 0;
              return { name: name, date: date, rate: Math.round(rate / 10) / 2 };
            }).toArray();
            return interval(5).pipe(
              take(datas.length),
              map(i => datas[i]),
              mergeMap(data => {
                const q = `INSERT INTO user (email, nickname, is_metacritic) SELECT ${mysql.escape(data.name)}, ${mysql.escape(data.name)}, ${mysql.escape(1)} FROM DUAL WHERE NOT EXISTS (SELECT email FROM user WHERE email = ${mysql.escape(data.name)});
                         SELECT * FROM user WHERE email = ${mysql.escape(data.name)};`;
                return from(database.query(q)).pipe(
                  map(res => res[1][0]),
                  mergeMap(row => {
                    const q = `INSERT INTO game_rate (game_id, user_id, rate, regi_date)
                    VALUE (${mysql.escape(item.id)}, ${mysql.escape(row.id)}, ${mysql.escape(data.rate)}, ${mysql.escape(data.date)})
                    ON DUPLICATE KEY UPDATE game_id = ${mysql.escape(item.id)}, user_id = ${mysql.escape(row.id)}`;
                    return from(database.query(q));
                  })
                );
              })
            );
          })
        );
      })
    )
    .subscribe(() => {
    }, (err) => {
      console.log(err);
      console.log("\007");
      console.log("\007");
      console.log("\007");
      console.log("\007");
      console.log("\007");
    });
});

router.get('/games/thumbnail', (req, res, next) => {
  const base = `https://store.steampowered.com/search/?term=`;
  const q = `SELECT * FROM game`;
  from(database.query(q))
    .pipe(
      mergeMap(list =>
        interval(2000).pipe(take(list.length), map(i => list[i]))
      ),
      mergeMap(item => {
        const option = {
          method: 'GET',
          uri: base + item.url,
          headers: { 'USER-Agent': 'Mozilla/5.0' },
          transform: body => cheerio.load(body)
        };

        return from(request(option)).pipe(
          tap(() => console.log(`============================= START TITLE: ${item.title} // ID: ${item.id} ==============================`)),
          map($ => $, err => console.log(err)),
          map($ => {
            const target = $('.col.search_capsule > img').first().attr('src');
            let url;
            if (target) {
              url = target.split('capsule_sm_120.jpg')[0] + `header.jpg`;
            } else {
              url = `http://via.placeholder.com/460x215`;
            }
            console.log(url);
            return {
              method: 'GET',
              uri: url,
              headers: { 'USER-Agent': 'Mozilla/5.0' },
              encoding: 'binary'
            }
          }),
          mergeMap(header => from(request(header))),
          mergeMap(file => {
            return from(fs.outputFile(`../assets/${item.url}.jpg`, file, { encoding: 'binary' }));
          }),
          map(() => item.url + ' success!')
        )
      })
    ).subscribe(() => {
  }, (err) => {
    console.log(err);
    console.log("\007");
    console.log("\007");
  });
});

module.exports = router;
