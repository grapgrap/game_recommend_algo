const express = require('express');
const router = express.Router();
const database = require('../db/database');
const moment = require('moment');
const mysql = require('mysql2');
const { from, of, zip, concat } = require('rxjs');
const { mergeMap, map, shareReplay, tap, reduce, filter, merge, groupBy, count, toArray, take, distinct } = require('rxjs/operators');

function collaborateFilter(targetUserId, gameId) {
  const CAN_NOT_COMPUTE = -999; // 예상 점수를 계산 할 수 없을 때 출력할 값

  const LIMIT_NUMBER_OF_TARGET_USER_GAMES = 100;
  const NUMBER_OF_MATCHED_GAME = 2;
  const LIMIT_NUMBER_OF_NEIGHBORHOODS = 30;
  const LIMIT_NUMBER_OF_GAMES = 100;

  // 타겟 유저의 게임 평가 리스트

  let q = `
    SELECT * FROM game_rate
     WHERE 
      user_id = ${targetUserId} 
      AND game_id != ${gameId} 
      ORDER BY regi_date DESC 
      ${ LIMIT_NUMBER_OF_TARGET_USER_GAMES === 0 ? '' : `LIMIT ${LIMIT_NUMBER_OF_TARGET_USER_GAMES}` }
  `;
  const targetUserRates = from(database.query(q)).pipe(
    mergeMap(targetUserRates => from(targetUserRates)),
    shareReplay()
  );

  q = `
    SELECT played.user_id as user_id, count FROM
      (SELECT user_id FROM game_rate WHERE game_id = ${gameId} AND user_id != ${targetUserId}) as played,
      (SELECT game_rate.user_id, COUNT(game_id) as count
      FROM game_rate
      WHERE game_rate.game_id in (SELECT * FROM (SELECT DISTINCT game_id FROM game_rate WHERE user_id = ${targetUserId} ORDER BY regi_date DESC LIMIT ${LIMIT_NUMBER_OF_TARGET_USER_GAMES}) as game_rate)
      GROUP BY user_id
      HAVING COUNT(game_id) >= ${NUMBER_OF_MATCHED_GAME}) as candidate
    WHERE
      played.user_id = candidate.user_id
    ORDER BY candidate.count DESC
    LIMIT ${LIMIT_NUMBER_OF_NEIGHBORHOODS}`;

  // N개 이상 겹치는 게임을 평가한 이웃 리스트
  const neighborhoods = from(database.query(q))
    .pipe(
      mergeMap(neighborhoods => from(neighborhoods)),
      shareReplay()
    );

  const targetGameRateCount = targetUserRates.pipe(
    count(),
    shareReplay()
  );

  const neighborhoodCount = neighborhoods.pipe(
    count(),
    shareReplay()
  );

  const neighborhoodsGameRatesNotTargetGame = neighborhoods.pipe(
    map(neighborhoods => neighborhoods.user_id),
    map(user_id => `SELECT * FROM game_rate WHERE user_id = ${user_id} AND game_id != ${gameId} ${ LIMIT_NUMBER_OF_GAMES === 0 ? '' : `LIMIT ${LIMIT_NUMBER_OF_GAMES}` }`),
    mergeMap(query => from(database.query(query))),
    map(neighborhoodGames => from(neighborhoodGames)),
    shareReplay()
  );

  const neighborhoodsGameRateAboutTargetGame = neighborhoods.pipe(
    map(neighborhoods => neighborhoods.user_id),
    map(user_id => `SELECT * FROM game_rate WHERE user_id = ${user_id} AND game_id = ${gameId} LIMIT 1`),
    mergeMap(query => from(database.query(query))),
    map(neighborhoodGames => from(neighborhoodGames)),
    shareReplay()
  );

  const neighborhoodsGameRates = neighborhoodsGameRatesNotTargetGame.pipe(
    merge(neighborhoodsGameRateAboutTargetGame),
  );

  const neighborhoodsSimilar = neighborhoodsGameRates.pipe(
    mergeMap(neighborhoodGameRates => sim(targetUserRates, neighborhoodGameRates)),
    shareReplay(),
  );

  const Rl = neighborhoodsGameRates.pipe(
    mergeMap(neighborhoodGameRates =>
      neighborhoodGameRates.pipe(
        reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
        concat(neighborhoodGameRates.pipe(count())),
        reduce((total, count) => total.rate / count),
      )
    ),
  );

  // Rli - Rl
  const Rli_Rl = neighborhoodsGameRates.pipe(
    mergeMap(neighborhoodGameRates =>
      neighborhoodGameRates.pipe(
        filter(gameRate => gameRate.game_id === gameId),
        zip(Rl),
        map(zip => zip[0].rate - zip[1]),
      )
    )
  );

  //
  const m = Rli_Rl.pipe(
    zip(neighborhoodsSimilar),
    map(zip => zip[0] * zip[1]),
    reduce((prev, next) => prev + next),
  );

  const d = neighborhoodsSimilar.pipe(
    map(similar => similar > 0 ? similar : similar * -1),
    reduce((prev, next) => prev + next),
  );

  const Rk = targetUserRates.pipe(
    reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
    concat(targetUserRates.pipe(count())),
    reduce((total, count) => total.rate / count)
  );

  const collabo = Rk.pipe(
    zip(m, d),
    map(zip => zip[0] + zip[1] / zip[2]),
    map(result => result > 5 ? 5 : result)
  );

  return targetGameRateCount.pipe(
    zip(neighborhoodCount),
    mergeMap(zip =>
      zip[0] === 0 || zip[1] === 0
        ? of(CAN_NOT_COMPUTE)
        : collabo
    )
  );
}

function sim(targetUserRates, neighborhoodGameRates) {
  const commonGames = targetUserRates.pipe(
    concat(neighborhoodGameRates),
    groupBy(gameRate => gameRate.game_id),
    mergeMap(group => group.pipe(toArray())),
    filter(group => group.length > 1),
    map(group => group[0]),
    shareReplay()
  );

  const Ri = commonGames.pipe(
    map(commonGame => commonGame.game_id),
    map(id => `SELECT game_id, AVG(rate) as rate FROM game_rate WHERE game_id = ${id} GROUP BY game_id`),
    mergeMap(query => from(database.query(query))),
    mergeMap(commonGameRates => from(commonGameRates)),
    shareReplay(),
  );

  const Ki = commonGames.pipe(
    mergeMap(commonGame => targetUserRates.pipe(
      filter(gameRate => gameRate.game_id === commonGame.game_id),
    )),
    shareReplay()
  );

  const Li = commonGames.pipe(
    mergeMap(commonGame => neighborhoodGameRates.pipe(
      filter(gameRate => gameRate.game_id === commonGame.game_id),
    )),
    shareReplay()
  );

  const Ki_Ri = Ki.pipe(zip(Ri),
    map(zip => ({ game_id: zip[0].game_id, rate: zip[0].rate - zip[1].rate })),
    shareReplay()
  );

  const Li_Ri = Li.pipe(zip(Ri),
    map(zip => ({ game_id: zip[0].game_id, rate: zip[0].rate - zip[1].rate })),
    shareReplay(),
  );

  // sig(  (ki - ri) * (li - ri) );
  const m = Ki_Ri.pipe(zip(Li_Ri),
    map(zip => zip[0].rate * zip[1].rate),
    reduce((prev, next) => prev + next),
    shareReplay()
  );

  // sqrt( sig( (ki - ri)^2 ) )
  const d1 = Ki_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next),
    map(rate => Math.sqrt(rate)),
    shareReplay()
  );

  // sqrt( sig( (li - ri)^2 ) )
  const d2 = Li_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next),
    map(rate => Math.sqrt(rate)),
    shareReplay()
  );

  // m / (sqrt(d1) * sqrt(d2));
  return m.pipe(
    zip(d1, d2),
    map(zip => zip[0] / zip[1] / zip[2]),
    shareReplay()
  );
}

function betterCBF(targetUserId, gameId) {
  const CAN_NOT_COMPUTE = -999; // 예상 점수를 계산 할 수 없을 때 출력할 값

  const NUMBER_OF_MATCHED_GAME = 2;
  const LIMIT_NUMBER_OF_NEIGHBORHOODS = 30;
  const LIMIT_NUMBER_OF_GAMES = 100;
  const LIMIT_DATE = moment().subtract(3, 'm').format('YYYY-MM-DD');

  const kRates = of(`SELECT * FROM game_rate WHERE user_id = ${targetUserId} AND game_id != ${gameId}`).pipe(
    mergeMap(query => from(database.query(query))),
    shareReplay()
  );

  const Rk = kRates.pipe(
    map(rates => rates.map(rate => rate.rate)),
    map(rates => rates.reduce((prev, current) => prev + current, 0) / rates.length),
  );

  const neighborhoods = of(`
        SELECT played.user_id as user_id
        FROM
          (SELECT user_id FROM game_rate WHERE game_id = ${gameId} AND user_id != ${targetUserId}) as played,
          (
            SELECT game_rate.user_id, COUNT(game_rate.game_id) as count
            FROM 
              (SELECT * FROM game_rate WHERE regi_date > ${LIMIT_DATE}) as game_rate,
              (SELECT DISTINCT game_id FROM game_rate WHERE user_id = ${targetUserId}) target
            WHERE 
              game_rate.game_id = target.game_id
            GROUP BY user_id
            HAVING COUNT(game_rate.game_id) >= ${NUMBER_OF_MATCHED_GAME}
          ) as candidate
        WHERE
        played.user_id = candidate.user_id
        ORDER BY candidate.count DESC
        LIMIT ${LIMIT_NUMBER_OF_NEIGHBORHOODS}
    `)
    .pipe(
      mergeMap(query => database.query(query)),
      mergeMap(list => from(list)),
      shareReplay()
    );
  const collabo = neighborhoods.pipe(
    map(neighborhood => neighborhood.user_id),
    mergeMap(neighborhood => {
      const lRates = from(database.query(`
        (SELECT * FROM game_rate WHERE user_id = ${neighborhood} AND game_id != ${gameId} AND regi_date > ${LIMIT_DATE})
        UNION (SELECT * FROM game_rate WHERE user_id = ${neighborhood} AND game_id = ${gameId} LIMIT 1)
      `)).pipe(shareReplay());

      const Rl = lRates.pipe(
        map(rates => rates.map(rate => rate.rate)),
        map(rates => rates.reduce((prev, current) => prev + current, 0) / rates.length),
        shareReplay()
      );
      const Rli = lRates.pipe(
        map(rates => rates.filter(rate => rate.game_id === gameId)),
        map(rates => rates[0].rate),
        shareReplay()
      );
      const sim = betterSim(kRates, lRates);
      return zip(Rli, Rl, sim);
    }),
    toArray(),
    shareReplay(),
    map(results => {
      const m = results.reduce((prev, current) => prev + (current[0] - current[1]) * current[2], 0);
      const d = results.reduce((prev, current) => prev + Math.abs(current[2]), 0);
      return m / d;
    }),
    mergeMap(result => Rk.pipe(
      map(rate => rate + result)
    ))
  );

  const kCount = kRates.pipe(count());
  const lCount = neighborhoods.pipe(count());

  return zip(kCount, lCount).pipe(
    mergeMap(zip =>
      zip[0] === 0 || zip[1] === 0
        ? of(CAN_NOT_COMPUTE)
        : collabo
    )
  );
}

function betterSim(targetUserRates, neighborhoodGameRates) {
  const commonGames = concat(
    targetUserRates.pipe(mergeMap(rates => from(rates))),
    neighborhoodGameRates.pipe(mergeMap(rates => from(rates)))
  ).pipe(
    groupBy(rate => rate.game_id),
    mergeMap(group => group.pipe(toArray())),
    filter(group => group.length > 1),
    map(group => group[0]),
    shareReplay(),
  );

  return commonGames.pipe(
    map(games => games.game_id),
    mergeMap(game => {
      const Ri = of(`SELECT game_id, AVG(rate) as rate FROM game_rate WHERE game_id = ${game}`).pipe(
        mergeMap(query => from(database.query(query))),
        map(res => res[0]),
        map(res => res.rate),
        shareReplay(),
      );

      const Ki = targetUserRates.pipe(
        map(rates => rates.filter(rate => rate.game_id === game)),
        mergeMap(rates => from(rates)),
        map(rate => rate.rate)
      );

      const Li = neighborhoodGameRates.pipe(
        map(rates => rates.filter(rate => rate.game_id === game)),
        mergeMap(rates => from(rates)),
        map(rate => rate.rate)
      );

      return zip(Ri, Ki, Li);
    }),
    toArray(),
    shareReplay(),
    map(results => {
      const m = results.reduce((prev, current) => prev + ( (current[1] - current[0]) * (current[2] - current[0]) ), 0);
      const d1 = results.reduce((prev, current) => prev + ( (current[1] - current[0]) * (current[1] - current[0]) ), 0);
      const d2 = results.reduce((prev, current) => prev + ( (current[2] - current[0]) * (current[2] - current[0]) ), 0);
      return m / (Math.sqrt(d1) * Math.sqrt(d2));
    })
  );
}

router.get('/predict-score', (req, res, next) => {
  const user_id = +req.query.user_id;
  const game_id = +req.query.game_id;
  const now = moment();
  const MUST_UPDATE_INTERVAL_DAYS = 30;
  const q =
    `
  SELECT predicted_rate
  FROM predicted_rate
  WHERE
  user_id = ${ mysql.escape(user_id) }
  AND game_id = ${ mysql.escape(game_id) }
  AND regi_date >= ${ mysql.escape(now.subtract(MUST_UPDATE_INTERVAL_DAYS, 'days').format('YYYY-MM-DD')) }
  `
  ;
  database.query(q).subscribe(rows => {
    // 예상 평점을 계산한지 3일이 지나지 않았고, 그 값이 유효한 값이면 캐시된 데이터 전송
    if (rows.length !== 0 && false) {
      res.json({ result: 'success', data: rows[0].predicted_rate });
    } else {
      let sub = betterCBF(user_id, game_id).subscribe(result => {
        let q =
          `
            INSERT INTO predicted_rate (
              game_id,
              user_id,
              predicted_rate,
              regi_date
            )
            VALUES (
            ${mysql.escape(game_id)},
            ${mysql.escape(user_id)},
            ${mysql.escape(result)},
            ${mysql.escape(now.format('YYYY-MM-DD'))}
          )`;
        database.query(q).subscribe(
          () => {
          },
          err => console.log('occur error in enroll /predict-score || ' + err.toString())
        );
        res.json({ result: 'success', data: result });
      });

      req.connection.on('close', () => {
        sub.unsubscribe();
        console.log('closed', sub.closed);
      });
    }
  });
});

router.get('/predict-test', (req, res, next) => {
  const user_id = 105140;
  const game_id = +req.query.game_id;
  betterCBF(user_id, game_id).subscribe(predict => res.json({
    result: 'success',
    predict: predict
  }));
});

function naiveBayesion(ratedTags) {
  const tags = ratedTags.pipe(distinct(tag => tag.tag_id), shareReplay());
  const resultByRates = tags.pipe(
    mergeMap(tag => {
      const targetRatedTags = ratedTags.pipe(filter(ratedTag => ratedTag.tag_id === tag.tag_id), shareReplay());
      return from([1, 2, 3, 4, 5]).pipe(
        mergeMap(rate => {
          const filteredTargetRateTagCountByRate = targetRatedTags.pipe(filter(ratedTag => ratedTag.rate === rate), count(), shareReplay());
          const targetRatedTagCount = targetRatedTags.pipe(count());
          return filteredTargetRateTagCountByRate.pipe(zip(targetRatedTagCount), map(zip => zip[0] / zip[1]));
        }),
        toArray()
      )
    }),
  );

  return tags.pipe(
    zip(resultByRates),
    map(zip => {
      const tag = zip[0];
      const result = zip[1];
      return {
        id: tag.tag_id,
        prediction: 1 * result[0] + 2 * result[1] + 3 * result[2] + 4 * result[3] + 5 * result[4]
      };
    }),
    toArray(),
    map(results => results.sort((prev, next) => next.prediction - prev.prediction)),
    mergeMap(results => from(results)),
    filter(result => result.prediction > 3),
    take(10),
    toArray(),
    shareReplay()
  );
}

router.get('/recommand', (req, res, next) => {
  const user_id = req.query.user_id;

  const ratedTags = from(database.query(
    `
  SELECT game_rate.rate, game_tag.tag_id
  FROM game_rate, game_tag
  WHERE
  user_id = ${user_id}
  AND game_rate.game_id = game_tag.game_id
  `
  )).pipe(
    mergeMap(list => from(list)),
    shareReplay()
  );

  const result = ratedTags.pipe(
    count(),
    mergeMap(length => length <= 0 ? of([]) : naiveBayesion(ratedTags)),
    mergeMap(list => list.length <= 0 ? of(null)
      : from(list).pipe(
        map(tag => tag.id),
        reduce((prev, next, index) => index === 0 ? prev +
          `tag_id = ${mysql.escape(next)}`
          : prev +
          ` OR tag_id = ${mysql.escape(next)}`
          ,
          `SELECT game_tag.game_id, game_tag.tag_id FROM game_tag WHERE `
        ),
        map(subQuery =>
          `
  SELECT game.id as id, game.title, game.url FROM
  (
    SELECT DISTINCT game_rate.game_id as game_id FROM
    (
      SELECT game_rate.game_id FROM game_rate
      WHERE user_id != ${user_id} GROUP BY game_id HAVING COUNT(rate) > 50 AND AVG(rate) > 3
    ) as game_rate
    INNER JOIN
    (${subQuery}) as game_tag
    ON game_rate.game_id = game_tag.game_id
  ) as filtered_game,
  game
  WHERE game.id = filtered_game.game_id
  ORDER BY game.release_date DESC
  `
        ),
        mergeMap(query => from(database.query(query))),
      )
    )
  );

  result.subscribe(data => res.json({ recommend: data }));
});

module.exports = router;