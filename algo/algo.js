const express = require('express');
const router = express.Router();
const database = require('../db/database');
const moment = require('moment');
const mysql = require('mysql2');
const { from, of } = require('rxjs');
const { mergeMap, map, shareReplay, tap, reduce, filter, concat, groupBy, count, toArray, zip, take, distinct } = require('rxjs/operators');

function collaborateFilter(targetUserId, gameId) {
  const CAN_NOT_COMPUTE = -999; // 예상 점수를 계산 할 수 없을 때 출력할 값

  // 타겟 유저의 게임 평가 리스트
  const targetUserRates = from(database.query(`SELECT * FROM game_rate WHERE user_id = ${targetUserId}`)).pipe(
    mergeMap(targetUserRates => from(targetUserRates)),
    shareReplay()
  );

  // N개 이상 겹치는 게임을 평가한 이웃 리스트
  const NUMBER_OF_MATCHED_GAME = 2;
  const neighborhoods = from(database.query(`
    SELECT candidate.user_id AS user_id FROM (
      SELECT game_rate.user_id
      FROM game_rate
      WHERE game_rate.game_id in (SELECT DISTINCT game_id FROM game_rate WHERE user_id = ${targetUserId})
      GROUP BY user_id
      HAVING COUNT(user_id) >= ${NUMBER_OF_MATCHED_GAME}
      ORDER BY COUNT(user_id) DESC
    ) AS candidate
    WHERE candidate.user_id in (SELECT user_id FROM game_rate WHERE game_id = ${gameId})
    LIMIT 20`)
  ).pipe(
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

  const neighborhoodGames = neighborhoods.pipe(
    map(neighborhoods => neighborhoods.user_id),
    reduce((prev, next, index) => index === 0 ? prev + `user_id = ${next}` : prev + ` OR user_id = ${next}`, `SELECT DISTINCT game_id FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    mergeMap(neiberhoohGames => from(neiberhoohGames)),
    shareReplay()
  );

  const neighborhoodsGameRates = neighborhoodGames.pipe(
    map(neiberhoohGame => neiberhoohGame.game_id),
    reduce((prev, next, index) => index === 0 ? prev + `game_id = ${next}` : prev + ` OR game_id = ${next}`, `SELECT * FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    mergeMap(neighborhoodsGameRates => from(neighborhoodsGameRates)),
    shareReplay()
  );

  const NUMBER_OF_NEIGHBORHOOD_GAME_RATE = 30;
  const splitedNeighborhoodsByUserId = neighborhoods.pipe(
    map(neighborhood => neighborhoodsGameRates.pipe(
      filter(gameRate => gameRate.user_id === neighborhood.user_id),
      take(NUMBER_OF_NEIGHBORHOOD_GAME_RATE),
    )),
    shareReplay()
  );

  const neighborhoodsSimilar = splitedNeighborhoodsByUserId.pipe(
    mergeMap(neighborhoodGameRates => sim(targetUserRates, neighborhoodGameRates)),
    shareReplay(),
  );

  const Rl = splitedNeighborhoodsByUserId.pipe(
    mergeMap(neighborhoodGameRates =>
      neighborhoodGameRates.pipe(
        reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
        concat(neighborhoodGameRates.pipe(count())),
        reduce((total, count) => total.rate / count),
      )
    )
  );

  // Rli - Rl
  const Rli_Rl = splitedNeighborhoodsByUserId.pipe(
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
    reduce((prev, next) => prev + next)
  );

  const Rk = targetUserRates.pipe(
    reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
    concat(targetUserRates.pipe(count())),
    reduce((total, count) => total.rate / count),
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

  const commonGameRates = commonGames.pipe(
    map(commonGame => commonGame.game_id),
    reduce((prev, next, index) => index === 0 ? prev + `game_id = ${next}` : prev + ` OR game_id = ${next}`, `SELECT * FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    shareReplay(),
    mergeMap(commonGameRates => from(commonGameRates))
  );

  const Ki = commonGames.pipe(
    mergeMap(commonGame => targetUserRates.pipe(
      filter(gameRate => gameRate.game_id === commonGame.game_id)
    )),
    shareReplay()
  );

  const Li = commonGames.pipe(
    mergeMap(commonGame => neighborhoodGameRates.pipe(
      filter(gameRate => gameRate.game_id === commonGame.game_id)
    )),
    shareReplay()
  );

  const splitedCommonGameRatesByGameId = commonGames.pipe(
    mergeMap(commonGame => commonGameRates.pipe(
      filter(gameRate => gameRate.game_id === commonGame.game_id),
    )),
    shareReplay()
  );

  const Ri = splitedCommonGameRatesByGameId.pipe(
    reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
    concat(splitedCommonGameRatesByGameId.pipe(count())),
    reduce((total, count) => ({ ...total, rate: total.rate / count })),
    shareReplay()
  );

  const Ki_Ri = Ki.pipe(zip(Ri),
    map(zip => ({ game_id: zip[0].game_id, rate: zip[0].rate - zip[1].rate })),
    shareReplay()
  );

  const Li_Ri = Li.pipe(zip(Ri),
    map(zip => ({ game_id: zip[0].game_id, rate: zip[0].rate - zip[1].rate })),
    shareReplay()
  );

  // sig(  (ki - ri) * (li - ri) );
  const m = Ki_Ri.pipe(zip(Li_Ri),
    map(zip => zip[0].rate * zip[1].rate),
    reduce((prev, next) => prev + next, 0)
  );

  // sqrt( sig( (ki - ri)^2 ) )
  const d1 = Ki_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next, 0),
    map(rate => Math.sqrt(rate))
  );

  // sqrt( sig( (li - ri)^2 ) )
  const d2 = Li_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next, 0),
    map(rate => Math.sqrt(rate))
  );

  // m / (sqrt(d1) * sqrt(d2));
  return m.pipe(
    zip(d1, d2),
    map(zip => zip[0] / zip[1] / zip[2])
  );
}

router.get('/predict-score', (req, res, next) => {
  const user_id = +req.query.user_id;
  const game_id = +req.query.game_id;
  const now = moment();
  const MUST_UPDATE_INTERVAL_DAYS = 30;
  const q = `
    SELECT predicted_rate 
      FROM predicted_rate 
      WHERE 
        user_id = ${ mysql.escape(user_id) } 
        AND game_id = ${ mysql.escape(game_id) } 
        AND regi_date >= ${ mysql.escape(now.subtract(MUST_UPDATE_INTERVAL_DAYS, 'days').format('YYYY-MM-DD')) }
  `;
  database.query(q).subscribe(rows => {
    // 예상 평점을 계산한지 3일이 지나지 않았고, 그 값이 유효한 값이면 캐시된 데이터 전송
    if (rows.length !== 0) {
      res.json({ result: 'success', data: rows[0].predicted_rate });
    } else {
      let sub = collaborateFilter(user_id, game_id).subscribe(result => {
        let q = `
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
        database.query(q).subscribe(res => console.log(res), err => console.log('occur error in enroll /predict-score || ' + err.toString()));
        res.json({ result: 'success', data: result });
      });

      req.connection.on('close', () => {
        sub.unsubscribe();
        console.log('closed', sub.closed);
      });
    }
  });
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

  const ratedTags = from(database.query(`
    SELECT game_rate.rate, game_tag.tag_id
      FROM game_rate, game_tag
      WHERE 
        user_id = ${user_id}
        AND game_rate.game_id = game_tag.game_id
  `)).pipe(
    mergeMap(list => from(list)),
    shareReplay()
  );

  const result = ratedTags.pipe(
    count(),
    mergeMap(length => length <= 0 ? of([]) : naiveBayesion(ratedTags)),
    mergeMap(list => list.length <= 0 ? of(null)
      : from(list).pipe(
        map(tag => tag.id),
        reduce((prev, next, index) => index === 0 ? prev + `tag_id = ${mysql.escape(next)}` : prev + ` OR tag_id = ${mysql.escape(next)}`, `SELECT * FROM game_tag WHERE `),
        map(subQuery => `
            SELECT game_rate.game_id as id, game.title, game.url, game_tag.tag_id FROM
              (SELECT game_id, AVG(rate) as rate FROM game_rate WHERE user_id != ${user_id} GROUP BY game_id HAVING COUNT(rate) > 10) as game_rate,
              (${subQuery} GROUP BY game_id ORDER BY null) as game_tag,
              game
              WHERE game_rate.game_id = game_tag.game_id AND game_rate.game_id = game.id AND game_tag.game_id = game.id
              AND game_rate.rate > 3
              ORDER BY game.release_date DESC
          `),
        tap(console.log),
        mergeMap(query => from(database.query(query))),
      )
    )
  );

  result.subscribe(data => res.json({ recommend: data }));
});

module.exports = router;