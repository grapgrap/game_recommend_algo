const express = require('express');
const router = express.Router();
const database = require('../db/database');
const mysql = require('mysql2');
const moment = require('moment');
const { from, of } = require('rxjs');
const { mergeMap, map, shareReplay, tap, reduce, filter, concat, groupBy, count, toArray, zip, take } = require('rxjs/operators');

function collaborateFilter(gameId, targetUserId) {
  const CAN_NOT_COMPUTE = -999; // 예상 점수를 계산 할 수 없을 때 출력할 값

  // 타겟 유저의 게임 평가 리스트
  const targetUserRates = from(database.query(`SELECT * FROM game_rate WHERE user_id = ${targetUserId}`)).pipe(
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(targetUserRates => from(targetUserRates)),
    shareReplay(),
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
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(neighborhoods => from(neighborhoods)),
    shareReplay()
  );

  const targetGameRateCount = targetUserRates.pipe(count());
  const neighborhoodCount = neighborhoods.pipe(count());

  const neighborhoodGames = neighborhoods.pipe(
    map(neighborhoods => neighborhoods.user_id),
    reduce((prev, next, index) => index === 0 ? prev + `user_id = ${next}` : prev + ` OR user_id = ${next}`, `SELECT DISTINCT game_id FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(neiberhoohGames => from(neiberhoohGames)),
    shareReplay()
  );

  const neighborhoodsGameRates = neighborhoodGames.pipe(
    map(neiberhoohGame => neiberhoohGame.game_id),
    reduce((prev, next, index) => index === 0 ? prev + `game_id = ${next}` : prev + ` OR game_id = ${next}`, `SELECT * FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(neighborhoodsGameRates => from(neighborhoodsGameRates)),
    shareReplay()
  );

  const NUMBER_OF_NEIGHBORHOOD_GAME_RATE = 20;
  const splitedNeighborhoodsByUserId = neighborhoods.pipe(
    map(neighborhood => neighborhoodsGameRates.pipe(
      filter(gameRate => gameRate.user_id === neighborhood.user_id),
      // take(NUMBER_OF_NEIGHBORHOOD_GAME_RATE),
    )),
    shareReplay()
  );

  const neighborhoodsSimilar = splitedNeighborhoodsByUserId.pipe(
    mergeMap(neighborhoodGameRates => sim(targetUserRates, neighborhoodGameRates)),
    shareReplay()
  );

  const Rl = splitedNeighborhoodsByUserId.pipe(
    mergeMap(neighborhoodGameRates =>
      neighborhoodGameRates.pipe(
        reduce((prev, next) => ({ ...prev, rate: prev.rate + next.rate })),
        concat(neighborhoodGameRates.pipe(count())),
        reduce((total, count) => total.rate / count),
      )
    ),
    shareReplay(),
  );

  // Rli - Rl
  const Rli_Rl = splitedNeighborhoodsByUserId.pipe(
    mergeMap(neighborhoodGameRates =>
      neighborhoodGameRates.pipe(
        filter(gameRate => gameRate.game_id === gameId),
        zip(Rl),
        map(zip => zip[0].rate - zip[1]),
      )
    ),
  );

  //
  const m = Rli_Rl.pipe(
    zip(neighborhoodsSimilar),
    map(zip => zip[0] * zip[1]),
    reduce((prev, next) => prev + next),
    shareReplay()
  );

  const d = neighborhoodsSimilar.pipe(
    map(similar => similar > 0 ? similar : similar * -1),
    reduce((prev, next) => prev + next),
    shareReplay()
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

  const commonGameRates = commonGames.pipe(
    map(commonGame => commonGame.game_id),
    reduce((prev, next, index) => index === 0 ? prev + `game_id = ${next}` : prev + ` OR game_id = ${next}`, `SELECT * FROM game_rate WHERE `),
    mergeMap(query => from(database.query(query))),
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(commonGameRates => from(commonGameRates)),
    shareReplay()
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
    reduce((prev, next) => prev + next, 0),
    shareReplay()
  );

  // sqrt( sig( (ki - ri)^2 ) )
  const d1 = Ki_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next, 0),
    map(rate => Math.sqrt(rate)),
    shareReplay()
  );

  // sqrt( sig( (li - ri)^2 ) )
  const d2 = Li_Ri.pipe(
    map(item => item.rate * item.rate),
    reduce((prev, next) => prev + next, 0),
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

router.get('/predict-score', (req, res, next) => {
  collaborateFilter(192, 354).subscribe(result => res.json({ result: result }));
});

router.get('/predict-score-test', (req, res, next) => {
  const targetUserId = 354;
  const targetUserRates = from(database.query(`SELECT * FROM game_rate WHERE user_id = ${targetUserId}`)).pipe(
    mergeMap(res => res),
    map(res => res[0]),
    mergeMap(targetUserRates => from(targetUserRates)),
    shareReplay(),
  );

  const test = targetUserRates.pipe(
    mergeMap(gameRate => collaborateFilter(gameRate.game_id, targetUserId).pipe(
      map(predict => {
        console.log({ real: gameRate.rate, predict: predict, isCorrect: Math.abs(gameRate.rate - predict) });
        return { real: gameRate.rate, predict: predict, isCorrect: Math.abs(gameRate.rate - predict) >= 0.5 }
      }),
    )),
  );

  const success = test.pipe(
    filter(result => result.isCorrect),
    count()
  );

  const result = test.pipe(
    count(),
    zip(success),
    map(zip => zip[1] / zip[0])
  );

  result.subscribe(
    () => {},
    () => {},
    () => {console.log('THIS TEST END')}
  );
});

module.exports = router;