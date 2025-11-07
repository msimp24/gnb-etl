const db = require('../config/db')

const getFactSummary = (req, res) => {
  const sql = 'select * from fact_market_summary'

  db.all(sql, (err, rows) => {
    if (err) {
      return res.status(500).json({
        status: 'failed',
        error: err,
      })
    }

    return res.status(200).send(rows)
  })
}

module.exports = { getFactSummary }
