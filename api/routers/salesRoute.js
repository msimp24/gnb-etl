const express = require('express')
const { getFactSummary } = require('../controllers/salesController')
const router = express.Router()

router.route('/get-all-facts').get(getFactSummary)

module.exports = router
