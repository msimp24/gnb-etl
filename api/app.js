const express = require('express')
const app = express()
const morgan = require('morgan')

app.use(morgan('dev'))

const salesRoutes = require('./routers/salesRoute')
app.use('/sales', salesRoutes)

app.use(express.json())

module.exports = app
