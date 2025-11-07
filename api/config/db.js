const sqlite3 = require('sqlite3')
const path = require('path')
require('dotenv').config({
  path: path.resolve(__dirname, '../../.env'),
})

const ENV_DB_PATH = process.env.DB_PATH
let dbPath

if (path.isAbsolute(ENV_DB_PATH)) {
  dbPath = ENV_DB_PATH
  console.log(dbPath)
  console.log('PROD')
} else {
  let dev_path = '../../' + ENV_DB_PATH
  dbPath = path.join(__dirname, dev_path)
  console.log('DEV')
}

let db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.log('Error occured' + err.message)
  } else {
    console.log('Database connected')
  }

  db.serialize(() => {
    db.run('PRAGMA journal_mode = WAL;') // Set WAL mode
    db.run('PRAGMA foreign_keys = ON;') // Optional: Enable foreign key support
  })
})

module.exports = db
