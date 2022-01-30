const http = require('http')
const path = require('path')
const express = require('express')
const socketIo = require('socket.io')
const needle = require('needle')
const config = require('dotenv').config()
const TOKEN = process.env.BEARER_TOKEN
const PORT = process.env.PORT || 3000
const emoji = require('emoji-strip')
const langdetect = require('langdetect');
const app = express()

const server = http.createServer(app)
const io = socketIo(server)

app.get('/', (req, res) => {
  res.sendFile(path.resolve(__dirname, '../', 'index.html'))
})

const rulesURL = 'https://api.twitter.com/2/tweets/search/stream/rules'
const streamURL =
  'https://api.twitter.com/2/tweets/search/stream?tweet.fields=public_metrics&expansions=author_id'

const rules = [{ value: 'BTCUSD' }]

// Get stream rules
async function getRules() {
  const response = await needle('get', rulesURL, {
    headers: {
      Authorization: `Bearer ${TOKEN}`,
    },
  })
  console.log(response.body)
  return response.body
}

// Set stream rules
async function setRules() {
  const data = {
    add: rules,
  }

  const response = await needle('post', rulesURL, data, {
    headers: {
      'content-type': 'application/json',
      Authorization: `Bearer ${TOKEN}`,
    },
  })

  return response.body
}

// Delete stream rules
async function deleteRules(rules) {
  if (!Array.isArray(rules.data)) {
    return null
  }

  const ids = rules.data.map((rule) => rule.id)

  const data = {
    delete: {
      ids: ids,
    },
  }

  const response = await needle('post', rulesURL, data, {
    headers: {
      'content-type': 'application/json',
      Authorization: `Bearer ${TOKEN}`,
    },
  })

  return response.body
}

function streamTweets(socket) {
  const stream = needle.get(streamURL, {
    headers: {
      Authorization: `Bearer ${TOKEN}`,
    },
  })

  stream.on('data', (data) => {
    try {
      const json = JSON.parse(data)
    //   text = json[data]
      let text = json.data.text
      text = text.replace(/(@)[\n\S]+/g, '')
      text = emoji(text)
      text = text.replace(/(^|\s)(:D|:\/|:\)+|;\)|:-\))(?=\s|[^[:alnum:]+-]|$)/g,'')
      text = text.replace(/(?:https?):\/\/[\n\S]+/g, '')
      text = text.replace(/(?:http?):\/\/[\n\S]+/g, '')
      text = text.replace('\r', '')
      .replace(/RT\s+/g, '')
      .replace('&amp;', '')
      .replace('&lt;', '')
      .replace('&gt;', '')
      .replace(/&gt;+/g,'')
      .replace(/#/g, '')
      .replace(/\s+/g, ' ').trim()
      text = text.replace(/([\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF])/g, '');
      if(langdetect.detect(text)[0].lang == 'en'){
        console.log(text)
        socket.emit('tweet', text)
      }
    } catch (error) {}
  })

  return stream
}

io.on('connection', async () => {
  console.log('Client connected...')

  let currentRules

  try {
    //   Get all stream rules
    currentRules = await getRules()

    // Delete all stream rules
    await deleteRules(currentRules)

    // Set rules based on array above
    await setRules()
  } catch (error) {
    console.error(error)
    process.exit(1)
  }

  const filteredStream = streamTweets(io)

  let timeout = 0
  filteredStream.on('timeout', () => {
    // Reconnect on error
    console.warn('A connection error occurred. Reconnecting…')
    setTimeout(() => {
      timeout++
      streamTweets(io)
    }, 2 ** timeout)
    streamTweets(io)
  })
})

server.listen(PORT, () => console.log(`Listening on port ${PORT}`))