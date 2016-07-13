var fs = require('fs');
var fileName = process.argv[2] || './data/allsubs.txt';
var request = require('request');
var outFileName = 'reddit-data.json';
var es = require('event-stream');
var seen = new Set();

var JSONStream = require('JSONStream');
var outgoing = createOutStream(outFileName);

var fs = require('fs');
var lines = fs.readFileSync(fileName).toString().split('\n');
var from = 0; // you can change this
var pageSize = 2;
var linkRe = /\/r\/([a-zA-Z0-9_:-]+)/g;
var urlRe = /\/r\/([a-zA-Z0-9_:-]+)/;

readProcessedFile(outFileName, downloadNexChunk);

function downloadNexChunk() {
  var to = from + pageSize;
  var chunk = lines.slice(from, to);

  console.warn('Processing from index ' + from);
  if (chunk.length > 0) {
    download(chunk, save)
  }

  function save(json) {
    json.forEach(function(record) {
      outgoing.write(record);
    })

    from = to;

    if (from < lines.length) {
      downloadNexChunk();
    } else {
      console.log('all done');
    }
  }
}

function readProcessedFile(fileName, done) {
  if (!fs.existsSync(fileName)) {
    done();
    return;
  }

  var processedRows = 0;

  console.warn('parsing processed list...');

  var parser = JSONStream.parse();
  fs.createReadStream(fileName)
    .pipe(parser)
    .pipe(es.mapSync(markProcessed))
    .on('end', reportDone);

  function markProcessed(record) {
    processedRows += 1;
    seen.add(record.url);
  }

  function reportDone() {
    console.warn('Loaded ' + processedRows + ' records from ' + fileName);
    done();
  }
}

function download(chunk, chunkDone) {
  var finished = 0;
  var allRecords = [];

  chunk.forEach(function(name) {
    downloadOne(name);
  });

  function downloadOne(name, retryCount) {
    if (seen.has(name)) {
      finished += 1;
      reportIfDone();
      return;
    }

    var link = 'https://www.reddit.com/r/' + name + '/about.json';
    console.warn('downloading ' + link);

    request(link, function(err, response, body) {
      finished += 1;
      if (err) {
        if (err.code === 'ETIMEDOUT') {
          if (retry()) return;
        }
        console.error('!!!', err);
        throw new Error('Failed to download ' + link);
      }
      if (response.statusCode !== 200) {
        if (ignoreCode(response.statusCode)) {
          console.warn('ignoring ' + link + ' since it was returned with status code: ' + response.statusCode);
          reportIfDone();
          return;
        }

        if (response.statusCode === 502 || response.statusCode === 503 || response.statusCode === 520) {
          if (retry()) return;
        }

        var message = '!!! status code ' + response.statusCode + ' for ' + link;
        message += '\nHeaders:';
        message += '\n' + JSON.stringify(response.headers);
        console.error(message);
        throw new Error(message);
      }

      var json = JSON.parse(body);
      var record = parseRecord(json.data);
      allRecords.push(record);

      reportIfDone();
    });

    function retry() {
      console.log('retrying. Current retryCount: ' + retryCount);
      if (typeof retryCount === 'number' && retryCount > 5) {
        console.log('no more retries');
        return false; // no more retries. We failed too many times
      }

      var retryAttempt = (retryCount || 0) + 1;

      console.log('sceduling retry attempt', retryAttempt);
      setTimeout(function() {
        finished -= 1;
        console.warn('retrying link ' + name + '; Retry attempt: ' + retryAttempt);
        downloadOne(name, retryAttempt);
      }, 1000 * retryAttempt);

      return true; // yes, one more chance please
    }
  }

  function reportIfDone() {
      if (finished === chunk.length) {
        setTimeout(chunkDone, 0, allRecords);
      }
  }
}

function parseRecord(data) {
  var urlMatch = data.url.match(urlRe);
  if (!urlMatch) {
    throw new Error('Cannot parse url: ' + data.url);
  }

  var parsed = {
    url: urlMatch[1],
    created: data.created,
    subscribers: data.subscribers
  };
  var links = getLinks(data.description);
  if (links && links.length) parsed.links = links;

  return parsed;
}

function getLinks(description) {
  if (!description) return;

  var match;
  var links = new Set();
  while(match = linkRe.exec(description)) {
    links.add(match[1]);
  }

  return Array.from(links);
}

function createOutStream(outFileName) {
  var outgoing = JSONStream.stringify(false);
  var fileStream = fs.createWriteStream(outFileName, {
    encoding: 'utf8',
    flags: 'a'
  });
  outgoing.pipe(fileStream);
  return outgoing;
}

function ignoreCode(code) {
  return code === 403 || // forbidden
    code === 404; // not found
}
