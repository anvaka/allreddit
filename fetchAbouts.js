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
var activeRequests = 0;
var maxConcurrent = 2;
var linkRe = /\/r\/([a-zA-Z0-9_:-]+)/g;
var urlRe = /\/r\/([a-zA-Z0-9_:-]+)/;

readProcessedFile(outFileName, downloadNext);

function downloadNext() {
  activeRequests += 1;

  download(lines[from], save)
  from += 1;
  scheduleNext();

  function save(json) {
    activeRequests -= 1;
    if (json) {
      outgoing.write(json);
      seen.add(json.url);
    }
    scheduleNext();
  }

  function scheduleNext() {
    if (from < lines.length && activeRequests < maxConcurrent) {
      setTimeout(downloadNext, 0);
    } else if (from >= lines.length) {
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
    if (seen.has(record.url)) {
      console.log('Wow, this record appears more than one time: ', record.url);
    }
    seen.add(record.url);
  }

  function reportDone() {
    console.warn('Loaded ' + processedRows + ' records from ' + fileName);
    done();
  }
}

function download(name, downloadComplete) {
  if (seen.has(name)) {
    setTimeout(downloadComplete, 0);
    return;
  }

  downloadOne(name);
  return;

  function downloadOne(name, retryCount) {
    var link = 'https://www.reddit.com/r/' + name + '/about.json';
    console.warn(from + '. Downloading ' + link);

    request(link, function(err, response, body) {
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
          downloadComplete();
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
      downloadComplete(record);
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
        console.warn('retrying link ' + name + '; Retry attempt: ' + retryAttempt);
        downloadOne(name, retryAttempt);
      }, 1000 * retryAttempt);

      return true; // yes, one more chance please
    }
  }
}

function parseRecord(data) {
  var urlMatch = data.url.match(urlRe);
  if (!urlMatch) {
    throw new Error('Cannot parse url: ' + data.url);
  }
  var subName = urlMatch[1];

  var parsed = {
    url: subName,
    created: data.created,
    subscribers: data.subscribers
  };
  var links = getLinks(subName, data.description);
  if (links && links.length) parsed.links = links;

  return parsed;
}

function getLinks(subName, description) {
  if (!description) return;

  var match;
  var links = new Set();
  while(match = linkRe.exec(description)) {
    // it's common to have the same subreddit have itself in description. Ignore those.
    var sameName = (subName.toUpperCase() === match[1].toUpperCase());

    if (!sameName) {
      links.add(match[1]);
    }
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
