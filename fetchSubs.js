// sitemap link can be found from https://www.reddit.com/robots.txt
var rootSitemap = 'http://reddit-sitemaps.s3-website-us-east-1.amazonaws.com/subreddit-sitemaps.xml';

var zlib = require('zlib');
var request = require('request');
var concat = require('concat-stream');

getAllSiteMaps(downloadEachSitemap);

function downloadEachSitemap(sitemaps) {
  console.warn('Downloaded ' + sitemaps.length + ' links to sitemaps. Processing each one...');
  sitemaps.forEach(downloadOneSitemap);

  function downloadOneSitemap(link) {
    request(link, { encoding: null })
      .pipe(zlib.createGunzip())
      .pipe(concat(function(stringBuffer) {
        parseSitemapFile(stringBuffer, link);
      }));
  }

  function parseSitemapFile(stringBuffer, fileName) {
    // Yeah, parsing xml with regex. I'm not a good software engineer?
    var siteMapRe = /<url><loc>https:\/\/www\.reddit\.com\/r\/([^\/]+?)\/<\/loc><\/url>/g;
    var match;
    var content = stringBuffer.toString();
    var count = 0;
    while(match = siteMapRe.exec(content)) {
      console.log(match[1]);
      count++;
    }

    console.warn('Processed ' + count + ' subs from ' + fileName);
  }
}

function getAllSiteMaps(callback) {
  // I heard xml parsing with regex is bad...
  var siteMapRe = /<sitemap><loc>([^<]+?)<\/loc><\/sitemap>/g;
  var all = [];

  request(rootSitemap, { encoding: null })
    .pipe(zlib.createGunzip())
    .pipe(concat(parseRootSitemap))

  function parseRootSitemap(stringBuffer) {
    var content = stringBuffer.toString();
    var match;
    while(match = siteMapRe.exec(content)) {
      all.push(match[1]);
    }

    callback(all);
  }
}
