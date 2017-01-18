/**
 * @fileOverview Video segments extracting logic.
 */

var debug = require('debug')('clickberry:video-segments:segmenter');
var ffmpeg = require('ffmpeg');
var fs = require('fs');
var uuid = require('node-uuid');
var request = require('request');
var path = require('path');
var events = require('events');
var util = require('util');
var async = require('async');

var tmp = require('tmp');
tmp.setGracefulCleanup(); // cleanup the temporary files even when an uncaught exception occurs

var AWS = require('aws-sdk');
var s3 = new AWS.S3({signatureVersion: 'v4'});

/**
 * A class for extracting frames from a vide file.
 *
 * @class
 */
var Segmenter = function (options) {
  options = options || {};

  /**
   * Additional options
   */
  this.options = options;

  /**
   * Number of images to upload at once.
   */
  this.options.batchSize = this.options.batchSize || 10;
};

/**
 * Segmenter inherits EventEmitter's on()/emit() methods.
 */
util.inherits(Segmenter, events.EventEmitter);


/**
 * Helper function for downloading file from the URI to the local file path.
 *
 * @method     downloadFile
 * @param      {string}    uri       File URI.
 * @param      {string}    filePath  Local file path to download to.
 * @param      {Function}  fn        Callback.
 */
function downloadToLocal(uri, filePath, fn) {
  debug('Downloading ' + uri + ' to ' + filePath);

  var error;
  var contentType;
  var file = fs.createWriteStream(filePath)
    .on('finish', function() {
      if (error) {
        return this.close();
      }
      
      this.close(function (err) {
        if (err) return fn(err);
        debug('Downloading ' + uri + ' to file ' + filePath + ' completed.');
        fn(null, contentType);
      });
    });

  request
    .get(uri)
    .on('response', function(res) {
      if (200 != res.statusCode) {
        error = new Error('Invalid status code: ' + res.statusCode + ' while downloading ' + uri);
        error.fatal = true;
        return fn(error);
      }

      contentType = res.headers['content-type'];
    })
    .on('error', function(err) {
      debug('Downloading ' + uri + ' error: ' + err);
      fn(err);
    })
  .pipe(file);
}

/**
 * Extracts segments from the local video file to specified folder.
 *
 * @method     extractSegments
 * @param      {string}    videoPath  Video file path.
 * @param      {string}    format     Video format (extension).
 * @param      {string}    toDir      Target directory path.
 * @param      {Function}  fn         Callback.
 */
function extractSegments(videoPath, format, toDir, fn) {
  debug('Extracting segments from video ' + videoPath + ' to ' + toDir);

  var ext = path.extname(videoPath);
  var filePattern = path.join(toDir, path.basename(videoPath).replace(ext, '_%d.' + format));

  try {
    var proc = new ffmpeg(videoPath);
    proc.then(function (video) {
      video.fnExtractSegments(filePattern, function (err, files) {
        if (err) return fn(err);
        fn(null, files);
      });
    }, function (err) {
      fn(err);
    });
  } catch (err) {
    fn(err);
  }
}

/**
 * Extracts video metadata from the video file.
 *
 * @param      {string}    videoPath  Video file path.
 * @param      {Function}  fn         Callback.
 */
function getVideoMetadata(videoPath, fn) {
  try {
    var proc = new ffmpeg(videoPath);
    proc.then(function (video) {
      fn(null, video.metadata);
    }, function (err) {
      fn(err);
    });
  } catch (err) {
    fn(err);
  }
}

/**
 * Helper function for uploading file to S3 bucket.
 *
 * @method     uploadFrameToS3
 * @param      {string}    filePath     Local file path.
 * @param      {string}    s3Bucket     S3 bucket name.
 * @param      {string}    s3Subdir     S3 sub-directory name.
 * @param      {string}    s3FileName   S3 file name.
 * @param      {string}    contentType  File content type.
 * @param      {Function}  fn           Callback.
 */
function uploadToS3(filePath, s3Bucket, s3Subdir, s3FileName, contentType, fn) {
  var fileStream = fs.createReadStream(filePath);
  var fileName = s3FileName || path.basename(filePath);
  var key = s3Subdir ? s3Subdir + '/' + fileName : fileName;
  debug('Uploading file ' + filePath + ' to the bucket: ' + key);

  var params = {
    Bucket: s3Bucket,
    Key: key,
    ACL: 'public-read',
    Body: fileStream,
    ContentType: contentType
  };

  s3.upload(params, function (err) {
    if (err) return fn(err);

    var uri = getObjectUri(s3Bucket, key);
    debug('File uploaded: ' + uri);
    fn(null, uri);
  });
}

/**
 * Builds full object URI.
 *
 * @method     getObjectUri
 * @param      {string}  s3Bucket    S3 bucket name.
 * @param      {string}  key     Object key name.
 * @return     {string}  Full object URI.
 */
function getObjectUri(s3Bucket, key) {
  return 'https://' + s3Bucket + '.s3.amazonaws.com/' + key;
}

/**
 * Wraps video segment logic for future processing.
 *
 * @method     processSegment
 *
 * @param      {string}     s3Bucket     S3 bucket name.
 * @param      {string}     s3Dir        S3 subdirectory name.
 * @param      {string}     file         Image file to upload.
 * @param      {string}     contentType  The content type
 * @param      {number}     frames       Number of frames per segment.
 * @param      {Segmenter}  segmenter    Segmenter object to emit events.
 * @return     {Function}   function (callback) {}
 */
function processSegment(s3Bucket, s3Dir, file, contentType, segmenter) {
  if (!processSegment.cache) {
    // caching regexes
    processSegment.cache = {
      idxPattern: /_(\d+)/
    };
  }

  return function (fn) {
    // get segment metadata
    getVideoMetadata(file, function (err, metadata) {
      if (err) return fn(err);

      // parsing frame idx
      var fileName = path.basename(file);
      var idxMathes = processSegment.cache.idxPattern.exec(fileName);
      var idx = +idxMathes[1];
      var s3FileName = idx + path.extname(file);

      // upload to s3
      uploadToS3(file, s3Bucket, s3Dir, s3FileName, contentType, function (err, uri) {
        if (err) return fn(err);

        // emit frame event
        var segment = {
          idx: idx, 
          uri: uri, 
          fps: metadata.video.fps
        };

        segmenter.emit('segment', segment);

        fn(null, segment);
      });

    });
  };  
}

/**
 * Wraps array of tasks to execute them later parallelly.
 *
 * @method     processBatch
 * @param      {Array}      tasks   Array of tasks: function (callback) {}
 * @return     {Funcvtion}  function (callback) {}
 */
function processBatch(tasks) {
  return function (fn) {
    debug('Executing batch...');
    async.parallel(tasks,
      function (err, results) {
        if (err) return fn(err);

        debug('Batch processed successfully.');
        fn(null, results);
      });
  };
}

/**
 * Downloads video and uploads segments to S3 bucket. 
 * Generates 'segment' event for each uploaded segment.
 *
 * @method     downloadAndExtractToS3
 * @param      {string}    videoUri  Video URI to download and extract frames from.
 * @param      {string}    s3Bucket  S3 bucket name to upload frames to.
 * @param      {Function}  fn         Callback function.
 */
Segmenter.prototype.downloadAndExtractToS3 = function (videoUri, s3Bucket, fn) {
  var segmenter = this;
  fn = fn || function (err) {
    if (err) debug(err);
  };

  var handleError = function (err) {
    // emit error event
    segmenter.emit('error', err);

    // call callback with error
    fn(err);
  };

  // create temp file
  tmp.file(function (err, localVideoPath, fd, cleanupLocalVideo) {
    if (err) return handleError(err);

    // download remote file
    downloadToLocal(videoUri, localVideoPath, function (err, contentType) {
      if (err) return handleError(err);

      // create temp dir for segments
      tmp.dir({unsafeCleanup: true}, function (err, segmentsPath, cleanupSegmentsDir) {
        if (err) return handleError(err);

        // extract segments
        var format = path.extname(videoUri).substring(1);
        extractSegments(localVideoPath, format, segmentsPath, function (err, files) {
          // delete temp file
          cleanupLocalVideo();

          if (err) return handleError(err);

          debug(files.length + ' segments extracted from video ' + videoUri);

          // upload files to s3 and generate events
          var segmentTasks = [];
          var s3Dir = path.basename(videoUri, path.extname(videoUri));

          // processing segments
          files.forEach(function (file) {
            segmentTasks.push(processSegment(s3Bucket, s3Dir, file, contentType, segmenter));
          });

          // break tasks into batches
          var batches = [],
              batchSize = segmenter.options.batchSize,
              batchNumber = Math.floor(segmentTasks.length / batchSize) + 1,
              i,
              start,
              end;
          for (i = 0; i < batchNumber; i++) {
            start = i * batchSize;
            end = Math.min((i + 1) * batchSize, segmentTasks.length);
            batches.push(processBatch(segmentTasks.slice(start, end)));
          }

          // execute batches serially
          debug('Video segments processing broken into ' + batches.length + ' batches');
          async.series(batches,
            function (err, results) {
              if (err) return handleError(err);

              var res = {};
              results.forEach(function (b) {
                b.forEach(function (f) {
                  res[f.idx] = f.uri;
                });
              });

              debug('All video segments processed and uploaded to S3');

              // remove local files
              cleanupSegmentsDir();

              // emit end event
              segmenter.emit('end', res);

              fn(null, res);
            });
        });
      });
    });
  });
};

module.exports = Segmenter;
