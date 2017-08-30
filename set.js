var parallel    = require('async').parallel;
var MongoClient = require('mongodb').MongoClient;

function set_key(keyspace,key,value, db, cb) {
  
  var doc       = {
    keyspace: keyspace,
	key:key
  };

  var updateDoc = {
	  
	  $set: {  
  key: key,
    value: value
	  }
  };
  
 

  var operation      = {
    upsert: true
  };

  db
    .collection('keyspace')
    .updateOne(doc, updateDoc , operation, function (err) {
      if(err) return cb(err);

    cb(null);
    });
}

module.exports = function (ctx, done) {
  var ks = ctx.data.keyspace.split(' ');
  var key = ctx.data.key;
  var value = ctx.data.value;
   
  

  MongoClient.connect(ctx.data.MONGO_URL, function (err, db) {
    if(err) return done(err);

    var job_list =  ks.map(function (keyspace) {

      return function (cb) {
        set_key(keyspace,key,value, db, function (err) {
          if(err) return cb(err);

          cb(null);
        });
      };

    });

    parallel(job_list, function (err) {
      if(err) return done(err);

      done(null, "" + key+ ' ' + value +" saved.");
    });

  });
};