var parallel    = require('async').parallel;
var MongoClient = require('mongodb').MongoClient;



 


function get_key(keyspace,key, db, done) {
  var doc       = {
    keyspace: keyspace,
    key: key
 
  };

 var jsonDoc = {
   value: "no-value."
};

 
         var  cursor =   db.collection('keyspace').find(doc);
           
           
            cursor.each(function(err, result) {
              
              if(result !== null){
                
                jsonDoc = JSON.stringify(result);
              
    if (err) throw err;
    console.log("jsonDoc---" +jsonDoc);
    done(null, "" + "jsonDoc"+ ' ' +jsonDoc);
 
          }
          
   
  } );
 
 
}

module.exports = function (ctx, done) {
  var ks = ctx.data.keyspace;
  var key = ctx.data.key;
 
  MongoClient.connect(ctx.data.MONGO_URL, function (err, db) {
    if(err) return done(err);
    
    get_key(ks,key, db, done);
 
  });
};