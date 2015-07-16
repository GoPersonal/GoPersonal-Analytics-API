// BASE SETUP
// =============================================================================

// call the packages we need
var app    = require('express')(); // define our app using express
var server = require('http').Server(app);
var io = require('socket.io')(server);
var bodyParser = require('body-parser');
var async = require('async');
var AWS = require('aws-sdk');

AWS.config.update({accessKeyId: 'AKIAJH3W6FM2G6QTJNYQ', secretAccessKey: '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'});


var dynamodb = new AWS.DynamoDB({region:'us-east-1'});

var port = process.env.PORT || 8082;        // set our port
server.listen(port);

app.get('/', function (req, res) {
  res.json({ message: 'Welcome to the GoPersonal.io Analytics API!' });  
});

io.on('connection', function (socket) {
	socket.emit('connection', { status: 'connected' });
	socket.on('get pageviews', function (data) {
		//console.log('data=' + data);
		//console.log('LastKey=' + data.LastKey);
		var msg = data;
    	var params = {
		  TableName: 'SitePageviews',
		  KeyConditions: {
		    SiteID: {
		      ComparisonOperator: 'EQ', /* required */
		      AttributeValueList: [
		        { /* AttributeValue */
		          S: msg.SiteID
		        }
		        /* more items */
		      ]
		    },
		    TimeStamp: {
		      ComparisonOperator: 'EQ', /* required */
		      AttributeValueList: [
		        { /* AttributeValue */
		          N: msg.LastKey
		        }
		        /* more items */
		      ]
		    }
		    /* anotherKey: ... */
		  }
		};
		dynamodb.query(params, function(err, data) {
			if (err) {
		  		console.log(err, err.stack); // an error occurred
		  	}
		  	else {
		  		var pageViews = 0;
		  		if (data.Count>0)
		  		{
		  			console.log()
		  			for(var i=0;i<data.Count;i++)
		  			{
		  				pageViews += parseInt(data.Items[i].PageViews.N);
		  			}
		  		}
		  		
		  		// Get Pageviews Response
	  			socket.emit('gpr', JSON.stringify({
	  				PageViews : pageViews
	  			}));
		  	}
		});  
  	});

	socket.on('next event', function (data) {
		//console.log('data=' + data);
		//console.log('LastKey=' + data.LastKey);
		var msg = data;
    	var params = {
		  TableName: 'SessionEvents',
		  KeyConditions: {
		    SessionID: {
		      ComparisonOperator: 'EQ', /* required */
		      AttributeValueList: [
		        { /* AttributeValue */
		          S: msg.SessionID
		        }
		        /* more items */
		      ]
		    },
		    TimeStamp: {
		      ComparisonOperator: 'GT', /* required */
		      AttributeValueList: [
		        { /* AttributeValue */
		          S: msg.LastKey
		        }
		        /* more items */
		      ]
		    }
		    /* anotherKey: ... */
		  }
		};
		dynamodb.query(params, function(err, data) {
			if (err) {
		  		console.log(err, err.stack); // an error occurred
		  	}
		  	else {
		  		var eventList = [];
		  		var IsLastBatch = data.LastEvaluatedKey ? false : true;
		  		var LastKey = msg.LastKey;
		  		if (data.Count>0)
		  		{
		  			for(var i=0;i<data.Count;i++)
		  			{
		  				eventList.push(JSON.parse(data.Items[i].Data.S));
		  			}
		  			
		  			LastKey = typeof(data.LastEvaluatedKey) != 'undefined' ? data.LastEvaluatedKey.TimeStamp.S : data.Items[data.Count-1].TimeStamp.S;
		  		}
		  		
	  			socket.emit('next event', JSON.stringify({
	  				LastKey : LastKey,
	  				IsLastBatch: IsLastBatch,
	  				Data : eventList
	  			}));
		  	}
		});  
  	});
});
console.log('Magic happens on port ' + port);